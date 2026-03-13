from __future__ import annotations

from pathlib import Path
import json
from datetime import datetime, timezone

import duckdb
import pandas as pd
from IPython.display import display

RUN_DIR = Path(globals().get(
    "RUN_DIR",
    r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\backtest\population_target_pti\population_target_pti_run_01",
))
RATIOS_ROOT = Path(globals().get("RATIOS_ROOT", r"D:\financial\ratios"))
OUT_DIR = Path(globals().get("OUT_DIR", RUN_DIR / "marketcap_audit"))
TOP_N = int(globals().get("TOP_N", 30))
SHOW_HEAD_TAIL = bool(globals().get("SHOW_HEAD_TAIL", True))

POP_PATH = RUN_DIR / "population_target_pti.parquet"
if not POP_PATH.exists():
    raise FileNotFoundError(f"No existe parquet PTI: {POP_PATH}")
if not RATIOS_ROOT.exists():
    raise FileNotFoundError(f"No existe RATIOS_ROOT: {RATIOS_ROOT}")

OUT_DIR.mkdir(parents=True, exist_ok=True)

TICKER_SUMMARY_PARQUET = OUT_DIR / "ticker_marketcap_coverage_comparison.parquet"
OVERLAP_SAMPLES_PARQUET = OUT_DIR / "marketcap_overlap_samples.parquet"
GLOBAL_SUMMARY_JSON = OUT_DIR / "marketcap_source_global_summary.json"
ARTIFACTS_JSON = OUT_DIR / "marketcap_audit_artifacts.json"

con = duckdb.connect()
con.execute("PRAGMA threads=4")

pop_sql = str(POP_PATH).replace("'", "''")
ratios_glob = str(RATIOS_ROOT / "ticker=*" / "*.parquet").replace("'", "''")

con.execute(f"""
CREATE OR REPLACE TEMP VIEW pop AS
SELECT
    upper(trim(CAST(ticker AS VARCHAR))) AS ticker,
    CAST(date AS DATE) AS date,
    TRY_CAST(close_t AS DOUBLE) AS close_t,
    TRY_CAST(shares_outstanding_t AS DOUBLE) AS shares_outstanding_t,
    TRY_CAST(market_cap_t AS DOUBLE) AS market_cap_formula,
    TRY_CAST(is_small_cap_t AS BOOLEAN) AS is_small_cap_t
FROM read_parquet('{pop_sql}')
WHERE ticker IS NOT NULL AND date IS NOT NULL
""")

con.execute(f"""
CREATE OR REPLACE TEMP VIEW ratios_raw AS
SELECT
    upper(trim(CAST(ticker AS VARCHAR))) AS ticker,
    CAST(date AS DATE) AS date,
    TRY_CAST(price AS DOUBLE) AS polygon_price,
    TRY_CAST(market_cap AS DOUBLE) AS polygon_market_cap,
    TRY_CAST(average_volume AS DOUBLE) AS average_volume,
    TRY_CAST(_ingested_utc AS VARCHAR) AS ingested_utc,
    TRY_CAST(cik AS VARCHAR) AS cik
FROM read_parquet('{ratios_glob}', union_by_name=true)
WHERE coalesce(_empty, FALSE) = FALSE
""")

con.execute("""
CREATE OR REPLACE TEMP VIEW ratios AS
SELECT *
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY ticker, date
            ORDER BY ingested_utc DESC NULLS LAST
        ) AS rn
    FROM ratios_raw
    WHERE ticker IS NOT NULL
      AND ticker <> ''
      AND date IS NOT NULL
      AND polygon_market_cap IS NOT NULL
)
WHERE rn = 1
""")

con.execute("""
CREATE OR REPLACE TEMP VIEW joined AS
SELECT
    p.ticker,
    p.date,
    p.close_t,
    p.shares_outstanding_t,
    p.market_cap_formula,
    p.is_small_cap_t,
    r.polygon_price,
    r.polygon_market_cap,
    r.average_volume,
    r.ingested_utc,
    r.cik,
    CASE WHEN p.market_cap_formula IS NOT NULL THEN 1 ELSE 0 END AS has_formula_market_cap,
    CASE WHEN r.polygon_market_cap IS NOT NULL THEN 1 ELSE 0 END AS has_polygon_market_cap,
    CASE WHEN p.market_cap_formula IS NOT NULL AND r.polygon_market_cap IS NOT NULL THEN 1 ELSE 0 END AS has_both,
    CASE
        WHEN p.market_cap_formula IS NOT NULL AND r.polygon_market_cap IS NOT NULL AND r.polygon_market_cap != 0
        THEN ABS(p.market_cap_formula - r.polygon_market_cap) / ABS(r.polygon_market_cap)
        ELSE NULL
    END AS abs_pct_diff_vs_polygon,
    CASE
        WHEN p.market_cap_formula IS NOT NULL AND r.polygon_market_cap IS NOT NULL
        THEN p.market_cap_formula - r.polygon_market_cap
        ELSE NULL
    END AS diff_formula_minus_polygon
FROM pop p
LEFT JOIN ratios r
  ON p.ticker = r.ticker
 AND p.date = r.date
""")

ticker_summary = con.execute("""
SELECT
    ticker,
    COUNT(*) AS rows_total,
    SUM(has_formula_market_cap) AS rows_formula_market_cap,
    SUM(has_polygon_market_cap) AS rows_polygon_market_cap,
    SUM(has_both) AS rows_both,
    ROUND(100.0 * SUM(has_formula_market_cap) / COUNT(*), 2) AS pct_formula_market_cap,
    ROUND(100.0 * SUM(has_polygon_market_cap) / COUNT(*), 2) AS pct_polygon_market_cap,
    ROUND(100.0 * SUM(has_both) / COUNT(*), 2) AS pct_both,
    AVG(abs_pct_diff_vs_polygon) AS mean_abs_pct_diff_vs_polygon,
    MEDIAN(abs_pct_diff_vs_polygon) AS median_abs_pct_diff_vs_polygon,
    quantile_cont(abs_pct_diff_vs_polygon, 0.95) AS p95_abs_pct_diff_vs_polygon,
    SUM(CASE WHEN abs_pct_diff_vs_polygon <= 0.01 THEN 1 ELSE 0 END) AS rows_within_1pct,
    SUM(CASE WHEN abs_pct_diff_vs_polygon <= 0.05 THEN 1 ELSE 0 END) AS rows_within_5pct,
    SUM(CASE WHEN abs_pct_diff_vs_polygon <= 0.10 THEN 1 ELSE 0 END) AS rows_within_10pct,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    MIN(CASE WHEN has_polygon_market_cap = 1 THEN date ELSE NULL END) AS polygon_min_date,
    MAX(CASE WHEN has_polygon_market_cap = 1 THEN date ELSE NULL END) AS polygon_max_date
FROM joined
GROUP BY ticker
ORDER BY rows_both DESC, rows_formula_market_cap DESC, ticker
""").df()

global_summary = con.execute("""
SELECT
    COUNT(*) AS rows_total,
    COUNT(DISTINCT ticker) AS tickers_total,
    SUM(has_formula_market_cap) AS rows_formula_market_cap,
    SUM(has_polygon_market_cap) AS rows_polygon_market_cap,
    SUM(has_both) AS rows_both,
    COUNT(DISTINCT CASE WHEN has_formula_market_cap = 1 THEN ticker END) AS tickers_with_formula_market_cap,
    COUNT(DISTINCT CASE WHEN has_polygon_market_cap = 1 THEN ticker END) AS tickers_with_polygon_market_cap,
    COUNT(DISTINCT CASE WHEN has_both = 1 THEN ticker END) AS tickers_with_both,
    AVG(abs_pct_diff_vs_polygon) AS mean_abs_pct_diff_vs_polygon,
    MEDIAN(abs_pct_diff_vs_polygon) AS median_abs_pct_diff_vs_polygon,
    quantile_cont(abs_pct_diff_vs_polygon, 0.95) AS p95_abs_pct_diff_vs_polygon,
    SUM(CASE WHEN abs_pct_diff_vs_polygon <= 0.01 THEN 1 ELSE 0 END) AS rows_within_1pct,
    SUM(CASE WHEN abs_pct_diff_vs_polygon <= 0.05 THEN 1 ELSE 0 END) AS rows_within_5pct,
    SUM(CASE WHEN abs_pct_diff_vs_polygon <= 0.10 THEN 1 ELSE 0 END) AS rows_within_10pct,
    MIN(CASE WHEN has_polygon_market_cap = 1 THEN date ELSE NULL END) AS polygon_min_date,
    MAX(CASE WHEN has_polygon_market_cap = 1 THEN date ELSE NULL END) AS polygon_max_date
FROM joined
""").df().iloc[0].to_dict()

overlap_samples = con.execute(f"""
SELECT
    ticker,
    date,
    close_t,
    shares_outstanding_t,
    market_cap_formula,
    polygon_price,
    polygon_market_cap,
    diff_formula_minus_polygon,
    abs_pct_diff_vs_polygon,
    is_small_cap_t,
    average_volume,
    ingested_utc,
    cik
FROM joined
WHERE has_both = 1
ORDER BY abs_pct_diff_vs_polygon DESC NULLS LAST, ticker, date
LIMIT {max(TOP_N * 20, 200)}
""").df()

ticker_summary.to_parquet(TICKER_SUMMARY_PARQUET, index=False)
overlap_samples.to_parquet(OVERLAP_SAMPLES_PARQUET, index=False)

summary_payload = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "paths_used": {
        "run_dir": str(RUN_DIR),
        "population_target_pti": str(POP_PATH),
        "ratios_root": str(RATIOS_ROOT),
        "ticker_summary_parquet": str(TICKER_SUMMARY_PARQUET),
        "overlap_samples_parquet": str(OVERLAP_SAMPLES_PARQUET),
    },
    "metrics": {
        **global_summary,
        "pct_rows_formula_market_cap": round(100.0 * global_summary["rows_formula_market_cap"] / global_summary["rows_total"], 2) if global_summary["rows_total"] else None,
        "pct_rows_polygon_market_cap": round(100.0 * global_summary["rows_polygon_market_cap"] / global_summary["rows_total"], 2) if global_summary["rows_total"] else None,
        "pct_rows_both": round(100.0 * global_summary["rows_both"] / global_summary["rows_total"], 2) if global_summary["rows_total"] else None,
    },
}
GLOBAL_SUMMARY_JSON.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
ARTIFACTS_JSON.write_text(json.dumps({
    "run_dir": str(RUN_DIR),
    "out_dir": str(OUT_DIR),
    "ticker_summary_parquet": str(TICKER_SUMMARY_PARQUET),
    "overlap_samples_parquet": str(OVERLAP_SAMPLES_PARQUET),
    "global_summary_json": str(GLOBAL_SUMMARY_JSON),
}, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== PATHS USADOS ===")
for k, v in summary_payload["paths_used"].items():
    print(f"{k}: {v}")

print("\n=== SUMMARY GLOBAL ===")
print(json.dumps(summary_payload["metrics"], indent=2, ensure_ascii=False, default=str))

print(f"\n=== TOP {TOP_N} TICKERS CON POLYGON MARKET CAP ===")
display(
    ticker_summary.loc[ticker_summary["rows_polygon_market_cap"] > 0]
    .sort_values(["rows_polygon_market_cap", "rows_both", "ticker"], ascending=[False, False, True])
    .head(TOP_N)
    .reset_index(drop=True)
)

print(f"\n=== TOP {TOP_N} TICKERS CON MEJOR SOLAPE FORMULA vs POLYGON ===")
display(
    ticker_summary.loc[ticker_summary["rows_both"] > 0]
    .sort_values(["median_abs_pct_diff_vs_polygon", "p95_abs_pct_diff_vs_polygon", "rows_both"], ascending=[True, True, False])
    .head(TOP_N)
    .reset_index(drop=True)
)

print(f"\n=== TOP {TOP_N} TICKERS CON PEOR DIFERENCIA FORMULA vs POLYGON ===")
display(
    ticker_summary.loc[ticker_summary["rows_both"] > 0]
    .sort_values(["median_abs_pct_diff_vs_polygon", "p95_abs_pct_diff_vs_polygon", "rows_both"], ascending=[False, False, False])
    .head(TOP_N)
    .reset_index(drop=True)
)

print(f"\n=== MUESTRAS DE SOLAPE (TOP {TOP_N}) ===")
display(overlap_samples.head(TOP_N).reset_index(drop=True))

if SHOW_HEAD_TAIL:
    print("\n=== HEAD ticker_summary ===")
    display(ticker_summary.head(5))
    print("\n=== TAIL ticker_summary ===")
    display(ticker_summary.tail(5))
