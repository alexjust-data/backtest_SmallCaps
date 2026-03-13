from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


UNIVERSE_PTI_ROOT = Path(
    globals().get(
        "UNIVERSE_PTI_ROOT",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_panel_pti",
    )
)
OHLCV_DAILY_ROOT = Path(globals().get("OHLCV_DAILY_ROOT", r"D:\ohlcv_daily"))
INCOME_STATEMENTS_ROOT = Path(globals().get("INCOME_STATEMENTS_ROOT", r"D:\financial\income_statements"))
BALANCE_SHEETS_ROOT = Path(globals().get("BALANCE_SHEETS_ROOT", r"D:\financial\balance_sheets"))

RUN_ID = str(globals().get("RUN_ID", datetime.now().strftime("%Y%m%d_%H%M%S_population_target_pti")))
OUT_BASE = Path(
    globals().get(
        "OUT_BASE",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\backtest\population_target_pti",
    )
)
OUT_DIR = Path(globals().get("OUT_DIR", OUT_BASE / RUN_ID))
OUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_FROM = str(globals().get("DATE_FROM", "2005-01-01"))
DATE_TO = str(globals().get("DATE_TO", "2026-12-31"))
TTL_DAYS = int(globals().get("TTL_DAYS", 180))
MAX_TICKERS = globals().get("MAX_TICKERS", None)
MAX_TICKERS = int(MAX_TICKERS) if MAX_TICKERS not in (None, "", 0, "0") else None

OUTPUT_PARQUET = Path(globals().get("OUTPUT_PARQUET", OUT_DIR / "population_target_pti.parquet"))
SUMMARY_JSON = Path(globals().get("SUMMARY_JSON", OUT_DIR / "population_target_pti_summary.json"))
YEAR_SUMMARY_PARQUET = Path(globals().get("YEAR_SUMMARY_PARQUET", OUT_DIR / "population_target_pti_year_summary.parquet"))
ARTIFACTS_JSON = Path(globals().get("ARTIFACTS_JSON", OUT_DIR / "population_target_pti_artifacts.json"))


def _glob_expr(root: Path) -> str:
    return str(root / "**" / "*.parquet")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


for p in [UNIVERSE_PTI_ROOT, OHLCV_DAILY_ROOT, INCOME_STATEMENTS_ROOT]:
    if not p.exists():
        raise FileNotFoundError(f"No existe path requerido: {p}")


con = duckdb.connect()
con.execute("PRAGMA threads=8")

universe_glob = _glob_expr(UNIVERSE_PTI_ROOT)
ohlcv_glob = _glob_expr(OHLCV_DAILY_ROOT)
income_glob = _glob_expr(INCOME_STATEMENTS_ROOT)
balance_glob = _glob_expr(BALANCE_SHEETS_ROOT) if BALANCE_SHEETS_ROOT.exists() else None

max_tickers_clause = f"LIMIT {MAX_TICKERS}" if MAX_TICKERS is not None else ""

sql = f"""
CREATE OR REPLACE TEMP VIEW spine_base AS
SELECT
    CAST(snapshot_date AS DATE) AS date,
    UPPER(TRIM(ticker)) AS ticker,
    entity_id,
    primary_exchange,
    active,
    cik,
    composite_figi,
    share_class_figi
 FROM read_parquet('{universe_glob}', union_by_name = true)
WHERE CAST(snapshot_date AS DATE) BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}';

CREATE OR REPLACE TEMP VIEW selected_tickers AS
SELECT ticker
FROM (
    SELECT DISTINCT ticker
    FROM spine_base
    ORDER BY ticker
    {max_tickers_clause}
);

CREATE OR REPLACE TEMP VIEW spine AS
SELECT
    s.date,
    s.ticker,
    s.entity_id,
    s.primary_exchange,
    CASE WHEN COALESCE(s.active, FALSE) THEN 'active' ELSE 'inactive' END AS status,
    s.cik,
    s.composite_figi,
    s.share_class_figi,
    EXTRACT(YEAR FROM s.date) AS year,
    EXTRACT(MONTH FROM s.date) AS month
FROM spine_base s
JOIN selected_tickers t USING (ticker);

CREATE OR REPLACE TEMP VIEW daily_close AS
SELECT
    UPPER(TRIM(ticker)) AS ticker,
    CAST(date AS DATE) AS date,
    CAST(c AS DOUBLE) AS close_t,
    CAST(v AS DOUBLE) AS volume_t,
    CAST(vw AS DOUBLE) AS vwap_t,
    CAST(n AS BIGINT) AS trades_t
FROM read_parquet('{ohlcv_glob}', union_by_name = true)
WHERE CAST(date AS DATE) BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}';

CREATE OR REPLACE TEMP VIEW shares_obs AS
SELECT
    UPPER(TRIM(ticker)) AS ticker,
    CAST(filing_date AS DATE) AS shares_observed_date,
    CAST(
        COALESCE(diluted_shares_outstanding, basic_shares_outstanding) AS DOUBLE
    ) AS shares_outstanding_obs,
    CASE
        WHEN diluted_shares_outstanding IS NOT NULL THEN 'diluted'
        WHEN basic_shares_outstanding IS NOT NULL THEN 'basic'
        ELSE NULL
    END AS shares_source,
    cik AS shares_cik,
    CAST(period_end AS DATE) AS shares_period_end
FROM read_parquet('{income_glob}', union_by_name = true)
WHERE COALESCE(_empty, FALSE) = FALSE
  AND filing_date IS NOT NULL
  AND COALESCE(diluted_shares_outstanding, basic_shares_outstanding) IS NOT NULL;

CREATE OR REPLACE TEMP VIEW joined AS
SELECT
    s.date,
    s.ticker,
    s.entity_id,
    s.primary_exchange,
    s.status,
    s.year,
    s.month,
    COALESCE(NULLIF(s.composite_figi, ''), NULLIF(s.share_class_figi, ''), s.entity_id) AS figi_like,
    dc.close_t,
    dc.volume_t,
    dc.vwap_t,
    dc.trades_t,
    sh.shares_observed_date,
    sh.shares_period_end,
    sh.shares_source,
    sh.shares_outstanding_obs,
    DATE_DIFF('day', sh.shares_observed_date, s.date) AS shares_age_days,
    CASE
        WHEN sh.shares_observed_date IS NULL THEN NULL
        WHEN DATE_DIFF('day', sh.shares_observed_date, s.date) > {TTL_DAYS} THEN NULL
        ELSE sh.shares_outstanding_obs
    END AS shares_outstanding_t
FROM spine s
LEFT JOIN daily_close dc
    ON s.ticker = dc.ticker
   AND s.date = dc.date
ASOF LEFT JOIN shares_obs sh
    ON s.ticker = sh.ticker
   AND s.date >= sh.shares_observed_date;

CREATE OR REPLACE TEMP VIEW final_population AS
SELECT
    date,
    ticker,
    entity_id,
    figi_like,
    primary_exchange,
    status,
    close_t,
    shares_outstanding_t,
    shares_source,
    shares_observed_date,
    shares_period_end,
    shares_age_days,
    CASE
        WHEN close_t IS NOT NULL AND shares_outstanding_t IS NOT NULL
            THEN close_t * shares_outstanding_t
        ELSE NULL
    END AS market_cap_t,
    CASE
        WHEN close_t IS NOT NULL AND shares_outstanding_t IS NOT NULL
            THEN (close_t * shares_outstanding_t) < 2000000000
        ELSE NULL
    END AS is_small_cap_t,
    year,
    month,
    volume_t,
    vwap_t,
    trades_t
FROM joined;
"""

con.execute(sql)
con.execute(f"COPY final_population TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1)")

year_summary = con.execute(
    """
    SELECT
        year,
        COUNT(*) AS rows_total,
        COUNT(close_t) AS rows_with_close_t,
        COUNT(shares_outstanding_t) AS rows_with_shares_t,
        COUNT(is_small_cap_t) AS rows_classifiable,
        SUM(CASE WHEN is_small_cap_t THEN 1 ELSE 0 END) AS rows_small_cap,
        SUM(CASE WHEN shares_observed_date > date THEN 1 ELSE 0 END) AS anti_lookahead_violations
    FROM final_population
    GROUP BY 1
    ORDER BY 1
    """
).df()
year_summary.to_parquet(YEAR_SUMMARY_PARQUET, index=False)

summary_row = con.execute(
    """
    SELECT
        COUNT(*) AS rows_total,
        COUNT(DISTINCT ticker) AS tickers_total,
        COUNT(close_t) AS rows_with_close_t,
        COUNT(shares_outstanding_t) AS rows_with_shares_t,
        COUNT(is_small_cap_t) AS rows_classifiable,
        SUM(CASE WHEN is_small_cap_t THEN 1 ELSE 0 END) AS rows_small_cap,
        SUM(CASE WHEN shares_observed_date > date THEN 1 ELSE 0 END) AS anti_lookahead_violations
    FROM final_population
    """
).fetchone()

summary = {
    "created_at_utc": _now_utc(),
    "date_from": DATE_FROM,
    "date_to": DATE_TO,
    "ttl_days": TTL_DAYS,
    "max_tickers": MAX_TICKERS,
    "paths_used": {
        "universe_pti_root": str(UNIVERSE_PTI_ROOT),
        "ohlcv_daily_root": str(OHLCV_DAILY_ROOT),
        "income_statements_root": str(INCOME_STATEMENTS_ROOT),
        "balance_sheets_root": str(BALANCE_SHEETS_ROOT) if BALANCE_SHEETS_ROOT.exists() else None,
        "output_parquet": str(OUTPUT_PARQUET),
        "year_summary_parquet": str(YEAR_SUMMARY_PARQUET),
    },
    "metrics": {
        "rows_total": int(summary_row[0] or 0),
        "tickers_total": int(summary_row[1] or 0),
        "rows_with_close_t": int(summary_row[2] or 0),
        "rows_with_shares_t": int(summary_row[3] or 0),
        "rows_classifiable": int(summary_row[4] or 0),
        "rows_small_cap": int(summary_row[5] or 0),
        "anti_lookahead_violations": int(summary_row[6] or 0),
    },
}
SUMMARY_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

artifacts = {
    "output_parquet": str(OUTPUT_PARQUET),
    "summary_json": str(SUMMARY_JSON),
    "year_summary_parquet": str(YEAR_SUMMARY_PARQUET),
    "paths_used": summary["paths_used"],
}
ARTIFACTS_JSON.write_text(json.dumps(artifacts, indent=2, ensure_ascii=False), encoding="utf-8")

BUILD_POPULATION_TARGET_OUTPUTS = artifacts

print("population_target_pti build completed")
print("OUT_DIR:", OUT_DIR)
print("OUTPUT_PARQUET:", OUTPUT_PARQUET)
print("SUMMARY_JSON:", SUMMARY_JSON)
print("YEAR_SUMMARY_PARQUET:", YEAR_SUMMARY_PARQUET)
print("rows_total:", summary["metrics"]["rows_total"])
print("tickers_total:", summary["metrics"]["tickers_total"])
print("rows_classifiable:", summary["metrics"]["rows_classifiable"])
print("rows_small_cap:", summary["metrics"]["rows_small_cap"])
print("anti_lookahead_violations:", summary["metrics"]["anti_lookahead_violations"])
