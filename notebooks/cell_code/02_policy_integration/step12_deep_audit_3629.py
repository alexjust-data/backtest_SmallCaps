from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")
RUN03 = PROJECT_ROOT / "runs" / "data_quality" / "03_time_coverage" / "20260224_103217_massive_v2"
PREFILTER_FP = RUN03 / "03_universe_prefilter.parquet"

if not PREFILTER_FP.exists():
    raise FileNotFoundError(f"No existe {PREFILTER_FP}")

pf = pl.read_parquet(PREFILTER_FP)
excluded = pf.filter(pl.col("eligible") == False).select(["ticker", "reason"])

q95 = DATA_ROOT / "quotes_p95"
o19 = DATA_ROOT / "ohlcv_intraday_1m" / "2019_2025"


def _quote_months(ticker: str) -> set[tuple[int, int]]:
    p = q95 / ticker
    out = set()
    if not p.exists():
        return out
    for y in p.glob("year=*"):
        try:
            yy = int(y.name.split("=", 1)[1])
        except Exception:
            continue
        for m in y.glob("month=*"):
            try:
                mm = int(m.name.split("=", 1)[1])
            except Exception:
                continue
            if any(m.glob("day=*/quotes.parquet")):
                out.add((yy, mm))
    return out


def _ohlcv_months(ticker: str) -> set[tuple[int, int]]:
    p = o19 / ticker
    out = set()
    if not p.exists():
        return out
    for y in p.glob("year=*"):
        try:
            yy = int(y.name.split("=", 1)[1])
        except Exception:
            continue
        for m in y.glob("month=*"):
            try:
                mm = int(m.name.split("=", 1)[1])
            except Exception:
                continue
            if (m / "minute.parquet").exists():
                out.add((yy, mm))
    return out


rows = []
for r in excluded.iter_rows(named=True):
    t = r["ticker"]
    qm = sorted(_quote_months(t))
    om = sorted(_ohlcv_months(t))
    ov = sorted(set(qm) & set(om))
    q_min = qm[0] if qm else None
    q_max = qm[-1] if qm else None
    o_min = om[0] if om else None

    rows.append(
        {
            "ticker": t,
            "reason_prefilter": r["reason"],
            "q_months_n": len(qm),
            "o_months_n": len(om),
            "overlap_months_n_2019_2025": len(ov),
            "has_overlap_any_2019_2025": len(ov) > 0,
            "q_first": q_min,
            "q_last": q_max,
            "o_first": o_min,
            "year_gap_o_first_minus_q_last": (
                (o_min[0] - q_max[0]) if (o_min is not None and q_max is not None) else None
            ),
            "window_recoverable": len(ov) > 0,
        }
    )

adf = pl.DataFrame(rows)

summary_main = {
    "n_excluded": int(adf.height),
    "reason_counts": adf.group_by("reason_prefilter").agg(pl.len().alias("n")).to_dicts(),
    "n_window_recoverable": int(adf.filter(pl.col("window_recoverable") == True).height),
    "n_no_overlap_any_2019_2025": int(adf.filter(pl.col("window_recoverable") == False).height),
}

year_bucket = (
    adf.with_columns([
        pl.col("q_first").map_elements(
            lambda x: x[0] if x is not None else None,
            return_dtype=pl.Int64,
        ).alias("q_first_year"),
    ])
    .with_columns([
        pl.when(pl.col("q_first_year") > 2019)
        .then(pl.lit("starts_after_2019"))
        .when(pl.col("q_first_year") == 2019)
        .then(pl.lit("starts_2019_but_after_M10"))
        .otherwise(pl.lit("starts_before_2019"))
        .alias("q_start_bucket")
    ])
    .group_by("q_start_bucket")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)

no_overlap_gap = (
    adf.filter(pl.col("window_recoverable") == False)
    .group_by("year_gap_o_first_minus_q_last")
    .agg(pl.len().alias("n"))
    .sort("year_gap_o_first_minus_q_last")
)

out_dir = (
    PROJECT_ROOT
    / "runs"
    / "backtest"
    / "02_policy_integration"
    / "schema_gap_diagnostics"
    / f"step12_deep_audit_3629_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
)
out_dir.mkdir(parents=True, exist_ok=True)

adf_fp = out_dir / "step12_excluded_3629_diagnostics.parquet"
yb_fp = out_dir / "step12_q_start_bucket.parquet"
gap_fp = out_dir / "step12_no_overlap_gap_distribution.parquet"
summary_fp = out_dir / "step12_deep_audit_summary.json"

adf.write_parquet(adf_fp)
year_bucket.write_parquet(yb_fp)
no_overlap_gap.write_parquet(gap_fp)

summary_fp.write_text(
    json.dumps(
        {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            **summary_main,
            "q_start_bucket": year_bucket.to_dicts(),
            "no_overlap_gap_distribution": no_overlap_gap.to_dicts(),
            "outputs": {
                "diagnostics": str(adf_fp),
                "q_start_bucket": str(yb_fp),
                "gap_distribution": str(gap_fp),
                "summary": str(summary_fp),
            },
        },
        indent=2,
        ensure_ascii=False,
    ),
    encoding="utf-8",
)

print("=== STEP 12 - DEEP AUDIT 3629 ===")
print("source_prefilter:", PREFILTER_FP)
print("output_dir:", out_dir)
print("n_excluded:", summary_main["n_excluded"])
print("reason_counts:", summary_main["reason_counts"])
print("n_window_recoverable:", summary_main["n_window_recoverable"])
print("n_no_overlap_any_2019_2025:", summary_main["n_no_overlap_any_2019_2025"])
print("[q_start_bucket]")
print(year_bucket)
print("[no_overlap gap distribution]")
print(no_overlap_gap)
print("Saved:")
print(adf_fp)
print(yb_fp)
print(gap_fp)
print(summary_fp)