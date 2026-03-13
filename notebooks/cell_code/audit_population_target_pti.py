from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
from IPython.display import display


RUN_DIR = Path(globals().get(
    "RUN_DIR",
    r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\backtest\population_target_pti\population_target_pti_run_01",
))
TOP_N = int(globals().get("TOP_N", 30))
SHOW_HEAD_TAIL = bool(globals().get("SHOW_HEAD_TAIL", True))

PARQUET_PATH = RUN_DIR / "population_target_pti.parquet"
SUMMARY_PATH = RUN_DIR / "population_target_pti_summary.json"
YEAR_SUMMARY_PATH = RUN_DIR / "population_target_pti_year_summary.parquet"

if not PARQUET_PATH.exists():
    raise FileNotFoundError(f"No existe parquet: {PARQUET_PATH}")

summary = None
if SUMMARY_PATH.exists():
    summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))

df = pd.read_parquet(PARQUET_PATH)
if df.empty:
    raise ValueError(f"Parquet vacio: {PARQUET_PATH}")

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["year"] = df["date"].dt.year

df["has_close_t"] = df["close_t"].notna()
df["has_shares_t"] = df["shares_outstanding_t"].notna()
df["is_classifiable"] = df["market_cap_t"].notna()
df["is_small_cap_true"] = df["is_small_cap_t"] == True
df["is_small_cap_false"] = df["is_small_cap_t"] == False
df["is_small_cap_null"] = df["is_small_cap_t"].isna()

year_audit = (
    df.groupby("year", dropna=False)
    .agg(
        rows_total=("ticker", "size"),
        tickers_n=("ticker", "nunique"),
        rows_with_close_t=("has_close_t", "sum"),
        rows_with_shares_t=("has_shares_t", "sum"),
        rows_classifiable=("is_classifiable", "sum"),
        rows_small_cap=("is_small_cap_true", "sum"),
        rows_not_small_cap=("is_small_cap_false", "sum"),
        rows_null_class=("is_small_cap_null", "sum"),
    )
    .reset_index()
)

for c in [
    "rows_with_close_t",
    "rows_with_shares_t",
    "rows_classifiable",
    "rows_small_cap",
    "rows_not_small_cap",
    "rows_null_class",
]:
    year_audit[f"pct_{c}"] = (100.0 * year_audit[c] / year_audit["rows_total"]).round(2)

top_missing_shares = (
    df.loc[df["shares_outstanding_t"].isna()]
    .groupby("ticker", dropna=False)
    .agg(rows_missing_shares=("ticker", "size"))
    .sort_values("rows_missing_shares", ascending=False)
    .head(TOP_N)
    .reset_index()
)

top_missing_close = (
    df.loc[df["close_t"].isna()]
    .groupby("ticker", dropna=False)
    .agg(rows_missing_close=("ticker", "size"))
    .sort_values("rows_missing_close", ascending=False)
    .head(TOP_N)
    .reset_index()
)

ARTIFACTS = {
    "run_dir": str(RUN_DIR),
    "parquet_path": str(PARQUET_PATH),
    "summary_path": str(SUMMARY_PATH),
    "year_summary_path": str(YEAR_SUMMARY_PATH),
}

print("=== PATHS USADOS ===")
for k, v in ARTIFACTS.items():
    print(f"{k}: {v}")

if summary is not None:
    print("\n=== SUMMARY GLOBAL ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

print("\n=== AUDITORIA POR A?O ===")
display(year_audit)

print(f"\n=== TOP {TOP_N} TICKERS SIN SHARES ===")
display(top_missing_shares)

print(f"\n=== TOP {TOP_N} TICKERS SIN CLOSE ===")
display(top_missing_close)

if SHOW_HEAD_TAIL:
    print("\n=== HEAD(5) ===")
    display(df.head(5))
    print("\n=== TAIL(5) ===")
    display(df.tail(5))
