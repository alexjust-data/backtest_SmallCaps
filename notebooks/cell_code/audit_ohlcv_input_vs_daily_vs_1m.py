from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
from IPython.display import display


INPUT_PARQUET = Path(globals().get(
    "INPUT_PARQUET",
    r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet",
))
DAILY_ROOT = Path(globals().get("DAILY_ROOT", r"D:\ohlcv_daily"))
MINUTE_ROOT = Path(globals().get("MINUTE_ROOT", r"D:\ohlcv_1m"))
DAILY_AUDIT_CSV = Path(globals().get("DAILY_AUDIT_CSV", DAILY_ROOT / "_run" / "download_ohlcv_daily_v1.ticker_audit.csv"))
MINUTE_AUDIT_CSV = Path(globals().get("MINUTE_AUDIT_CSV", MINUTE_ROOT / "_run" / "download_ohlcv_minute_v1.ticker_audit.csv"))
MINUTE_ERRORS_CSV = Path(globals().get("MINUTE_ERRORS_CSV", MINUTE_ROOT / "_run" / "download_ohlcv_minute_v1.errors.csv"))
TOP_N = int(globals().get("TOP_N", 50))


def _ticker_set_from_input(path: Path) -> list[str]:
    df = pd.read_parquet(path, columns=["ticker"])
    s = df["ticker"].astype("string").str.strip().dropna().str.upper()
    s = s[s != ""]
    return sorted(s.drop_duplicates().tolist())


def _ticker_dirs(root: Path) -> list[str]:
    if not root.exists():
        raise FileNotFoundError(f"No existe root: {root}")
    out = []
    for p in root.glob("ticker=*"):
        if p.is_dir():
            out.append(p.name.split("=", 1)[1])
    return sorted(set(out))


for p in [INPUT_PARQUET, DAILY_AUDIT_CSV, MINUTE_AUDIT_CSV]:
    if not p.exists():
        raise FileNotFoundError(p)

input_tickers = _ticker_set_from_input(INPUT_PARQUET)
daily_dirs = _ticker_dirs(DAILY_ROOT)
minute_dirs = _ticker_dirs(MINUTE_ROOT)

daily_audit = pd.read_csv(DAILY_AUDIT_CSV)
minute_audit = pd.read_csv(MINUTE_AUDIT_CSV)
minute_errors = pd.read_csv(MINUTE_ERRORS_CSV) if MINUTE_ERRORS_CSV.exists() else pd.DataFrame()

input_set = set(input_tickers)
daily_set = set(daily_dirs)
minute_set = set(minute_dirs)

missing_in_daily = sorted(input_set - daily_set)
missing_in_minute = sorted(input_set - minute_set)
extra_in_daily = sorted(daily_set - input_set)
extra_in_minute = sorted(minute_set - input_set)
missing_minute_vs_daily = sorted(daily_set - minute_set)

minute_missing_audit = minute_audit[minute_audit["ticker"].isin(missing_in_minute)].copy()
daily_missing_audit = daily_audit[daily_audit["ticker"].isin(missing_in_daily)].copy()

minute_error_causes = pd.DataFrame()
if not minute_missing_audit.empty:
    minute_error_causes = (
        minute_missing_audit.groupby(["status", "http_status", "msg"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "http_status", "msg"], ascending=[False, True, True])
        .reset_index(drop=True)
    )

summary = {
    "paths_used": {
        "input_parquet": str(INPUT_PARQUET),
        "daily_root": str(DAILY_ROOT),
        "minute_root": str(MINUTE_ROOT),
        "daily_audit_csv": str(DAILY_AUDIT_CSV),
        "minute_audit_csv": str(MINUTE_AUDIT_CSV),
        "minute_errors_csv": str(MINUTE_ERRORS_CSV),
    },
    "counts": {
        "input_unique_tickers": len(input_tickers),
        "daily_dir_tickers": len(daily_dirs),
        "minute_dir_tickers": len(minute_dirs),
        "missing_in_daily": len(missing_in_daily),
        "missing_in_minute": len(missing_in_minute),
        "extra_in_daily": len(extra_in_daily),
        "extra_in_minute": len(extra_in_minute),
        "missing_minute_vs_daily": len(missing_minute_vs_daily),
    },
}

print("=== PATHS USADOS ===")
for k, v in summary["paths_used"].items():
    print(f"{k}: {v}")

print("\n=== RESUMEN ===")
print(json.dumps(summary["counts"], indent=2, ensure_ascii=False))

print("\n=== AUDIT DAILY STATUS ===")
print(daily_audit["status"].value_counts(dropna=False).to_string())

print("\n=== AUDIT MINUTE STATUS ===")
print(minute_audit["status"].value_counts(dropna=False).to_string())

print("\n=== CAUSAS DE LOS FALTANTES EN 1M ===")
if minute_error_causes.empty:
    print("Sin causas; no hay faltantes en 1m")
else:
    display(minute_error_causes)

print(f"\n=== EJEMPLOS FALTANTES EN 1M (TOP {TOP_N}) ===")
display(pd.DataFrame({"ticker": missing_in_minute[:TOP_N]}))

print(f"\n=== EJEMPLOS FALTANTES EN DAILY (TOP {TOP_N}) ===")
display(pd.DataFrame({"ticker": missing_in_daily[:TOP_N]}))

print(f"\n=== INPUT vs DAILY vs MINUTE (TOP {TOP_N}) ===")
comparison = pd.DataFrame({"ticker": input_tickers[:]}).copy()
comparison = comparison.assign(
    in_daily=comparison["ticker"].isin(daily_set),
    in_minute=comparison["ticker"].isin(minute_set),
)
comparison = comparison[(~comparison["in_daily"]) | (~comparison["in_minute"])].head(TOP_N).reset_index(drop=True)
display(comparison)

if not minute_missing_audit.empty:
    print(f"\n=== DETAIL MINUTE AUDIT PARA FALTANTES (TOP {TOP_N}) ===")
    display(minute_missing_audit.sort_values(["http_status", "ticker"]).head(TOP_N).reset_index(drop=True))

if not minute_errors.empty:
    print(f"\n=== MINUTE ERRORS CSV (TOP {TOP_N}) ===")
    display(minute_errors.head(TOP_N).reset_index(drop=True))
