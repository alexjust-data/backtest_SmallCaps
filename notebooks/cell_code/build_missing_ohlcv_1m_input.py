from pathlib import Path
import json
import pandas as pd


INPUT_PARQUET = Path(
    globals().get(
        "INPUT_PARQUET",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet",
    )
)
DAILY_ROOT = Path(globals().get("DAILY_ROOT", r"D:\ohlcv_daily"))
MINUTE_ROOT = Path(globals().get("MINUTE_ROOT", r"D:\ohlcv_1m"))
OUTDIR = Path(
    globals().get(
        "OUTDIR",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti",
    )
)

OUT_PARQUET = OUTDIR / "tickers_missing_in_ohlcv_1m_vs_daily.parquet"
OUT_CSV = OUTDIR / "tickers_missing_in_ohlcv_1m_vs_daily.csv"
SUMMARY_JSON = OUTDIR / "tickers_missing_in_ohlcv_1m_vs_daily.summary.json"


def _ticker_set_from_parquet(path: Path) -> set[str]:
    d = pd.read_parquet(path, columns=["ticker"]).copy()
    s = d["ticker"].astype("string").str.strip().dropna().str.upper()
    s = s[s != ""]
    return set(s.drop_duplicates().tolist())


def _ticker_dirs(root: Path) -> set[str]:
    out: set[str] = set()
    for p in root.glob("ticker=*"):
        if p.is_dir():
            out.add(p.name.split("=", 1)[1].strip().upper())
    return out


if not INPUT_PARQUET.exists():
    raise FileNotFoundError(INPUT_PARQUET)
if not DAILY_ROOT.exists():
    raise FileNotFoundError(DAILY_ROOT)
if not MINUTE_ROOT.exists():
    raise FileNotFoundError(MINUTE_ROOT)

OUTDIR.mkdir(parents=True, exist_ok=True)

input_df = pd.read_parquet(INPUT_PARQUET).copy()
input_df["ticker"] = input_df["ticker"].astype("string").str.strip().str.upper()
input_df = input_df.dropna(subset=["ticker"])
input_df = input_df[input_df["ticker"] != ""].copy()

input_set = _ticker_set_from_parquet(INPUT_PARQUET)
daily_set = _ticker_dirs(DAILY_ROOT)
minute_set = _ticker_dirs(MINUTE_ROOT)

missing_vs_daily = sorted((daily_set - minute_set) & input_set)
missing_vs_input = sorted((input_set - minute_set))

missing_df = (
    input_df[input_df["ticker"].isin(missing_vs_daily)]
    .drop_duplicates(subset=["ticker"], keep="first")
    .sort_values("ticker")
    .reset_index(drop=True)
)

missing_df.to_parquet(OUT_PARQUET, index=False)
missing_df.to_csv(OUT_CSV, index=False, encoding="utf-8")

summary = {
    "paths_used": {
        "input_parquet": str(INPUT_PARQUET),
        "daily_root": str(DAILY_ROOT),
        "minute_root": str(MINUTE_ROOT),
        "out_parquet": str(OUT_PARQUET),
        "out_csv": str(OUT_CSV),
    },
    "counts": {
        "input_unique_tickers": int(len(input_set)),
        "daily_dir_tickers": int(len(daily_set)),
        "minute_dir_tickers": int(len(minute_set)),
        "missing_in_minute_vs_daily": int(len(missing_vs_daily)),
        "missing_in_minute_vs_input": int(len(missing_vs_input)),
        "written_rows": int(len(missing_df)),
    },
    "sample_missing_tickers": missing_vs_daily[:50],
}
SUMMARY_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("build_missing_ohlcv_1m_input completed")
print("OUT_PARQUET:", OUT_PARQUET)
print("OUT_CSV:", OUT_CSV)
print("SUMMARY_JSON:", SUMMARY_JSON)
print("missing_in_minute_vs_daily:", len(missing_vs_daily))
print("written_rows:", len(missing_df))
