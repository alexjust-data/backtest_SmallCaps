from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def build_smoke_tasks(ticker: str, date_str: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    ticker = str(ticker).upper().strip()
    date_str = str(date_str).strip()
    tasks = pd.DataFrame([{"ticker": ticker, "date": date_str}])
    tickers = pd.DataFrame([{"ticker": ticker, "list_date": date_str, "delist_date": pd.NA, "end_date": date_str}])
    meta = {
        "mode": "smoke",
        "tickers_count": 1,
        "tasks_total": 1,
        "date_min": date_str,
        "date_max": date_str,
        "tickers_selected": [ticker],
    }
    return tasks, tickers, meta


def build_lifecycle_tasks(universe_path: Path, lifecycle_path: Path, cutoff: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    u = pd.read_parquet(universe_path)
    lc = pd.read_csv(lifecycle_path)

    u["ticker"] = u["ticker"].astype(str).str.upper().str.strip()
    lc["ticker"] = lc["ticker"].astype(str).str.upper().str.strip()

    base = lc[lc["ticker"].isin(set(u["ticker"]))].copy()
    base["list_date"] = pd.to_datetime(base["list_date"], errors="coerce")
    base["delist_date"] = pd.to_datetime(base["delist_date"], errors="coerce")
    base["end_date"] = base["delist_date"].fillna(cutoff)

    base = base.dropna(subset=["ticker", "list_date", "end_date"]).copy()
    base = base[base["end_date"] >= base["list_date"]].copy()
    base = base.sort_values(["ticker", "list_date"]).drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)

    rows: list[dict[str, str]] = []
    for _, r in base.iterrows():
        for d in pd.bdate_range(r["list_date"].date(), r["end_date"].date()):
            rows.append({"ticker": r["ticker"], "date": d.strftime("%Y-%m-%d")})

    tasks = pd.DataFrame(rows)
    meta = {
        "mode": "lifecycle",
        "universe_path": str(universe_path),
        "lifecycle_path": str(lifecycle_path),
        "tickers_count": int(base["ticker"].nunique()),
        "tasks_total": int(len(tasks)),
        "date_min": str(tasks["date"].min()) if len(tasks) else None,
        "date_max": str(tasks["date"].max()) if len(tasks) else None,
    }
    return tasks, base, meta


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--mode", choices=["smoke", "lifecycle"], default="smoke")
    ap.add_argument("--ticker", default="ABAX")
    ap.add_argument("--date", default="2013-10-14")
    ap.add_argument(
        "--universe",
        default=r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper_excluding_ohlcv_1m_missing_vs_daily_ge_2B_or_null.parquet",
    )
    ap.add_argument(
        "--lifecycle",
        default=r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\official_lifecycle_compiled.csv",
    )
    ap.add_argument("--cutoff", default="2026-03-13")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "smoke":
        tasks, tickers, meta = build_smoke_tasks(args.ticker, args.date)
    else:
        tasks, tickers, meta = build_lifecycle_tasks(Path(args.universe), Path(args.lifecycle), pd.Timestamp(args.cutoff))

    meta["run_id"] = args.run_id
    tasks_csv = inputs_dir / "tasks_trades_ticks.csv"
    tickers_csv = inputs_dir / "tickers_trades_ticks.csv"
    meta_json = inputs_dir / "tasks_trades_ticks_meta.json"

    tasks.to_csv(tasks_csv, index=False)
    tickers.to_csv(tickers_csv, index=False)
    meta_json.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    print(tasks_csv)
    print(tickers_csv)
    print(meta_json)
    print(json.dumps(meta, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
