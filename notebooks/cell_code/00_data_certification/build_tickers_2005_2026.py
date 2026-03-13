from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_ALL = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_all.parquet")
DEFAULT_OUT = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-all", default=str(DEFAULT_ALL))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--start", default="2005-01-01")
    ap.add_argument("--end", default="2026-12-31")
    args, _ = ap.parse_known_args()

    start = pd.to_datetime(args.start).date()
    end = pd.to_datetime(args.end).date()

    d = pd.read_parquet(args.in_all)
    d["first_seen_date"] = pd.to_datetime(d["first_seen_date"], errors="coerce").dt.date
    d["last_seen_date"] = pd.to_datetime(d["last_seen_date"], errors="coerce").dt.date

    # Intersección de intervalos: [first_seen, last_seen] intersecta [start, end]
    m = (
        d["first_seen_date"].notna()
        & d["last_seen_date"].notna()
        & (d["first_seen_date"] <= end)
        & (d["last_seen_date"] >= start)
    )
    out = d[m].copy()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)

    print(f"input_rows={len(d):,}")
    print(f"output_rows={len(out):,}")
    print(f"window={start}..{end}")
    print(f"saved={out_path}")


if __name__ == "__main__":
    main()

