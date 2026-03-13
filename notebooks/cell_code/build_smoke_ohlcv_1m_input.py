from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def build_smoke_input(
    source_parquet: Path,
    output_parquet: Path,
    tickers: Iterable[str] | None = None,
    top_n: int = 2,
) -> pd.DataFrame:
    d = pd.read_parquet(source_parquet, columns=["ticker"]).copy()
    d["ticker"] = d["ticker"].astype("string").str.strip().str.upper()
    d = d[d["ticker"].notna() & (d["ticker"] != "")]
    d = d.drop_duplicates(subset=["ticker"]).sort_values("ticker").reset_index(drop=True)

    if tickers:
        keep = [str(t).strip().upper() for t in tickers if str(t).strip()]
        out = d[d["ticker"].isin(keep)].copy()
    else:
        out = d.head(int(top_n)).copy()

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_parquet, index=False)
    return out


if __name__ == "__main__":
    SOURCE_PARQUET = Path(
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_missing_in_ohlcv_1m_vs_daily.parquet"
    )
    OUTPUT_PARQUET = Path(
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_missing_in_ohlcv_1m_vs_daily_smoke.parquet"
    )
    TICKERS = ["AAPL", "ABT"]

    out = build_smoke_input(
        source_parquet=SOURCE_PARQUET,
        output_parquet=OUTPUT_PARQUET,
        tickers=TICKERS,
        top_n=2,
    )
    print("smoke_input_rows:", len(out))
    print("tickers:", out["ticker"].tolist())
    print("output_parquet:", OUTPUT_PARQUET)
