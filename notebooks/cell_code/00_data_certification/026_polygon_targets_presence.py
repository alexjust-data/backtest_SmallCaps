from pathlib import Path
import pandas as pd

DATA_ROOT = Path(r"C:\\TSIS_Data\\data")

targets = [
    ("day_aggs_v1 (direct local folder/file)", [DATA_ROOT / "day_aggs", DATA_ROOT / "day_aggs_v1", DATA_ROOT / "additional" / "day_aggs.parquet"]),
    ("minute_aggs (local)", [DATA_ROOT / "ohlcv_intraday_1m"]),
    ("trades tick-level (local)", [DATA_ROOT / "trades_ticks_2004_2018", DATA_ROOT / "trades_ticks_2019_2025"]),
    ("quotes tick-level (local)", [DATA_ROOT / "quotes_p95", DATA_ROOT / "quotes_p95_2004_2018", DATA_ROOT / "quotes_p95_2019_2025"]),
    ("dividends", [DATA_ROOT / "additional" / "corporate_actions" / "dividends.parquet", DATA_ROOT / "additional" / "dividends.parquet"]),
    ("splits", [DATA_ROOT / "additional" / "corporate_actions" / "splits.parquet"]),
    ("ticker_events", [DATA_ROOT / "additional" / "corporate_actions" / "ticker_events.parquet"]),
    ("all_tickers catalog", [DATA_ROOT / "reference" / "all_tickers.parquet", DATA_ROOT / "reference" / "tickers.parquet"]),
    ("snapshots/open-close persisted", [DATA_ROOT / "snapshots", DATA_ROOT / "open_close"]),
    ("technical indicators persisted", [DATA_ROOT / "technical_indicators", DATA_ROOT / "indicators", DATA_ROOT / "regime_indicators"]),
]
rows=[]
for name,cands in targets:
    ex=[str(c) for c in cands if c.exists()]
    rows.append({'target':name,'present':len(ex)>0,'existing_paths':' | '.join(ex) if ex else ''})
out=pd.DataFrame(rows)
print(out.to_string(index=False))
