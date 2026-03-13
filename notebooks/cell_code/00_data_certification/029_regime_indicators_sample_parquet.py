from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(r"C:\\TSIS_Data\\data\\regime_indicators")
if not ROOT.exists():
    raise FileNotFoundError(f"Folder not found: {ROOT}")
FILE = next(ROOT.rglob("*.parquet"), None)
if FILE is None:
    raise FileNotFoundError(f"No parquet files found in: {ROOT}")

print("FILE:", FILE)
meta = pq.read_metadata(FILE)
print("rows:", meta.num_rows)
print("row_groups:", meta.num_row_groups)
print("columns:", meta.num_columns)
print("\nSchema literal:")
t = pq.read_table(FILE)
print(t.schema)

df = t.to_pandas()
print("\nPrimeras 2 filas literales:")
print(df.head(2).to_string(index=False))
print("\nUltimas 2 filas literales:")
print(df.tail(2).to_string(index=False))
for col in ['date','timestamp','published_utc','datetime']:
    if col in df.columns:
        d = pd.to_datetime(df[col], errors='coerce', utc=True)
        print(f"\nRango {col}:", d.min(), "->", d.max())
