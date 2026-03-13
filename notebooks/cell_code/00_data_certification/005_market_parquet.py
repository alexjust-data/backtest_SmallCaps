from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

FILE = Path(r"C:\\TSIS_Data\\data\\trades_ticks_2004_2018\\AACB\\year=2004\\month=01\\day=2004-01-02\\market.parquet")

if not FILE.exists():
    roots = [
        Path(r"C:\\TSIS_Data\\data\\trades_ticks_2019_2025"),
        Path(r"C:\\TSIS_Data\\data\\trades_ticks_2004_2018"),
    ]
    found = None
    for root in roots:
        if not root.exists():
            continue
        found = next(root.glob("*/year=*/month=*/day=*/*.parquet"), None)
        if found is not None:
            break
    if found is None:
        raise FileNotFoundError("No trade parquet found in trades_ticks_*")
    FILE = found

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

if 'timestamp' in df.columns:
    ts = pd.to_datetime(df['timestamp'], unit='ns', errors='coerce')
    print("\nRango timestamp(ns):", ts.min(), "->", ts.max())
