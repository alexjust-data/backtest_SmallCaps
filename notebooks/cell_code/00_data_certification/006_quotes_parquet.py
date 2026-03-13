from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

FILE = Path(r"C:\\TSIS_Data\\data\\quotes_p95\\AABA\\year=2019\\month=01\\day=02\\quotes.parquet")

if not FILE.exists():
    roots = [
        Path(r"C:\\TSIS_Data\\data\\quotes_p95"),
        Path(r"C:\\TSIS_Data\\data\\quotes_p95_2019_2025"),
        Path(r"C:\\TSIS_Data\\data\\quotes_p95_2004_2018"),
    ]
    found = None
    for root in roots:
        if not root.exists():
            continue
        found = next(root.glob("*/year=*/month=*/day=*/quotes.parquet"), None)
        if found is not None:
            break
    if found is None:
        raise FileNotFoundError("No quotes.parquet found in quotes_p95*")
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
if 'sip_timestamp' in df.columns:
    ts = pd.to_datetime(df['sip_timestamp'], unit='ns', errors='coerce')
    print("Rango sip_timestamp(ns):", ts.min(), "->", ts.max())
