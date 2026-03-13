from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

CANDIDATES = [
    Path(r"C:\\TSIS_Data\\data\\additional\\corporate_actions\\dividends.parquet"),
    Path(r"C:\\TSIS_Data\\data\\additional\\dividends.parquet"),
    Path(r"C:\\TSIS_Data\\data\\corporate_actions\\dividends.parquet"),
]
FILE = None
for c in CANDIDATES:
    if c.exists():
        FILE = c
        break

if FILE is None:
    print("DIVIDENDS FILE NOT FOUND")
    print("Searched paths:")
    for c in CANDIDATES:
        print("-", c)
else:
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
    for col in ["ex_dividend_date", "pay_date", "record_date", "declaration_date", "date"]:
        if col in df.columns:
            d = pd.to_datetime(df[col], errors='coerce')
            print(f"\nRango {col}:", d.min(), "->", d.max())
