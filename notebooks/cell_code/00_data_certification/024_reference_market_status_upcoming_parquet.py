from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

FILE = Path(r"C:\\TSIS_Data\\data\\reference\\market_status_upcoming.parquet")
if not FILE.exists():
    print("FILE NOT FOUND:", FILE)
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
    if 'date' in df.columns:
        d = pd.to_datetime(df['date'], errors='coerce')
        print('\nRango date:', d.min(), '->', d.max())
