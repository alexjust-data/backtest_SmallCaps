from pathlib import Path
import pyarrow.parquet as pq

P = Path(r"C:\\TSIS_Data\\data\\ohlcv_intraday_1m\\2004_2018\\AACB\\year=2004\\month=01\\minute.parquet")

print("FILE:", P)
meta = pq.read_metadata(P)
print("rows:", meta.num_rows)
print("row_groups:", meta.num_row_groups)
print("columns:", meta.num_columns)
print("\nSchema literal:")
t = pq.read_table(P)
print(t.schema)
df = t.to_pandas().sort_values('timestamp')
print("\nPrimeras 2 filas literales:")
print(df.head(2).to_string(index=False))
print("\nUltimas 2 filas literales:")
print(df.tail(2).to_string(index=False))
