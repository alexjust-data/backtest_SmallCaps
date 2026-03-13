from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

HEAD_N = int(globals().get('HEAD_N', 2))
TRANSPOSE = bool(globals().get('TRANSPOSE', False))
SHOW_TAIL = bool(globals().get('SHOW_TAIL', True))


def _show_df(df: pd.DataFrame):
    view = df.T if TRANSPOSE else df
    try:
        display(view.style.hide(axis='index'))
    except Exception:
        try:
            display(view)
        except Exception:
            print(view.to_string(index=TRANSPOSE))


def _show_literal(file_path: Path, title: str = ''):
    if (not file_path.exists()) or (not file_path.is_file()):
        print(f"FILE NOT FOUND: {file_path}")
        return

    print(f"FILE: {file_path}")
    meta = pq.read_metadata(file_path)
    print(f"rows: {meta.num_rows}")
    print(f"row_groups: {meta.num_row_groups}")
    print(f"columns: {meta.num_columns}")

    t = pq.read_table(file_path)
    print("\nSchema literal:")
    print(t.schema)

    df = t.to_pandas()

    print(f"\nPrimeras {HEAD_N} filas literales:")
    _show_df(df.head(HEAD_N))

    # rangos temporales comunes
    for col, unit in [('timestamp', 'ns'), ('sip_timestamp', 'ns'), ('published_utc', None), ('date', None), ('datetime', None), ('execution_date', None), ('listing_date', None), ('period_end', None), ('filing_date', None), ('settlement_date', None)]:
        if col in df.columns:
            if unit == 'ns':
                d = pd.to_datetime(df[col], unit='ns', errors='coerce')
            else:
                d = pd.to_datetime(df[col], errors='coerce', utc=False)
            print(f"Rango {col}: {d.min()} -> {d.max()}")

FILE = Path(r"C:\\TSIS_Data\\data\\additional\\economic\\inflation_expectations.parquet")
_show_literal(FILE, "inflation_expectations.parquet")
