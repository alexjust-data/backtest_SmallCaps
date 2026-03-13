from pathlib import Path
import json
import pyarrow.parquet as pq

DATASET_LABEL = "Corporate Actions - Dividends"
SOURCE_URL = "https://polygon.io/docs/stocks/get_v3_reference_dividends"


def _guess_definition(col: str):
    m = {
        "ticker": "Simbolo del activo.",
        "date": "Fecha asociada al evento.",
        "year": "Ano de particion local.",
        "month": "Mes de particion local.",
        "day": "Dia de particion local.",
    }
    return m.get(col, f"Campo '{col}' presente en este parquet.")


def _guess_mapping(col: str):
    api = {"ticker": "ticker/sym", "date": "date"}
    return api.get(col, "N/A")


def _guess_certeza(col: str):
    if col in {"year", "month", "day"}:
        return "particion_local"
    return "oficial+inferencia_local"


def _print_field_dictionary(table):
    selected = {}
    for f in table.schema:
        col = f.name
        selected[col] = {
            "definicion": _guess_definition(col),
            "tipo_esperado": str(f.type),
            "origen_oficial": DATASET_LABEL,
            "mapeo_api": _guess_mapping(col),
            "nota_local": "Documentacion tecnica unificada para certificacion; revisar mapeo fino si aplica.",
            "source_url": SOURCE_URL,
            "certeza": _guess_certeza(col),
        }

    print("\nDiccionario tecnico de variables (oficial + mapeo local):")
    print(json.dumps(selected, indent=2, ensure_ascii=False))


def _show_attr_table(file_path: Path, title: str = ""):
    if (not file_path.exists()) or (not file_path.is_file()):
        print(f"FILE NOT FOUND: {file_path}")
        return

    print(f"FILE: {file_path}")
    meta = pq.read_metadata(file_path)
    print(f"rows: {meta.num_rows}")
    print(f"row_groups: {meta.num_row_groups}")
    print(f"columns: {meta.num_columns}")

    t = pq.read_table(file_path)

    if title:
        print(title)
    _print_field_dictionary(t)


cands = [
    Path(r"C:\\TSIS_Data\\data\\additional\\corporate_actions\\dividends.parquet"),
    Path(r"C:\\TSIS_Data\\data\\additional\\dividends.parquet"),
    Path(r"C:\\TSIS_Data\\data\\corporate_actions\\dividends.parquet"),
]
FILE = next((c for c in cands if c.exists()), Path())
_show_attr_table(FILE, "dividends.parquet")
