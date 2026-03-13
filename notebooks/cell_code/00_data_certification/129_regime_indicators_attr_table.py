from pathlib import Path
import json
import pyarrow.parquet as pq

DATASET_LABEL = "Regime Indicators (local derivado)"
SOURCE_URL = "N/A"


def _guess_definition(col: str):
    m = {
        "ticker": "Simbolo del activo.",
        "timestamp": "Marca temporal del registro.",
        "datetime": "Fecha-hora del registro.",
        "date": "Fecha del registro.",
        "open": "Precio de apertura de la ventana.",
        "high": "Precio maximo de la ventana.",
        "low": "Precio minimo de la ventana.",
        "close": "Precio de cierre de la ventana.",
        "volume": "Volumen agregado en la ventana temporal.",
        "vwap": "Precio medio ponderado por volumen.",
    }
    return m.get(col, f"Campo '{col}' presente en este parquet.")


def _guess_mapping(col: str):
    api = {
        "open": "o",
        "high": "h",
        "low": "l",
        "close": "c",
        "volume": "v",
        "vwap": "vw",
        "timestamp": "t",
    }
    return api.get(col, "N/A")


def _guess_certeza(col: str):
    return "inferido_local"


def _print_field_dictionary(table):
    selected = {}
    for f in table.schema:
        col = f.name
        selected[col] = {
            "definicion": _guess_definition(col),
            "tipo_esperado": str(f.type),
            "origen_oficial": DATASET_LABEL,
            "mapeo_api": _guess_mapping(col),
            "nota_local": "Dataset derivado local; no mapeo 1:1 oficial para todas las columnas.",
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


ROOT = Path(r"C:\\TSIS_Data\\data\\regime_indicators")
FILE = next(ROOT.rglob("*.parquet"), Path()) if ROOT.exists() else Path()
_show_attr_table(FILE, "regime_indicators sample")
