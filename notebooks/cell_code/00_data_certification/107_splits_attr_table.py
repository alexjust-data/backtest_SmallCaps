from pathlib import Path
import json
import pyarrow.parquet as pq

DATASET_LABEL = "Corporate Actions - Splits"
SOURCE_URL = "https://polygon.io/docs/stocks/get_v3_reference_splits"


def _guess_definition(col: str):
    m = {
        "ticker": "Simbolo del activo.",
        "timestamp": "Marca temporal del evento en feed/SIP o derivado local.",
        "participant_timestamp": "Marca temporal del participante/venue.",
        "sip_timestamp": "Marca temporal SIP consolidada.",
        "sequence_number": "Secuencia del evento en el feed.",
        "price": "Precio del evento/trade.",
        "size": "Tamano/cantidad asociada al evento.",
        "volume": "Volumen agregado en la ventana temporal.",
        "vwap": "Precio medio ponderado por volumen.",
        "open": "Precio de apertura de la ventana.",
        "high": "Precio maximo de la ventana.",
        "low": "Precio minimo de la ventana.",
        "close": "Precio de cierre de la ventana.",
        "transactions": "Numero de transacciones agregadas.",
        "date": "Fecha asociada al registro.",
        "datetime": "Fecha-hora asociada al registro.",
        "year": "Ano de particion local.",
        "month": "Mes de particion local.",
        "day": "Dia de particion local.",
        "conditions": "Condiciones/codigos del evento segun feed.",
        "exchange": "Codigo de exchange/venue.",
        "ask_price": "Mejor precio ask (oferta).",
        "ask_size": "Tamano del mejor ask.",
        "bid_price": "Mejor precio bid (demanda).",
        "bid_size": "Tamano del mejor bid.",
    }
    return m.get(col, f"Campo '{col}' presente en este parquet.")


def _guess_mapping(col: str):
    api = {
        "open": "o", "high": "h", "low": "l", "close": "c", "volume": "v", "vwap": "vw",
        "transactions": "n", "timestamp": "t", "price": "p", "size": "s", "ticker": "ticker/sym",
        "bid_price": "bp", "bid_size": "bs", "ask_price": "ap", "ask_size": "as",
    }
    return api.get(col, "N/A")


def _guess_certeza(col: str):
    if col in {"year", "month", "day"}:
        return "particion_local"
    if SOURCE_URL == "N/A":
        return "inferido_local"
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

FILE = Path(r"C:\\TSIS_Data\\data\\additional\\corporate_actions\\splits.parquet")
_show_attr_table(FILE, "splits.parquet")
