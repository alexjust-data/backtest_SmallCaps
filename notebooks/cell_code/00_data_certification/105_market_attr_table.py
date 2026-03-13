from pathlib import Path
import json
import pyarrow.parquet as pq


def _print_field_dictionary(columns):
    definitions = {
        "timestamp": {
            "definicion": "Timestamp SIP del trade.",
            "tipo_esperado": "integer (epoch ns)",
            "origen_oficial": "Stocks Trades endpoint / trades_v1",
            "mapeo_api": "sip_timestamp / t",
            "nota_local": "En market.parquet suele venir como int64 en nanosegundos.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/trades",
            "certeza": "oficial+inferencia_local",
        },
        "price": {
            "definicion": "Precio ejecutado del trade.",
            "tipo_esperado": "number",
            "origen_oficial": "Stocks Trades endpoint / trades_v1",
            "mapeo_api": "price / p",
            "nota_local": "Precio por tick de transaccion.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/trades",
            "certeza": "oficial",
        },
        "size": {
            "definicion": "Cantidad de acciones negociadas en el trade.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Trades endpoint / trades_v1",
            "mapeo_api": "size / s",
            "nota_local": "Volumen unitario del trade.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/trades",
            "certeza": "oficial",
        },
        "exchange": {
            "definicion": "Identificador de exchange donde se reporta la ejecucion.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Trades endpoint / reference exchanges",
            "mapeo_api": "exchange / x",
            "nota_local": "Se puede mapear con tablas de exchanges de referencia.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/trades",
            "certeza": "oficial",
        },
        "conditions": {
            "definicion": "Condiciones de ejecucion del trade (trade conditions).",
            "tipo_esperado": "string/list",
            "origen_oficial": "Stocks Trades endpoint",
            "mapeo_api": "conditions / c",
            "nota_local": "En este parquet aparece como texto; puede variar por pipeline.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/trades",
            "certeza": "oficial+inferencia_local",
        },
        "year": {
            "definicion": "Ano de particion del almacenamiento local.",
            "tipo_esperado": "integer/category",
            "origen_oficial": "Particion local",
            "mapeo_api": "N/A",
            "nota_local": "No es variable de mercado; organiza carpetas.",
            "source_url": "N/A",
            "certeza": "particion_local",
        },
        "month": {
            "definicion": "Mes de particion del almacenamiento local.",
            "tipo_esperado": "integer/category",
            "origen_oficial": "Particion local",
            "mapeo_api": "N/A",
            "nota_local": "No es variable de mercado; organiza carpetas.",
            "source_url": "N/A",
            "certeza": "particion_local",
        },
        "day": {
            "definicion": "Dia de particion del almacenamiento local.",
            "tipo_esperado": "string/date/category",
            "origen_oficial": "Particion local",
            "mapeo_api": "N/A",
            "nota_local": "No es variable de mercado; organiza carpetas.",
            "source_url": "N/A",
            "certeza": "particion_local",
        },
    }

    selected = {k: v for k, v in definitions.items() if k in columns}
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
    _print_field_dictionary(t.schema.names)


FILE = Path(r"C:\\TSIS_Data\\data\\trades_ticks_2004_2018\\AACB\\year=2004\\month=01\\day=2004-01-02\\market.parquet")
if not FILE.exists():
    for root in [Path(r"C:\\TSIS_Data\\data\\trades_ticks_2019_2025"), Path(r"C:\\TSIS_Data\\data\\trades_ticks_2004_2018")]:
        if root.exists():
            cand = next(root.glob("*/year=*/month=*/day=*/*.parquet"), None)
            if cand is not None:
                FILE = cand
                break
_show_attr_table(FILE, "market.parquet")
