from pathlib import Path
import json
import pyarrow.parquet as pq


def _print_field_dictionary(columns):
    definitions = {
        "ask_exchange": {
            "definicion": "Exchange del mejor ask (oferta) en NBBO.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "ask_exchange / ax",
            "nota_local": "Codigo de venue para la punta ask.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "ask_price": {
            "definicion": "Precio del mejor ask en ese instante.",
            "tipo_esperado": "number",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "ask_price / ap",
            "nota_local": "Top-of-book lado vendedor.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "ask_size": {
            "definicion": "Tamano del mejor ask.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "ask_size / as",
            "nota_local": "Cantidad de acciones disponibles al ask.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "bid_exchange": {
            "definicion": "Exchange del mejor bid (demanda) en NBBO.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "bid_exchange / bx",
            "nota_local": "Codigo de venue para la punta bid.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "bid_price": {
            "definicion": "Precio del mejor bid en ese instante.",
            "tipo_esperado": "number",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "bid_price / bp",
            "nota_local": "Top-of-book lado comprador.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "bid_size": {
            "definicion": "Tamano del mejor bid.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "bid_size / bs",
            "nota_local": "Cantidad de acciones disponibles al bid.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "conditions": {
            "definicion": "Lista de condiciones de quote publicadas por SIP.",
            "tipo_esperado": "list[integer]",
            "origen_oficial": "Stocks Quotes endpoint",
            "mapeo_api": "conditions / c",
            "nota_local": "Codigos de condicion para filtrar/interpretar quote.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "participant_timestamp": {
            "definicion": "Timestamp del participante/venue que origina el quote.",
            "tipo_esperado": "integer (epoch ns)",
            "origen_oficial": "Stocks Quotes endpoint",
            "mapeo_api": "participant_timestamp / y",
            "nota_local": "Puede diferir del timestamp SIP.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "sequence_number": {
            "definicion": "Secuencia monotona del mensaje dentro del feed.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Quotes endpoint",
            "mapeo_api": "sequence_number / q",
            "nota_local": "Ayuda a ordenar eventos con mismo timestamp.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "timestamp": {
            "definicion": "Timestamp SIP del quote.",
            "tipo_esperado": "integer (epoch ns)",
            "origen_oficial": "Stocks Quotes endpoint / quotes_v1",
            "mapeo_api": "sip_timestamp / t",
            "nota_local": "Marca temporal principal del evento NBBO.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial+inferencia_local",
        },
        "tape": {
            "definicion": "Tape SIP (A/B/C) codificado numericamente.",
            "tipo_esperado": "integer",
            "origen_oficial": "Stocks Quotes endpoint",
            "mapeo_api": "tape / z",
            "nota_local": "Clasifica la consolidacion SIP del quote.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
        },
        "indicators": {
            "definicion": "Indicadores adicionales del quote.",
            "tipo_esperado": "list[integer]",
            "origen_oficial": "Stocks Quotes endpoint",
            "mapeo_api": "indicators / i",
            "nota_local": "Banderas de calidad/estado del quote segun feed.",
            "source_url": "https://polygon.io/docs/rest/stocks/trades-quotes/quotes",
            "certeza": "oficial",
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
            "tipo_esperado": "integer/string/category",
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


FILE = Path(r"C:\\TSIS_Data\\data\\quotes_p95\\AABA\\year=2019\\month=01\\day=02\\quotes.parquet")
if not FILE.exists():
    for root in [
        Path(r"C:\\TSIS_Data\\data\\quotes_p95"),
        Path(r"C:\\TSIS_Data\\data\\quotes_p95_2019_2025"),
        Path(r"C:\\TSIS_Data\\data\\quotes_p95_2004_2018"),
    ]:
        if root.exists():
            cand = next(root.glob("*/year=*/month=*/day=*/quotes.parquet"), None)
            if cand is not None:
                FILE = cand
                break
_show_attr_table(FILE, "quotes.parquet")
