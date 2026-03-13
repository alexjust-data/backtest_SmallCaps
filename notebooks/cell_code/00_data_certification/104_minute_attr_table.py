from pathlib import Path
import json
import pyarrow.parquet as pq


def _print_field_dictionary(columns):
    definitions = {
        'ticker': {
            'definicion': 'Simbolo del activo (equity) al que pertenece la barra.',
            'tipo_esperado': 'string',
            'origen_oficial': 'Flat Files minute aggs sample / REST aggregates',
            'mapeo_api': 'ticker / sym',
            'nota_local': 'Identificador del ticker en el parquet local.',
            'source_url': 'https://massive.com/docs/flat-files/stocks/minute-aggregates',
            'certeza': 'oficial'
        },
        'open': {
            'definicion': 'Precio de apertura de la ventana de agregacion (1 minuto).',
            'tipo_esperado': 'number',
            'origen_oficial': 'REST aggregates custom bars',
            'mapeo_api': 'o',
            'nota_local': 'Corresponde al open de la vela por minuto.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        'high': {
            'definicion': 'Precio maximo dentro de la ventana de agregacion.',
            'tipo_esperado': 'number',
            'origen_oficial': 'REST aggregates custom bars',
            'mapeo_api': 'h',
            'nota_local': 'Maximo de la vela por minuto.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        'low': {
            'definicion': 'Precio minimo dentro de la ventana de agregacion.',
            'tipo_esperado': 'number',
            'origen_oficial': 'REST aggregates custom bars',
            'mapeo_api': 'l',
            'nota_local': 'Minimo de la vela por minuto.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        'close': {
            'definicion': 'Precio de cierre de la ventana de agregacion.',
            'tipo_esperado': 'number',
            'origen_oficial': 'REST aggregates custom bars',
            'mapeo_api': 'c',
            'nota_local': 'Close de la vela por minuto.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        'volume': {
            'definicion': 'Volumen negociado en la ventana (suma de size de trades elegibles).',
            'tipo_esperado': 'number',
            'origen_oficial': 'REST aggregates custom bars / Flat Files minute aggs',
            'mapeo_api': 'v',
            'nota_local': 'Volumen por minuto en el parquet local.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        'vwap': {
            'definicion': 'Volume Weighted Average Price de la ventana.',
            'tipo_esperado': 'number',
            'origen_oficial': 'REST aggregates custom bars',
            'mapeo_api': 'vw',
            'nota_local': 'VWAP por minuto.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        'transactions': {
            'definicion': 'Numero de transacciones que componen la agregacion.',
            'tipo_esperado': 'integer',
            'origen_oficial': 'REST aggregates custom bars / Flat Files sample',
            'mapeo_api': 'n / transactions',
            'nota_local': 'Conteo de trades en el minuto.',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial'
        },
        't': {
            'definicion': 'Timestamp de inicio de la ventana de agregacion.',
            'tipo_esperado': 'integer (epoch)',
            'origen_oficial': 'REST aggregates custom bars',
            'mapeo_api': 't',
            'nota_local': 'En este dataset local se observa en milisegundos Unix. Inferido por valores y schema timestamp[ms].',
            'source_url': 'https://polygon.io/docs/rest/stocks/aggregates/custom-bars',
            'certeza': 'oficial+inferencia_local'
        },
        'timestamp': {
            'definicion': 'Marca temporal de la barra en formato datetime del parquet local.',
            'tipo_esperado': 'timestamp',
            'origen_oficial': 'Derivado local de window_start/t',
            'mapeo_api': 'window_start / t (normalizado)',
            'nota_local': 'Representacion legible del inicio de ventana por minuto.',
            'source_url': 'https://massive.com/docs/flat-files/stocks/minute-aggregates',
            'certeza': 'inferido_local'
        },
        'date': {
            'definicion': 'Fecha de mercado (dia) asociada a la barra.',
            'tipo_esperado': 'date',
            'origen_oficial': 'Derivado local',
            'mapeo_api': 'derivado de timestamp',
            'nota_local': 'Campo auxiliar para particion/analitica diaria.',
            'source_url': 'N/A',
            'certeza': 'inferido_local'
        },
        'minute': {
            'definicion': 'Etiqueta textual del minuto (YYYY-MM-DD HH:MM).',
            'tipo_esperado': 'string',
            'origen_oficial': 'Derivado local',
            'mapeo_api': 'derivado de timestamp',
            'nota_local': 'Campo auxiliar de presentacion.',
            'source_url': 'N/A',
            'certeza': 'inferido_local'
        },
        'year': {
            'definicion': 'Ano de particion del almacenamiento local.',
            'tipo_esperado': 'integer/category',
            'origen_oficial': 'Particion local',
            'mapeo_api': 'N/A',
            'nota_local': 'No es variable de mercado; es organizacion de carpetas.',
            'source_url': 'N/A',
            'certeza': 'particion_local'
        },
        'month': {
            'definicion': 'Mes de particion del almacenamiento local.',
            'tipo_esperado': 'integer/category',
            'origen_oficial': 'Particion local',
            'mapeo_api': 'N/A',
            'nota_local': 'No es variable de mercado; es organizacion de carpetas.',
            'source_url': 'N/A',
            'certeza': 'particion_local'
        },
    }

    selected = {k: v for k, v in definitions.items() if k in columns}
    print('\nDiccionario tecnico de variables (oficial + mapeo local):')
    print(json.dumps(selected, indent=2, ensure_ascii=False))


def _show_attr_table(file_path: Path, title: str = ''):
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


FILE = Path(r"C:\TSIS_Data\data\ohlcv_intraday_1m\2004_2018\AACB\year=2004\month=01\minute.parquet")
_show_attr_table(FILE, "minute.parquet")
