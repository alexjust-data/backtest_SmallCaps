from pathlib import Path
import subprocess
import pandas as pd

DATA_ROOT = Path(r"C:\TSIS_Data\data")

try:
    display  # type: ignore[name-defined]
except NameError:
    def display(obj):  # type: ignore[override]
        try:
            print(obj.to_string(index=False))
        except Exception:
            print(obj)


def _count_parquet_fast(path: Path):
    if not path.exists():
        return 0
    try:
        p = subprocess.run(
            ["rg", "--files", str(path), "-g", "*.parquet"],
            capture_output=True,
            text=True,
            timeout=90,
        )
        if p.returncode not in (0, 1):
            return -1
        out = p.stdout.strip()
        return 0 if not out else out.count("\n") + 1
    except Exception:
        return -1


def _years_minute(root: Path):
    # ohlcv_intraday_1m/<era>/<ticker>/year=YYYY/month=MM/minute.parquet
    if not root.exists():
        return []
    years = set()
    for era in root.iterdir():
        if not era.is_dir():
            continue
        for ticker in era.iterdir():
            if not ticker.is_dir():
                continue
            for y in ticker.glob("year=*"):
                try:
                    years.add(int(y.name.split("=")[1]))
                except Exception:
                    pass
    return sorted(years)


def _years_ticker_year(root: Path):
    # trades/quotes roots: <root>/<ticker>/year=YYYY/...
    if not root.exists():
        return []
    years = set()
    for ticker in root.iterdir():
        if not ticker.is_dir():
            continue
        for y in ticker.glob("year=*"):
            try:
                years.add(int(y.name.split("=")[1]))
            except Exception:
                pass
    return sorted(years)


targets = [
    {
        "target": "Minute Aggregates (flat files)",
        "polygon_scope": "us_stocks_sip/minute_aggs_v1",
        "local_paths": [DATA_ROOT / "ohlcv_intraday_1m"],
        "expected_type": "folder",
        "years_fn": _years_minute,
    },
    {
        "target": "Day Aggregates (flat files)",
        "polygon_scope": "us_stocks_sip/day_aggs_v1",
        "local_paths": [DATA_ROOT / "ohlcv_daily", DATA_ROOT / "ohlcv_day"],
        "expected_type": "folder",
        "years_fn": _years_ticker_year,
    },
    {
        "target": "Second Aggregates (API/stream)",
        "polygon_scope": "Stocks aggregates (1-second bars)",
        "local_paths": [DATA_ROOT / "ohlcv_intraday_1s"],
        "expected_type": "folder",
        "years_fn": _years_ticker_year,
    },
    {
        "target": "Trades (tick-level)",
        "polygon_scope": "us_stocks_sip/trades_v1",
        "local_paths": [DATA_ROOT / "trades_ticks_2004_2018", DATA_ROOT / "trades_ticks_2019_2025"],
        "expected_type": "folder",
        "years_fn": _years_ticker_year,
    },
    {
        "target": "Quotes (NBBO tick-level)",
        "polygon_scope": "us_stocks_sip/quotes_v1",
        "local_paths": [DATA_ROOT / "quotes_p95", DATA_ROOT / "quotes_p95_2004_2018", DATA_ROOT / "quotes_p95_2019_2025"],
        "expected_type": "folder",
        "years_fn": _years_ticker_year,
    },
    {
        "target": "Snapshots (REST)",
        "polygon_scope": "Stocks snapshots endpoints",
        "local_paths": [DATA_ROOT / "snapshots"],
        "expected_type": "folder",
        "years_fn": _years_ticker_year,
    },
    {
        "target": "Corporate Actions - Splits",
        "polygon_scope": "Reference Splits",
        "local_paths": [DATA_ROOT / "additional" / "corporate_actions" / "splits.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Corporate Actions - Dividends",
        "polygon_scope": "Reference Dividends",
        "local_paths": [DATA_ROOT / "additional" / "corporate_actions" / "dividends.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Reference - Ticker Events",
        "polygon_scope": "Ticker Events",
        "local_paths": [DATA_ROOT / "additional" / "corporate_actions" / "ticker_events.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Economic - Inflation",
        "polygon_scope": "Economic endpoints",
        "local_paths": [DATA_ROOT / "additional" / "economic" / "inflation.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Economic - Inflation Expectations",
        "polygon_scope": "Economic endpoints",
        "local_paths": [DATA_ROOT / "additional" / "economic" / "inflation_expectations.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Economic - Treasury Yields",
        "polygon_scope": "Economic endpoints",
        "local_paths": [DATA_ROOT / "additional" / "economic" / "treasury_yields.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "IPOs",
        "polygon_scope": "Reference IPOs",
        "local_paths": [DATA_ROOT / "additional" / "ipos" / "all_ipos.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "News",
        "polygon_scope": "Stocks News endpoint",
        "local_paths": [DATA_ROOT / "additional" / "news"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Fundamentals - Balance Sheets",
        "polygon_scope": "Fundamentals statements",
        "local_paths": [DATA_ROOT / "fundamentals" / "balance_sheets"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Fundamentals - Cash Flow Statements",
        "polygon_scope": "Fundamentals statements",
        "local_paths": [DATA_ROOT / "fundamentals" / "cash_flow_statements"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Fundamentals - Income Statements",
        "polygon_scope": "Fundamentals statements",
        "local_paths": [DATA_ROOT / "fundamentals" / "income_statements"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Fundamentals - Ratios",
        "polygon_scope": "Fundamentals ratios",
        "local_paths": [DATA_ROOT / "fundamentals" / "smallcap_ratios", DATA_ROOT / "fundamentals" / "financial_ratios"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Short Data - Short Interest",
        "polygon_scope": "Short Interest endpoint",
        "local_paths": [DATA_ROOT / "short_data" / "short_interest"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Short Data - Short Volume",
        "polygon_scope": "Short Volume endpoint",
        "local_paths": [DATA_ROOT / "short_data" / "short_volume"],
        "expected_type": "folder",
        "years_fn": None,
    },
    {
        "target": "Reference - Condition Codes",
        "polygon_scope": "Reference endpoints",
        "local_paths": [DATA_ROOT / "reference" / "condition_codes.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Reference - Exchanges",
        "polygon_scope": "Reference endpoints",
        "local_paths": [DATA_ROOT / "reference" / "exchanges.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Reference - Market Status Upcoming",
        "polygon_scope": "Reference endpoints",
        "local_paths": [DATA_ROOT / "reference" / "market_status_upcoming.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
    {
        "target": "Reference - Ticker Types",
        "polygon_scope": "Reference endpoints",
        "local_paths": [DATA_ROOT / "reference" / "ticker_types.parquet"],
        "expected_type": "file",
        "years_fn": None,
    },
]

rows = []
for t in targets:
    found_paths = [p for p in t["local_paths"] if p.exists()]
    exists = len(found_paths) > 0

    parquet_count = None
    years = []
    if exists and t["expected_type"] == "folder":
        counts = [_count_parquet_fast(p) for p in found_paths]
        parquet_count = -1 if any(c < 0 for c in counts) else sum(counts)
        if t["years_fn"] is not None:
            ys = set()
            for p in found_paths:
                ys.update(t["years_fn"](p))
            years = sorted(ys)

    rows.append(
        {
            "target": t["target"],
            "polygon_scope": t["polygon_scope"],
            "status_local": "OK" if exists else "MISSING",
            "paths_found": " | ".join(str(p) for p in found_paths) if exists else "",
            "parquet_count": parquet_count,
            "years_min": min(years) if years else None,
            "years_max": max(years) if years else None,
        }
    )

audit_df = pd.DataFrame(rows).sort_values(["status_local", "target"], ascending=[True, True]).reset_index(drop=True)
print("Polygon targets presence audit (max-plan scope, local evidence):")
display(audit_df)

missing_df = audit_df[audit_df["status_local"] == "MISSING"][["target", "polygon_scope"]].reset_index(drop=True)
print("\nMissing targets:")
display(missing_df if not missing_df.empty else pd.DataFrame([{"target": "None", "polygon_scope": "All present"}]))

missing_docs = {
    "Corporate Actions - Dividends": {
        "que_es": "Eventos de dividendos por ticker (cash amount, fechas ex-dividend, pay date, record date, etc.).",
        "canal": "REST (Reference / Corporate Actions)",
        "granularidad": "Evento corporativo (no barra temporal).",
        "historico": "Depende del plan; util para lifecycle y ajustes corporativos.",
        "doc_massive": "https://massive.com/docs/rest/stocks/corporate-actions/dividends",
    },
    "Day Aggregates (flat files)": {
        "que_es": "OHLCV diario por equity US en archivo diario bulk.",
        "canal": "Flat Files S3",
        "granularidad": "1 dia (daily bars).",
        "historico": "En Stocks Advanced: all history (segun tabla de plan en docs).",
        "doc_massive": "https://massive.com/docs/flat-files/stocks/day-aggregates",
    },
    "Second Aggregates (API/stream)": {
        "que_es": "Barras OHLCV por segundo en tiempo real para stocks.",
        "canal": "WebSocket",
        "granularidad": "1 segundo.",
        "historico": "No flat-file historico dedicado; para historico se reconstruye desde trades.",
        "doc_massive": "https://massive.com/docs/stocks/ws_stocks_a",
    },
    "Snapshots (REST)": {
        "que_es": "Snapshot consolidado del mercado/ticker con ultimo trade, quote y agregados recientes.",
        "canal": "REST",
        "granularidad": "Estado instantaneo (point-in-time).",
        "historico": "Endpoint de estado actual; no dataset historico bulk equivalente.",
        "doc_massive": "https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot",
    },
}

if not missing_df.empty:
    rows = []
    for _, r in missing_df.iterrows():
        m = missing_docs.get(r["target"])
        if m is None:
            rows.append(
                {
                    "target": r["target"],
                    "que_es": "Sin descripcion mapeada en esta version.",
                    "canal": "",
                    "granularidad": "",
                    "historico": "",
                    "doc_massive": "",
                }
            )
        else:
            rows.append(
                {
                    "target": r["target"],
                    "que_es": m["que_es"],
                    "canal": m["canal"],
                    "granularidad": m["granularidad"],
                    "historico": m["historico"],
                    "doc_massive": m["doc_massive"],
                }
            )
    missing_doc_df = pd.DataFrame(rows)
    print("\nDiccionario de faltantes (referencia Massive):")
    display(missing_doc_df)

print("\nNotes:")
print("- `status_local=OK` valida presencia local, no completitud histórica por ticker.")
print("- `parquet_count=-1` significa timeout/errores en conteo rápido.")
print("- En datasets masivos, este audit prioriza robustez/velocidad sobre conteo exacto exhaustivo.")
