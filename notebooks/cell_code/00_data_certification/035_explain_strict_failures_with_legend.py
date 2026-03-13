from pathlib import Path
import pandas as pd


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _find_col(df: pd.DataFrame, candidates):
    cols = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        key = str(c).strip().lower()
        if key in cols:
            return cols[key]
    return None


BASE = Path(globals().get("BASE", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\granular_strict"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_strict_events.csv"))
RETRY_CSV = Path(globals().get("RETRY_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_strict.csv"))

CAUSE_SUMMARY_CSV = BASE / "quotes_agent_strict_errors_cause_summary.csv"
ERRORS_CSV = BASE / "quotes_agent_strict_errors_granular.csv"

legend = pd.DataFrame([
    {"cause": "parquet_unreadable", "tipo": "HARD_FAIL", "definicion": "El parquet no se puede abrir/leer."},
    {"cause": "zero_byte_file", "tipo": "HARD_FAIL", "definicion": "Archivo de 0 bytes."},
    {"cause": "invalid_partition_path", "tipo": "HARD_FAIL", "definicion": "Ruta no cumple patron ticker/year/month/day/quotes.parquet."},
    {"cause": "zero_rows", "tipo": "HARD_FAIL", "definicion": "Parquet leido pero sin filas."},
    {"cause": "missing_required_columns", "tipo": "HARD_FAIL", "definicion": "Faltan columnas requeridas del esquema estricto."},
    {"cause": "negative_prices_any_row", "tipo": "HARD_FAIL", "definicion": "Hay filas con bid/ask negativos."},
    {"cause": "crossed_ratio_gt_threshold", "tipo": "HARD_FAIL", "definicion": "Porcentaje de bid>ask supera el umbral estricto."},
    {"cause": "dtype_mismatch", "tipo": "SOFT_FAIL", "definicion": "Tipo de dato distinto al esperado."},
    {"cause": "crossed_rows_present_but_under_threshold", "tipo": "SOFT_FAIL", "definicion": "Hay bid>ask, pero por debajo del umbral."},
    {"cause": "soft_rule_eval_error", "tipo": "SOFT_FAIL", "definicion": "Error evaluando regla soft; archivo no necesariamente corrupto."},
])

print("Leyenda de causas:")
try:
    display(legend)
except Exception:
    print(legend.to_string(index=False))

if not EVENTS_CSV.exists():
    raise FileNotFoundError(f"No existe events csv: {EVENTS_CSV}")

try:
    events = pd.read_csv(EVENTS_CSV)
except Exception:
    events = pd.read_csv(EVENTS_CSV, engine="python", on_bad_lines="skip")
events = _norm_cols(events)

if RETRY_CSV.exists():
    try:
        retry = pd.read_csv(RETRY_CSV)
    except Exception:
        retry = pd.read_csv(RETRY_CSV, engine="python", on_bad_lines="skip")
else:
    retry = pd.DataFrame(columns=["file"])
retry = _norm_cols(retry)

sev_col_events = _find_col(events, ["severity", "severity_y", "severity_x"])
file_col_events = _find_col(events, ["file", "path"])
ts_col_events = _find_col(events, ["processed_at_utc", "processed_at", "timestamp_utc"])

print("\nResumen por severidad (events):")
if sev_col_events is None:
    print(f"No existe columna severity en events. Columnas detectadas: {list(events.columns)}")
else:
    sev_df = events[sev_col_events].value_counts(dropna=False).rename_axis("severity").reset_index(name="count")
    try:
        display(sev_df)
    except Exception:
        print(sev_df.to_string(index=False))

print("\nRetry queue total:")
print(len(retry))

file_col_retry = _find_col(retry, ["file", "path"])
if not retry.empty and file_col_retry and file_col_events and sev_col_events:
    ev_tmp = events.copy()
    if ts_col_events:
        ev_tmp = ev_tmp.sort_values(ts_col_events)
    ev_last = ev_tmp.drop_duplicates(file_col_events, keep="last")
    rq_join = retry.merge(
        ev_last[[file_col_events, sev_col_events]].rename(columns={file_col_events: file_col_retry}),
        on=file_col_retry,
        how="left",
    )
    sev_col_join = _find_col(rq_join, ["severity", "severity_y", "severity_x"])
    print("\nRetry queue por severidad del ultimo estado:")
    if sev_col_join is None:
        print(f"No se pudo determinar columna severity en retry join. Columnas: {list(rq_join.columns)}")
    else:
        rq_df = rq_join[sev_col_join].value_counts(dropna=False).rename_axis("severity").reset_index(name="count")
        try:
            display(rq_df)
        except Exception:
            print(rq_df.to_string(index=False))
else:
    print("\nRetry queue por severidad del ultimo estado:")
    print("No aplica: faltan columnas requeridas (file/severity) o retry vacio.")

if CAUSE_SUMMARY_CSV.exists():
    cause_summary = pd.read_csv(CAUSE_SUMMARY_CSV)
    cause_summary = _norm_cols(cause_summary)
    cause_col = _find_col(cause_summary, ["cause", "causa"])
    count_col = _find_col(cause_summary, ["count", "conteo", "n"])
    if cause_col is None:
        print(f"\nNo se encontro columna de causa en {CAUSE_SUMMARY_CSV}. Columnas: {list(cause_summary.columns)}")
    else:
        cause_summary["cause_clean"] = (
            cause_summary[cause_col].astype(str)
            .str.replace(r"^\['", "", regex=True)
            .str.replace(r"'\]$", "", regex=True)
        )
        top_causes = cause_summary.merge(
            legend[["cause", "tipo", "definicion"]],
            left_on="cause_clean",
            right_on="cause",
            how="left",
        ).drop(columns=["cause"], errors="ignore")
        print("\nTop causas (con definicion):")
        if count_col and count_col in top_causes.columns:
            top_causes = top_causes.sort_values(count_col, ascending=False)
        try:
            display(top_causes.head(30))
        except Exception:
            print(top_causes.head(30).to_string(index=False))
else:
    print(f"\nNo existe: {CAUSE_SUMMARY_CSV}")

if ERRORS_CSV.exists():
    errors = pd.read_csv(ERRORS_CSV)
    errors = _norm_cols(errors)
    print("\nTop archivos con mas crossed_ratio_pct:")
    cols = [
        c for c in [
            "ticker",
            "year",
            "month",
            "day",
            "severity",
            "crossed_rows",
            "crossed_ratio_pct",
            "issues",
            "warns",
            "file",
        ] if c in errors.columns
    ]
    if cols and "crossed_ratio_pct" in errors.columns:
        top_err = errors[cols].sort_values("crossed_ratio_pct", ascending=False)
    elif cols:
        top_err = errors[cols]
    else:
        top_err = errors.head(20)
    try:
        display(top_err.head(20))
    except Exception:
        print(top_err.head(20).to_string(index=False))
else:
    print(f"\nNo existe: {ERRORS_CSV}")
