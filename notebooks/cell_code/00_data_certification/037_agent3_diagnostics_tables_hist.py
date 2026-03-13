from pathlib import Path
import ast
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

OUT_DIR = Path(globals().get("OUT_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\agent03_outputs"))
TOP_N = int(globals().get("TOP_N", 15))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 5)))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", ""))
BATCH_MANIFEST_CSV = Path(globals().get("BATCH_MANIFEST_CSV", ""))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", ""))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", ""))
DIAG_HEAD_N = globals().get("DIAG_HEAD_N", None)  # None = sin truncar
COV_HEAD_N = globals().get("COV_HEAD_N", None)    # None = sin truncar
MAX_CROSSED_RATIO_PCT = globals().get("MAX_CROSSED_RATIO_PCT", None)
# Bandas custom para causa under-threshold (en %). Se recortan automaticamente al umbral de corrida.
UNDER_THRESHOLD_BINS = globals().get(
    "UNDER_THRESHOLD_BINS",
    [0.0, 0.02, 0.08, 0.10, 0.125, 0.25, 0.375],
)


def _parse_listlike(v):
    if isinstance(v, list):
        return [str(x) for x in v if str(x) not in ("", "None")]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    try:
        x = ast.literal_eval(s)
        if isinstance(x, list):
            return [str(i) for i in x if str(i) not in ("", "None")]
        return [str(x)]
    except Exception:
        return [s]


def _print_table(df: pd.DataFrame):
    try:
        print(df.to_markdown(index=False))
    except Exception:
        print(df.to_string(index=False))


def _ensure_ticker_from_file(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ticker" not in out.columns and "file" in out.columns:
        ext = out["file"].astype(str).str.extract(
            r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
        )
        out = pd.concat([out, ext], axis=1)
    return out




cov_csv = OUT_DIR / "coverage_by_ticker.csv"
q_csv = OUT_DIR / "quality_summary_by_ticker.csv"
causes_csv = OUT_DIR / "causes_by_ticker.csv"
diag_csv = OUT_DIR / "ticker_diagnosis.csv"

if not cov_csv.exists() or not q_csv.exists() or not diag_csv.exists():
    raise FileNotFoundError(
        f"Faltan artefactos en {OUT_DIR}. Ejecuta antes el script 036 para generar coverage/quality/diagnosis."
    )

coverage = pd.read_csv(cov_csv)
quality = pd.read_csv(q_csv)
diagnosis = pd.read_csv(diag_csv)
causes = pd.read_csv(causes_csv) if causes_csv.exists() else pd.DataFrame(columns=["ticker", "cause", "cause_type", "count", "definicion"])

cause_legend = pd.DataFrame([
    {"cause": "parquet_unreadable", "tipo": "HARD_FAIL", "definicion": "El parquet no se puede abrir/leer."},
    {"cause": "zero_byte_file", "tipo": "HARD_FAIL", "definicion": "Archivo de 0 bytes."},
    {"cause": "invalid_partition_path", "tipo": "HARD_FAIL", "definicion": "Ruta no cumple patron ticker/year/month/day/quotes.parquet."},
    {"cause": "zero_rows", "tipo": "HARD_FAIL", "definicion": "Parquet leido pero sin filas."},
    {"cause": "missing_required_columns", "tipo": "HARD_FAIL", "definicion": "Faltan columnas requeridas del esquema estricto."},
    {"cause": "negative_prices_any_row", "tipo": "HARD_FAIL", "definicion": "Hay filas con bid/ask negativos."},
    {"cause": "crossed_ratio_gt_threshold", "tipo": "HARD_FAIL", "definicion": "Porcentaje de bid>ask supera el umbral estricto."},
    {"cause": "crossed_ratio_gt_hard_cap", "tipo": "HARD_FAIL", "definicion": "Porcentaje de bid>ask supera el hard cap operativo."},
    {"cause": "ask_integer_with_crossed_anomaly", "tipo": "HARD_FAIL", "definicion": "ask_price enterizado masivamente junto con cruces bid>ask altos."},
    {"cause": "dtype_mismatch", "tipo": "SOFT_FAIL", "definicion": "Tipo de dato distinto al esperado."},
    {"cause": "crossed_rows_present_but_under_threshold", "tipo": "SOFT_FAIL", "definicion": "Hay bid>ask, pero por debajo del umbral."},
    {"cause": "soft_rule_eval_error", "tipo": "SOFT_FAIL", "definicion": "Error evaluando regla soft; archivo no necesariamente corrupto."},
    {"cause": "low_coverage_no_validation_cause", "tipo": "LOW_COVERAGE", "definicion": "Cobertura insuficiente en ventana esperada; sin causa tecnica registrada en events_current."},
    {"cause": "missing_in_disk", "tipo": "OPERATIONAL", "definicion": "El evento existe pero el archivo ya no esta en disco."},
    {"cause": "missing_in_events", "tipo": "OPERATIONAL", "definicion": "El archivo existe en disco pero no aparece validado en events_current."},
    {"cause": "retry_pending", "tipo": "OPERATIONAL", "definicion": "Archivo pendiente de reintento en retry_queue_current."},
    {"cause": "retry_frozen_exhausted", "tipo": "OPERATIONAL", "definicion": "Archivo congelado por superar maximo de reintentos."},
])

thr_txt = f"{MAX_CROSSED_RATIO_PCT}%" if MAX_CROSSED_RATIO_PCT not in (None, "") else "N/A"
thr_value = None
hard_cap_txt = "N/A"
ask_int_thr_txt = "N/A"
ask_int_cross_thr_txt = "N/A"
if MAX_CROSSED_RATIO_PCT not in (None, ""):
    try:
        thr_value = float(MAX_CROSSED_RATIO_PCT)
    except Exception:
        thr_value = None
if MAX_CROSSED_RATIO_PCT in (None, ""):
    # Fallback automatico: inferir run_config desde EVENTS_CSV
    if (not str(RUN_CONFIG_JSON) or str(RUN_CONFIG_JSON) in (".", "")) and EVENTS_CSV:
        RUN_CONFIG_JSON = EVENTS_CSV.with_name("run_config_quotes_strict.json")
    try:
        if RUN_CONFIG_JSON and RUN_CONFIG_JSON.exists():
            import json
            _cfg = json.loads(RUN_CONFIG_JSON.read_text(encoding="utf-8"))
            _thr = _cfg.get("max_crossed_ratio_pct", None)
            if _thr not in (None, ""):
                thr_txt = f"{_thr}%"
                try:
                    thr_value = float(_thr)
                except Exception:
                    thr_value = None
            _hard = _cfg.get("hard_fail_crossed_pct", None)
            _aski = _cfg.get("hard_fail_ask_integer_pct", None)
            _askc = _cfg.get("hard_fail_ask_int_crossed_pct", None)
            if _hard not in (None, ""):
                hard_cap_txt = f"{_hard}%"
            if _aski not in (None, ""):
                ask_int_thr_txt = f"{_aski}%"
            if _askc not in (None, ""):
                ask_int_cross_thr_txt = f"{_askc}%"
    except Exception:
        pass

print("\nLeyenda de causas:")
try:
    with pd.option_context("display.max_colwidth", None, "display.width", 220):
        display(cause_legend)
except Exception:
    print(cause_legend.to_string(index=False, max_colwidth=None))

if EVENTS_CSV and EVENTS_CSV.exists():
    ev = pd.read_csv(EVENTS_CSV)
    if "severity" in ev.columns:
        sev = ev.groupby("severity", dropna=False).size().reset_index(name="count")
        print("\nResumen por severidad (events):")
        try:
            display(sev)
        except Exception:
            print(sev.to_string(index=False))
    else:
        print("\nResumen por severidad (events): columna severity no encontrada en EVENTS_CSV")
else:
    print("\nResumen por severidad (events): EVENTS_CSV no proporcionado o no existe")

# -------- Tabla 1: Diagnostico (sin columnas temporales para no redundar) --------
diag_cols = [
    c
    for c in [
        "ticker",
        "ticker_gate_status",
        "RETRY_PENDING",
        "HARD_FAIL",
        "SOFT_FAIL",
        "cause",
        "count",
        "definicion",
    ]
    if c in diagnosis.columns
]
diag_view = diagnosis[diag_cols].copy()
max_files_target = None
tickers_from_manifest = None
batch_manifest_files = set()
if BATCH_MANIFEST_CSV and BATCH_MANIFEST_CSV.exists():
    try:
        _m = pd.read_csv(BATCH_MANIFEST_CSV)
        if "file" in _m.columns:
            batch_manifest_files = set(_m["file"].dropna().astype(str).tolist())
            max_files_target = int(_m["file"].dropna().nunique())
            _mx = _m["file"].dropna().astype(str).str.extract(
                r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
            )
            tickers_from_manifest = int(_mx["ticker"].dropna().nunique())
        else:
            max_files_target = int(len(_m))
    except Exception:
        max_files_target = None

files_covered = None
if EVENTS_CSV and EVENTS_CSV.exists():
    try:
        _e = pd.read_csv(EVENTS_CSV)
        files_covered = int(_e["file"].dropna().nunique()) if "file" in _e.columns else int(len(_e))
    except Exception:
        files_covered = None

# Si hay batch_manifest, usarlo como fuente oficial del lote actual (evita mezclar corridas).
if max_files_target is not None:
    files_covered = max_files_target

# fallback cuando no hay batch_manifest en la corrida
if max_files_target is None and files_covered is not None:
    max_files_target = files_covered

tickers_found = tickers_from_manifest
if tickers_found is None:
    tickers_found = int(diagnosis["ticker"].dropna().nunique()) if "ticker" in diagnosis.columns else None
print(
    f"Agente 02 en 032 cubrio MAX_FILES = {max_files_target if max_files_target is not None else 'N/A'}, "
    f"files cubiertos/encontrados = {files_covered if files_covered is not None else 'N/A'}, "
    f"en un total de tickers = {tickers_found if tickers_found is not None else 'N/A'}."
)
if DIAG_HEAD_N not in (None, ""):
    try:
        print(f"Salida truncada a head({int(DIAG_HEAD_N)}).")
    except Exception:
        pass
print("\nTicker diagnosis (por que no pasa):")
print("")
print("Leyenda de ticker diagnosis (atributos):")
diag_legend_lines = [
    "- ticker : Simbolo del activo.",
    "- ticker_gate_status : Estado de cierre por ticker (PASSING, LOW_COVERAGE, NO_CLOSE_RETRY_PENDING, NO_CLOSE_HARD_FAIL_PRESENT).",
    "- RETRY_PENDING : Numero de archivos/dias del ticker pendientes de reintento.",
    "- HARD_FAIL : Numero de archivos/dias con fallo duro en la validacion.",
    "- SOFT_FAIL : Numero de archivos/dias con fallo suave en la validacion.",
    "- cause : Causa principal asociada al ticker en el snapshot actual.",
    "- count : Conteo de ocurrencias de la causa principal para ese ticker.",
    "- definicion : Definicion tecnica de la causa principal.",
]
for line in diag_legend_lines:
    print(line)
try:
    diag_sorted = diag_view.sort_values(["ticker_gate_status", "ticker"], ascending=[True, True])
    if DIAG_HEAD_N not in (None, ""):
        try:
            n = int(DIAG_HEAD_N)
            diag_sorted = diag_sorted.head(max(n, 0))
        except Exception:
            pass
    display(diag_sorted)
except Exception:
    diag_sorted = diag_view.sort_values(["ticker_gate_status", "ticker"], ascending=[True, True])
    if DIAG_HEAD_N not in (None, ""):
        try:
            n = int(DIAG_HEAD_N)
            diag_sorted = diag_sorted.head(max(n, 0))
        except Exception:
            pass
    print(diag_sorted.to_string(index=False))

# -------- Tabla 2: Cobertura temporal --------
print("\nCobertura temporal por ticker:")
print("")
print("Leyenda de cobertura (atributos):")
coverage_legend_lines = [
    "- ticker : Simbolo del activo.",
    "- exp_min : Inicio de ventana esperada del ticker (lifecycle oficial acotado por observado).",
    "- exp_max : Fin de ventana esperada del ticker (lifecycle oficial acotado por observado).",
    "- expected_days : Dias esperados dentro de [exp_min, exp_max].",
    "- present_days : Dias con archivo/evento presente para el ticker en la corrida analizada.",
    "- present_ok_days : Dias presentes cuyo estado cuenta como cobertura valida (segun COVERAGE_OK_STATUSES).",
    "- missing_days_ok : Dias faltantes para cobertura valida: expected_days - present_ok_days. Mide faltantes de dias OK, no archivo faltante en disco.",
    "- coverage_ratio_ok : Ratio de cobertura valida: present_ok_days / expected_days.",
]
for line in coverage_legend_lines:
    print(line)
cov_cols = [
    c
    for c in [
        "ticker",
        "exp_min",
        "exp_max",
        "expected_days",
        "present_days",
        "present_ok_days",
        "missing_days_ok",
        "coverage_ratio_ok",
    ]
    if c in coverage.columns
]
cov_view = coverage[cov_cols].copy()
try:
    cov_sorted = cov_view.sort_values(["coverage_ratio_ok", "ticker"], ascending=[True, True])
    if COV_HEAD_N not in (None, ""):
        try:
            n = int(COV_HEAD_N)
            print(f"Salida truncada a head({n}).")
            cov_sorted = cov_sorted.head(max(n, 0))
        except Exception:
            pass
    display(cov_sorted)
except Exception:
    cov_sorted = cov_view.sort_values(["coverage_ratio_ok", "ticker"], ascending=[True, True])
    if COV_HEAD_N not in (None, ""):
        try:
            n = int(COV_HEAD_N)
            print(f"Salida truncada a head({n}).")
            cov_sorted = cov_sorted.head(max(n, 0))
        except Exception:
            pass
    print(cov_sorted.to_string(index=False))

# -------- Hist: Mayores causas globales --------
if not causes.empty and {"cause", "count"}.issubset(causes.columns):
    cglob = (
        causes.groupby("cause", dropna=False)["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
        .head(TOP_N)
    )
    cglob = cglob.merge(cause_legend[["cause", "definicion"]], on="cause", how="left")

    print(f"\nLeyenda Top {TOP_N} causas globales de error (MAX_FILES = {max_files_target if max_files_target is not None else 'N/A'}):")
    print("")
    cglob_tbl = cglob.copy()
    cglob_tbl["definicion"] = cglob_tbl.apply(
        lambda r: (
            f"{r['definicion']} Umbral usado en corrida: {thr_txt}."
            if str(r["cause"]) in ("crossed_rows_present_but_under_threshold", "crossed_ratio_gt_threshold")
            else f"{r['definicion']} Hard cap usado en corrida: {hard_cap_txt}."
            if str(r["cause"]) in ("crossed_ratio_gt_hard_cap",)
            else f"{r['definicion']} Umbrales usados en corrida: ask_integer_pct>{ask_int_thr_txt} y crossed_pct>{ask_int_cross_thr_txt}."
            if str(r["cause"]) in ("ask_integer_with_crossed_anomaly",)
            else r["definicion"]
        ),
        axis=1,
    )
    _print_table(cglob_tbl[["cause", "count", "definicion"]])
    print("")


    plt.figure(figsize=FIGSIZE)
    plt.barh(cglob["cause"].astype(str), cglob["count"].astype(float))
    plt.title(f"Top {TOP_N} causas globales de error")
    plt.xlabel("count")
    plt.ylabel("cause")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.show()

    # -------- Hist extra: retry_pending granular por causas --------
    if EVENTS_CSV.exists() and RETRY_QUEUE_CSV.exists():
        try:
            evx = pd.read_csv(EVENTS_CSV)
        except Exception:
            evx = pd.read_csv(EVENTS_CSV, engine="python", on_bad_lines="skip")
        try:
            rqx = pd.read_csv(RETRY_QUEUE_CSV)
        except Exception:
            rqx = pd.read_csv(RETRY_QUEUE_CSV, engine="python", on_bad_lines="skip")

        if "file" in evx.columns and "file" in rqx.columns:
            # Acotar al lote actual (batch_manifest) para coherencia con MAX_FILES/tickers reportados.
            if batch_manifest_files:
                evx = evx[evx["file"].astype(str).isin(batch_manifest_files)].copy()
                rqx = rqx[rqx["file"].astype(str).isin(batch_manifest_files)].copy()

            pending_files = set(rqx["file"].dropna().astype(str).tolist())
            evp = evx[evx["file"].astype(str).isin(pending_files)].copy()

            rows = []
            for _, r in evp.iterrows():
                issues = _parse_listlike(r.get("issues"))
                warns = _parse_listlike(r.get("warns"))
                if not issues and not warns:
                    rows.append({"cause": "no_issue_or_warn_recorded", "count": 1})
                for c in issues:
                    rows.append({"cause": c, "count": 1})
                for c in warns:
                    rows.append({"cause": c, "count": 1})

            if rows:
                rc = pd.DataFrame(rows).groupby("cause", dropna=False)["count"].sum().reset_index()
                rc = rc.sort_values("count", ascending=False).head(TOP_N)
                print("\nDESGLOSE GRANULADO POR TIPO DE CAUSA:\n")
                print(f"\nLeyenda de causas (retry_pending granulado) (MAX_FILES = {max_files_target if max_files_target is not None else 'N/A'}):")
                print("")
                rc_tbl = rc.copy()
                rc_tbl = rc_tbl.merge(cause_legend[["cause", "definicion"]], on="cause", how="left")
                rc_tbl["definicion"] = rc_tbl.apply(
                    lambda r: (
                        f"{r['definicion']} Umbral usado en corrida: {thr_txt}."
                        if str(r["cause"]) in ("crossed_rows_present_but_under_threshold", "crossed_ratio_gt_threshold")
                        else f"{r['definicion']} Hard cap usado en corrida: {hard_cap_txt}."
                        if str(r["cause"]) in ("crossed_ratio_gt_hard_cap",)
                        else f"{r['definicion']} Umbrales usados en corrida: ask_integer_pct>{ask_int_thr_txt} y crossed_pct>{ask_int_cross_thr_txt}."
                        if str(r["cause"]) in ("ask_integer_with_crossed_anomaly",)
                        else r["definicion"]
                    ),
                    axis=1,
                )
                _print_table(rc_tbl[["cause", "count", "definicion"]])
                print("")


                plt.figure(figsize=FIGSIZE)
                plt.barh(rc["cause"].astype(str), rc["count"].astype(float))
                plt.title(f"retry_pending (Top {TOP_N})")
                plt.xlabel("count")
                plt.ylabel("cause")
                plt.gca().invert_yaxis()
                plt.tight_layout()
                plt.show()

            else:
                print("\nNo hay causas granulares para retry_pending en esta corrida.")

            # -------- Tablas/hist para causas clave de microestructura --------
            if "ticker" not in evx.columns and "file" in evx.columns:
                ext = evx["file"].astype(str).str.extract(
                    r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
                )
                evx = pd.concat([evx, ext], axis=1)

            target_causes = [
                "crossed_rows_present_but_under_threshold",
                "crossed_ratio_gt_threshold",
            ]

            for tc in target_causes:
                rows_tc = []
                for _, r in evx.iterrows():
                    issues = _parse_listlike(r.get("issues"))
                    warns = _parse_listlike(r.get("warns"))
                    all_causes = set(issues + warns)
                    if tc in all_causes:
                        rows_tc.append(
                            {
                                "ticker": r.get("ticker"),
                                "file": r.get("file"),
                                "crossed_rows": r.get("crossed_rows", 0),
                                "crossed_ratio_pct": r.get("crossed_ratio_pct", 0),
                                "severity": r.get("severity", None),
                            }
                        )

                df_tc = pd.DataFrame(rows_tc)
                tickers_tc = tickers_found if tickers_found is not None else (int(df_tc["ticker"].dropna().nunique()) if "ticker" in df_tc.columns else 0)
                print(
                    f"\n{tc} (MAX_FILES = {max_files_target if max_files_target is not None else 'N/A'}, "
                    f"en un total de tickers = {tickers_tc}):"
                )
                defn = cause_legend.loc[cause_legend["cause"] == tc, "definicion"]
                d = defn.iloc[0] if len(defn) else "Causa sin definicion mapeada."
                if tc in ("crossed_rows_present_but_under_threshold", "crossed_ratio_gt_threshold"):
                    d = f"{d} Umbral usado en corrida: {thr_txt}."
                if tc in ("crossed_ratio_gt_hard_cap",):
                    d = f"{d} Hard cap usado en corrida: {hard_cap_txt}."
                if tc in ("ask_integer_with_crossed_anomaly",):
                    d = f"{d} Umbrales usados en corrida: ask_integer_pct>{ask_int_thr_txt} y crossed_pct>{ask_int_cross_thr_txt}."
                legend_tc_tbl = pd.DataFrame([{"cause": tc, "count": int(len(df_tc)), "definicion": d}])
                _print_table(legend_tc_tbl[["cause", "count", "definicion"]])
                print("")
                print("")
                if df_tc.empty:
                    print("Sin ocurrencias en esta corrida.")
                    continue

                for c in ["crossed_rows", "crossed_ratio_pct"]:
                    if c in df_tc.columns:
                        df_tc[c] = pd.to_numeric(df_tc[c], errors="coerce").fillna(0)

                by_t = (
                    df_tc.groupby("ticker", dropna=False)
                    .agg(
                        files=("file", "count"),
                        crossed_rows_total=("crossed_rows", "sum"),
                        mean_crossed_ratio_pct=("crossed_ratio_pct", "mean"),
                        max_crossed_ratio_pct=("crossed_ratio_pct", "max"),
                    )
                    .reset_index()
                    .sort_values("files", ascending=False)
                )

                if thr_value is None:
                    print("No se pudo inferir umbral de corrida; no se generan bandas por umbral.")
                    continue

                # Cuantos tickers por umbrales (basado en max_crossed_ratio_pct por ticker)
                if tc == "crossed_rows_present_but_under_threshold":
                    # bandas granulares de 0 -> umbral (customizables desde celda)
                    try:
                        edges = sorted(set(float(e) for e in UNDER_THRESHOLD_BINS))
                    except Exception:
                        edges = [0.0, 0.02, 0.08, 0.10, 0.125, 0.25, 0.375]
                    edges = [e for e in edges if e >= 0 and e < thr_value]
                    edges = [0.0] + edges if 0.0 not in edges else edges
                    edges.append(float(thr_value))
                    edges = sorted(set(edges))
                    if len(edges) < 2:
                        edges = [0.0, thr_value]
                    labels = [f"[{edges[i]:.4f}, {edges[i+1]:.4f})%" for i in range(len(edges) - 1)]
                    by_t["umbral_bin"] = pd.cut(
                        by_t["max_crossed_ratio_pct"],
                        bins=edges,
                        labels=labels,
                        include_lowest=True,
                        right=False,
                    )
                    expected_bins = labels
                else:
                    # bandas desde umbral -> infinito
                    edges = [thr_value, thr_value * 2, thr_value * 4, thr_value * 8, np.inf]
                    labels = [
                        f"[{edges[0]:.4f}, {edges[1]:.4f})%",
                        f"[{edges[1]:.4f}, {edges[2]:.4f})%",
                        f"[{edges[2]:.4f}, {edges[3]:.4f})%",
                        f">= {edges[3]:.4f}%",
                    ]
                    by_t["umbral_bin"] = pd.cut(
                        by_t["max_crossed_ratio_pct"],
                        bins=edges,
                        labels=labels,
                        include_lowest=True,
                        right=False,
                    )
                    expected_bins = labels

                band = (
                    by_t.groupby("umbral_bin", dropna=False)["ticker"]
                    .nunique()
                    .reset_index(name="tickers")
                )
                band["umbral_bin"] = band["umbral_bin"].astype(str)

                # Forzar aparicion de todas las bandas, incluso con 0 tickers.
                full = pd.DataFrame({"umbral_bin": expected_bins, "_ord": range(len(expected_bins))})
                band = full.merge(band, on="umbral_bin", how="left")
                band["tickers"] = band["tickers"].fillna(0).astype(int)
                band = band.sort_values("_ord", ascending=True).drop(columns=["_ord"]).reset_index(drop=True)

                try:
                    _print_table(band)
                except Exception:
                    _print_table(band)

                plt.figure(figsize=FIGSIZE)
                plt.barh(band["umbral_bin"].astype(str), band["tickers"].astype(float))
                plt.title(f"{tc} - tickers por umbral")
                plt.xlabel("tickers")
                plt.ylabel("umbral_bin")
                plt.gca().invert_yaxis()
                plt.tight_layout()
                plt.show()


            # -------- Bloque especifico: dtype_mismatch por tipo --------
            dm_rows = []
            for _, r in evx.iterrows():
                warns = _parse_listlike(r.get("warns"))
                if "dtype_mismatch" not in set(warns):
                    continue
                mismatches = _parse_listlike(r.get("dtype_mismatches"))
                if not mismatches:
                    dm_rows.append({"dtype_mismatch_type": "dtype_mismatch_without_detail"})
                else:
                    for m in mismatches:
                        dm_rows.append({"dtype_mismatch_type": str(m)})

            print(
                f"\ndtype_mismatch (MAX_FILES = {max_files_target if max_files_target is not None else 'N/A'}, "
                f"en un total de tickers = {tickers_found if tickers_found is not None else 'N/A'}):"
            )
            dmd = cause_legend.loc[cause_legend["cause"] == "dtype_mismatch", "definicion"]
            dmd_txt = dmd.iloc[0] if len(dmd) else "Causa sin definicion mapeada."
            dm_tbl_legend = pd.DataFrame(
                [{"cause": "dtype_mismatch", "count": int(len(dm_rows)), "definicion": dmd_txt}]
            )
            _print_table(dm_tbl_legend[["cause", "count", "definicion"]])
            print("")

            if dm_rows:
                dm = (
                    pd.DataFrame(dm_rows)
                    .groupby("dtype_mismatch_type", dropna=False)
                    .size()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False)
                    .head(TOP_N)
                )
                _print_table(dm)
                print("")


                plt.figure(figsize=FIGSIZE)
                plt.barh(dm["dtype_mismatch_type"].astype(str), dm["count"].astype(float))
                plt.title(f"dtype_mismatch por tipo (Top {TOP_N})")
                plt.xlabel("count")
                plt.ylabel("dtype_mismatch_type")
                plt.gca().invert_yaxis()
                plt.tight_layout()
                plt.show()

            else:
                print("Sin ocurrencias de dtype_mismatch en esta corrida/lote.")
        else:
            print("\nNo se puede granular retry_pending: falta columna file en events/retry queue.")
    else:
        print("\nNo se puede dibujar retry_pending granular: faltan EVENTS_CSV o RETRY_QUEUE_CSV.")
else:
    print("\nNo hay causes_by_ticker.csv o esta vacio; no se puede dibujar histograma de causas.")
