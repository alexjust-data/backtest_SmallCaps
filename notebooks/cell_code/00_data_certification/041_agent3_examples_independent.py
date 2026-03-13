from pathlib import Path
import ast
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

# Entradas
RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", RUN_DIR / "quotes_agent_strict_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", RUN_DIR / "retry_queue_quotes_strict_current.csv"))
CAUSES_CSV = Path(globals().get("CAUSES_CSV", RUN_DIR / "agent03_outputs" / "causes_by_ticker.csv"))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", RUN_DIR / "run_config_quotes_strict.json"))

TOP_N = int(globals().get("TOP_N", 10))
EXAMPLE_FILES_PER_TABLE = int(globals().get("EXAMPLE_FILES_PER_TABLE", 4))
UNDER_THRESHOLD_BINS = globals().get("UNDER_THRESHOLD_BINS", [0.0, 0.02, 0.08, 0.10, 0.125, 0.25, 0.375])

# Opcional (leer parquet y graficar bid/ask de ejemplos)
PLOT_FILE_EXAMPLES = bool(globals().get("PLOT_FILE_EXAMPLES", False))
MAX_FILE_PLOTS = int(globals().get("MAX_FILE_PLOTS", 2))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 4)))
DTYPE_SAMPLE_ROWS = int(globals().get("DTYPE_SAMPLE_ROWS", 5))


def _print_table(df: pd.DataFrame):
    display(df)


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


def _ensure_ticker(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ticker" not in out.columns and "file" in out.columns:
        ext = out["file"].astype(str).str.extract(
            r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
        )
        out = pd.concat([out, ext], axis=1)
    return out


def _rows_for_cause(events_df: pd.DataFrame, cause: str) -> pd.DataFrame:
    rows = []
    e = _ensure_ticker(events_df)
    for _, r in e.iterrows():
        issues = _parse_listlike(r.get("issues"))
        warns = _parse_listlike(r.get("warns"))
        if cause in set(issues + warns):
            rows.append(
                {
                    "cause": cause,
                    "ticker": r.get("ticker"),
                    "file": r.get("file"),
                    "crossed_rows": r.get("crossed_rows", 0),
                    "crossed_ratio_pct": r.get("crossed_ratio_pct", 0),
                    "severity": r.get("severity", None),
                    "issues": r.get("issues", None),
                    "warns": r.get("warns", None),
                }
            )
    return pd.DataFrame(rows)


def _show_example_files(df_rows: pd.DataFrame, title: str):
    print(f"\n{title}")
    if df_rows.empty:
        print("Sin ejemplos.")
        return pd.DataFrame()
    d = _ensure_ticker(df_rows)
    for c in ["crossed_rows", "crossed_ratio_pct"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
    d = d.sort_values(["crossed_ratio_pct", "crossed_rows"], ascending=[False, False])
    worst = d.drop_duplicates(subset=["ticker"], keep="first")
    cols = [c for c in ["ticker", "cause", "file", "crossed_ratio_pct", "crossed_rows", "severity", "issues", "warns"] if c in worst.columns]
    out = worst[cols].head(EXAMPLE_FILES_PER_TABLE)
    _print_table(out)
    return out


def _plot_file_examples(df_files: pd.DataFrame):
    if not PLOT_FILE_EXAMPLES or pq is None or df_files.empty:
        return
    n = 0
    for _, r in df_files.iterrows():
        if n >= MAX_FILE_PLOTS:
            break
        fp = Path(str(r.get("file", "")))
        if not fp.exists():
            continue
        try:
            t = pq.read_table(fp, columns=[c for c in ["bid_price", "ask_price", "timestamp"] if c in pq.ParquetFile(fp).schema.names])
            pdf = t.to_pandas()
            if not {"bid_price", "ask_price"}.issubset(pdf.columns):
                continue
            pdf = pdf.head(3000).copy()
            plt.figure(figsize=FIGSIZE)
            plt.plot(pdf.index, pdf["bid_price"], label="bid_price", linewidth=0.8)
            plt.plot(pdf.index, pdf["ask_price"], label="ask_price", linewidth=0.8)
            plt.title(f"Example file: {fp.name} | ticker={r.get('ticker')}")
            plt.xlabel("row")
            plt.ylabel("price")
            plt.legend()
            plt.tight_layout()
            plt.show()
            n += 1
        except Exception:
            continue


def _dtype_to_text(pa_type):
    s = str(pa_type).lower()
    if "timestamp" in s:
        return "int64"
    if s in ("double", "float64"):
        return "double"
    if s in ("float", "float32"):
        return "float"
    if s in ("int64", "int32", "int16", "int8"):
        return s
    if "dictionary" in s:
        return "dictionary"
    return s


def _show_dtype_mismatch_evidence(df_files: pd.DataFrame):
    if pq is None or df_files.empty:
        return

    expected = {
        "timestamp": "int64",
        "bid_price": "double",
        "ask_price": "double",
        "bid_size": "int64",
        "ask_size": "int64",
    }

    printed_full_schema_once = False
    for _, r in df_files.iterrows():
        fp = Path(str(r.get("file", "")))
        if not fp.exists():
            continue
        print(f"\nSchema check file: {fp}")
        try:
            pf = pq.ParquetFile(fp)
            top_fields = list(pf.schema_arrow)
            schema_map = {f.name: _dtype_to_text(f.type) for f in top_fields}

            rows = []
            for col, exp in expected.items():
                act = schema_map.get(col, "MISSING")
                ok = (act == exp)
                rows.append(
                    {
                        "column": col,
                        "expected_dtype": exp,
                        "actual_dtype": act,
                        "status": "OK" if ok else "MISMATCH",
                    }
                )
            chk = pd.DataFrame(rows)
            if not printed_full_schema_once:
                _print_table(chk)
                printed_full_schema_once = True
            else:
                only_mm = chk[chk["status"] == "MISMATCH"].reset_index(drop=True)
                if only_mm.empty:
                    print("Sin mismatch de schema en este archivo.")
                else:
                    print("Solo columnas con mismatch:")
                    _print_table(only_mm)

            cols = [c for c in expected.keys() if c in schema_map]
            if cols:
                sample = pq.read_table(fp, columns=cols).to_pandas().head(DTYPE_SAMPLE_ROWS)
                print(f"Sample rows head({DTYPE_SAMPLE_ROWS}):")
                _print_table(sample)
        except Exception as ex:
            print(f"No se pudo leer schema/sample: {ex}")


if not EVENTS_CSV.exists():
    raise FileNotFoundError(f"No existe EVENTS_CSV: {EVENTS_CSV}")

try:
    ev = pd.read_csv(EVENTS_CSV)
except Exception:
    ev = pd.read_csv(EVENTS_CSV, engine="python", on_bad_lines="skip")
ev = _ensure_ticker(ev)
for c in ["crossed_rows", "crossed_ratio_pct"]:
    if c in ev.columns:
        ev[c] = pd.to_numeric(ev[c], errors="coerce").fillna(0)

thr = None
if RUN_CONFIG_JSON.exists():
    try:
        cfg = json.loads(RUN_CONFIG_JSON.read_text(encoding="utf-8"))
        thr = cfg.get("max_crossed_ratio_pct", None)
        thr = float(thr) if thr not in (None, "") else None
    except Exception:
        thr = None

# 1) Top causas globales
print("\n=== TOP CAUSAS GLOBALES ===")
print("Que es: ranking global de causas detectadas en la corrida.")
print("Que ver: las causas con mayor volumen para priorizar limpieza/reintentos.")
if CAUSES_CSV.exists():
    c = pd.read_csv(CAUSES_CSV)
    cglob = c.groupby("cause", dropna=False)["count"].sum().reset_index().sort_values("count", ascending=False).head(TOP_N)
else:
    rows = []
    for _, r in ev.iterrows():
        for x in _parse_listlike(r.get("issues")):
            rows.append({"cause": x, "count": 1})
        for x in _parse_listlike(r.get("warns")):
            rows.append({"cause": x, "count": 1})
    cglob = pd.DataFrame(rows).groupby("cause", dropna=False)["count"].sum().reset_index().sort_values("count", ascending=False).head(TOP_N) if rows else pd.DataFrame(columns=["cause","count"])

_print_table(cglob)

# 2) retry_pending granular
print("\n=== RETRY_PENDING GRANULAR ===")
print("Que es: desglose interno de archivos pendientes de reintento.")
print("Que ver: que causas concretas estan alimentando la cola retry_pending.")
if RETRY_QUEUE_CSV.exists() and "file" in ev.columns:
    rq = pd.read_csv(RETRY_QUEUE_CSV)
    pending_files = set(rq["file"].dropna().astype(str).tolist()) if "file" in rq.columns else set()
    evp = ev[ev["file"].astype(str).isin(pending_files)].copy()
    rows = []
    for _, r in evp.iterrows():
        for x in _parse_listlike(r.get("issues")):
            rows.append({"cause": x, "count": 1})
        for x in _parse_listlike(r.get("warns")):
            rows.append({"cause": x, "count": 1})
    rc = pd.DataFrame(rows).groupby("cause", dropna=False)["count"].sum().reset_index().sort_values("count", ascending=False).head(TOP_N) if rows else pd.DataFrame(columns=["cause","count"])
    _print_table(rc)
else:
    print("retry_queue no disponible.")

# 3) crossed_rows_present_but_under_threshold
print("\n=== CROSSED_UNDER_THRESHOLD ===")
print("Que es: casos con bid_price > ask_price por debajo del umbral estricto.")
print("Que ver: distribucion por bins y tickers afectados en zona 'warning'.")
under = _rows_for_cause(ev, "crossed_rows_present_but_under_threshold")
if not under.empty and thr is not None:
    by_t = under.groupby("ticker", dropna=False).agg(max_crossed_ratio_pct=("crossed_ratio_pct", "max")).reset_index()
    edges = sorted(set([e for e in [0.0] + [float(x) for x in UNDER_THRESHOLD_BINS] + [thr] if 0 <= float(e) <= thr]))
    if len(edges) < 2:
        edges = [0.0, thr]
    labels = [f"[{edges[i]:.4f}, {edges[i+1]:.4f})%" for i in range(len(edges)-1)]
    by_t["umbral_bin"] = pd.cut(by_t["max_crossed_ratio_pct"], bins=edges, labels=labels, include_lowest=True, right=False)
    b = by_t.groupby("umbral_bin", dropna=False)["ticker"].nunique().reset_index(name="tickers")
    _print_table(b)
shown = _show_example_files(under.assign(cause="crossed_rows_present_but_under_threshold"), "Parquet examples (worst per ticker) - crossed under")
_plot_file_examples(shown)

# 4) crossed_ratio_gt_threshold
print("\n=== CROSSED_GT_THRESHOLD ===")
print("Que es: casos con bid_price > ask_price por encima del umbral estricto.")
print("Que ver: bins altos y tickers criticos (HARD_FAIL potencial).")
over = _rows_for_cause(ev, "crossed_ratio_gt_threshold")
if not over.empty and thr is not None:
    by_t = over.groupby("ticker", dropna=False).agg(max_crossed_ratio_pct=("crossed_ratio_pct", "max")).reset_index()
    edges = [thr, thr*2, thr*4, thr*8, np.inf]
    labels = [f"[{edges[0]:.4f}, {edges[1]:.4f})%", f"[{edges[1]:.4f}, {edges[2]:.4f})%", f"[{edges[2]:.4f}, {edges[3]:.4f})%", f">= {edges[3]:.4f}%"]
    by_t["umbral_bin"] = pd.cut(by_t["max_crossed_ratio_pct"], bins=edges, labels=labels, include_lowest=True, right=False)
    b = by_t.groupby("umbral_bin", dropna=False)["ticker"].nunique().reset_index(name="tickers")
    _print_table(b)
shown = _show_example_files(over.assign(cause="crossed_ratio_gt_threshold"), "Parquet examples (worst per ticker) - crossed over")
_plot_file_examples(shown)

# 5) dtype_mismatch por tipo
print("\n  === DTYPE_MISMATCH POR TIPO ===")
print("  Que es: diferencias de tipo entre schema esperado y schema real del parquet.")
print("  Schema: estructura tecnica del parquet (nombres de columnas + tipo de dato de cada columna).")
print("  Que ver: columnas que desvían (ej. ask_price int64 vs double) y evidencia por archivo.")
dm_rows = []
for _, r in ev.iterrows():
    warns = _parse_listlike(r.get("warns"))
    if "dtype_mismatch" not in set(warns):
        continue
    dms = _parse_listlike(r.get("dtype_mismatches"))
    if not dms:
        dm_rows.append({"dtype_mismatch_type": "dtype_mismatch_without_detail", "count": 1})
    else:
        for x in dms:
            dm_rows.append({"dtype_mismatch_type": x, "count": 1})
if dm_rows:
    dm = (
        pd.DataFrame(dm_rows)
        .groupby("dtype_mismatch_type", dropna=False)["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )
    _print_table(dm)
    ev_dm = ev[[c for c in ["ticker", "file", "crossed_ratio_pct", "crossed_rows", "severity", "issues", "warns"] if c in ev.columns]].copy()
    shown = _show_example_files(ev_dm.assign(cause="dtype_mismatch"), "Parquet examples (worst per ticker) - dtype_mismatch")
    _show_dtype_mismatch_evidence(shown)
else:
    print("Sin dtype_mismatch en esta corrida.")
