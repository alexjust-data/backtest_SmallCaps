from pathlib import Path
import re
import json
import ast
import pandas as pd
import numpy as np

# =========================
# Config (sobrescribible desde celda)
# =========================
QUOTES_ROOT = Path(globals().get("QUOTES_ROOT", r"D:\quotes"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_strict_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_strict_current.csv"))
RETRY_FROZEN_CSV = Path(globals().get("RETRY_FROZEN_CSV", r""))
BATCH_MANIFEST_CSV = Path(globals().get("BATCH_MANIFEST_CSV", r""))
OFFICIAL_LIFECYCLE_CSV = Path(globals().get("OFFICIAL_LIFECYCLE_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\official_lifecycle_compiled.csv"))
OUT_DIR = Path(globals().get("OUT_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\agent03_outputs"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
USE_BATCH_MANIFEST = bool(globals().get("USE_BATCH_MANIFEST", False))
ACCEPT_ALL_RAW = bool(globals().get("ACCEPT_ALL_RAW", True))

DATE_FROM = globals().get("DATE_FROM", None)
DATE_TO = globals().get("DATE_TO", None)
MAX_TICKERS = globals().get("MAX_TICKERS", None)
MAX_TICKERS = int(MAX_TICKERS) if MAX_TICKERS not in (None, "") else None

COVERAGE_OK_STATUSES = set(globals().get("COVERAGE_OK_STATUSES", ["PASS", "SOFT_FAIL"]))
MIN_COVERAGE_TO_PASS = float(globals().get("MIN_COVERAGE_TO_PASS", 0.95))
DIAG_HEAD_N = globals().get("DIAG_HEAD_N", 30)  # None = todo
DIAG_TRANSPOSE = bool(globals().get("DIAG_TRANSPOSE", False))

CAUSE_LEGEND = {
    "parquet_unreadable": "El parquet no se puede abrir/leer.",
    "zero_byte_file": "Archivo de 0 bytes.",
    "invalid_partition_path": "Ruta no cumple patron ticker/year/month/day/quotes.parquet.",
    "zero_rows": "Parquet leido pero sin filas.",
    "missing_required_columns": "Faltan columnas requeridas del esquema estricto.",
    "negative_prices_any_row": "Hay filas con bid/ask negativos.",
    "crossed_ratio_gt_threshold": "Porcentaje de bid>ask supera el umbral estricto.",
    "dtype_mismatch": "Tipo de dato distinto al esperado.",
    "crossed_rows_present_but_under_threshold": "Hay bid>ask, pero por debajo del umbral.",
    "soft_rule_eval_error": "Error evaluando regla soft; archivo no necesariamente corrupto.",
    "missing_in_disk": "El evento existe pero el archivo ya no esta en disco.",
    "missing_in_events": "El archivo existe en disco pero no aparece validado en events_current.",
    "retry_frozen_exhausted": "Archivo en cola de revision agotada (legacy retry), congelado tras superar el maximo de intentos.",
    "retry_pending": "Archivo en cola de revision posterior (legacy retry_queue_current); no bloquea la aceptacion inicial del bruto.",
}


def _safe_read_csv(path: Path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")


def _parse_file_to_day(series: pd.Series):
    pat = re.compile(r"[/\\](?P<ticker>[^/\\]+)[/\\]year=(?P<year>\d{4})[/\\]month=(?P<month>\d{2})[/\\]day=(?P<day>\d{2})[/\\]quotes\.parquet$")
    ext = series.astype(str).str.extract(pat)
    ext["date"] = pd.to_datetime(ext["year"] + "-" + ext["month"] + "-" + ext["day"], errors="coerce")
    return ext


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


def _build_expected_window(events_df: pd.DataFrame, lifecycle_csv: Path):
    obs = events_df.groupby("ticker", dropna=False).agg(obs_min=("date", "min"), obs_max=("date", "max")).reset_index()
    if "ticker" in obs.columns:
        obs["ticker"] = obs["ticker"].astype("string").str.strip()
        obs.loc[obs["ticker"] == "", "ticker"] = pd.NA

    if not lifecycle_csv.exists():
        return obs.rename(columns={"obs_min": "exp_min", "obs_max": "exp_max"})

    life = _safe_read_csv(lifecycle_csv)
    if life.empty:
        return obs.rename(columns={"obs_min": "exp_min", "obs_max": "exp_max"})

    cols = {c.lower(): c for c in life.columns}
    tcol = cols.get("ticker")
    lcol = cols.get("list_date")
    dcol = cols.get("delist_date")

    if not tcol or not lcol:
        return obs.rename(columns={"obs_min": "exp_min", "obs_max": "exp_max"})

    keep = [tcol, lcol] + ([dcol] if dcol else [])
    life2 = life[keep].copy()
    life2.columns = ["ticker", "list_date"] + (["delist_date"] if dcol else [])
    life2["ticker"] = life2["ticker"].astype("string").str.strip()
    life2.loc[life2["ticker"] == "", "ticker"] = pd.NA
    life2["list_date"] = pd.to_datetime(life2["list_date"], errors="coerce")
    if "delist_date" in life2.columns:
        life2["delist_date"] = pd.to_datetime(life2["delist_date"], errors="coerce")
    else:
        life2["delist_date"] = pd.NaT
    life2["delist_date"] = life2["delist_date"].fillna(pd.Timestamp.today().normalize())

    expected = obs.merge(life2[["ticker", "list_date", "delist_date"]], on="ticker", how="left")
    expected["exp_min"] = expected["list_date"].fillna(expected["obs_min"])
    expected["exp_max"] = expected["delist_date"].fillna(expected["obs_max"])
    expected["exp_min"] = expected[["exp_min", "obs_min"]].max(axis=1)
    expected["exp_max"] = expected[["exp_max", "obs_max"]].min(axis=1)

    return expected[["ticker", "exp_min", "exp_max", "obs_min", "obs_max"]]


# ===== Load =====
events = _safe_read_csv(EVENTS_CSV)
if events.empty:
    raise RuntimeError(f"No events found: {EVENTS_CSV}")

if "processed_at_utc" in events.columns:
    events["processed_at_utc"] = pd.to_datetime(events["processed_at_utc"], errors="coerce")
    events = events.sort_values("processed_at_utc")

if "file" in events.columns:
    events = events.drop_duplicates(subset=["file"], keep="last")

# Acotar todo el analisis al lote actual si existe batch manifest.
manifest_files = set()
if USE_BATCH_MANIFEST and str(BATCH_MANIFEST_CSV) not in ("", ".") and BATCH_MANIFEST_CSV.exists():
    mdf = _safe_read_csv(BATCH_MANIFEST_CSV)
    if not mdf.empty and "file" in mdf.columns:
        manifest_files = set(mdf["file"].dropna().astype(str).tolist())
if manifest_files and "file" in events.columns:
    events = events[events["file"].astype(str).isin(manifest_files)].copy()

parts = _parse_file_to_day(events["file"])
events = events.reset_index(drop=True).copy()
events["ticker"] = parts["ticker"].astype("string")
events["year"] = parts["year"]
events["month"] = parts["month"]
events["day"] = parts["day"]
events["date"] = parts["date"]
events["ticker"] = events["ticker"].astype("string").str.strip()
events.loc[events["ticker"] == "", "ticker"] = pd.NA
events = events[events["ticker"].notna() & events["date"].notna()].copy()

if DATE_FROM is not None:
    events = events[events["date"] >= pd.to_datetime(DATE_FROM)]
if DATE_TO is not None:
    events = events[events["date"] <= pd.to_datetime(DATE_TO)]
if MAX_TICKERS is not None:
    keep = events["ticker"].dropna().unique()[:MAX_TICKERS]
    events = events[events["ticker"].isin(keep)]

retry_df = _safe_read_csv(RETRY_QUEUE_CSV)
if manifest_files and (not retry_df.empty) and "file" in retry_df.columns:
    retry_df = retry_df[retry_df["file"].astype(str).isin(manifest_files)].copy()
retry_files = set(retry_df["file"].dropna().tolist()) if (not retry_df.empty and "file" in retry_df.columns) else set()
retry_frozen_df = _safe_read_csv(RETRY_FROZEN_CSV) if str(RETRY_FROZEN_CSV) not in ("", ".") else pd.DataFrame()
retry_frozen_files = set(retry_frozen_df["file"].dropna().tolist()) if (not retry_frozen_df.empty and "file" in retry_frozen_df.columns) else set()

events["retry_pending"] = events["file"].isin(retry_files)
events["review_queue_pending"] = events["retry_pending"]
events["day_status"] = np.where(events["retry_pending"], "RETRY_PENDING", events.get("severity", "UNKNOWN"))

# ===== Coverage =====
expected = _build_expected_window(events, OFFICIAL_LIFECYCLE_CSV)
present = events.groupby("ticker", dropna=False)["date"].nunique().rename("present_days").reset_index()
present_ok = (
    events[events["day_status"].isin(COVERAGE_OK_STATUSES)]
    .groupby("ticker", dropna=False)["date"].nunique()
    .rename("present_ok_days")
    .reset_index()
)
present["ticker"] = present["ticker"].astype("string")
present_ok["ticker"] = present_ok["ticker"].astype("string")

cov = expected.merge(present, on="ticker", how="left").merge(present_ok, on="ticker", how="left")
cov["present_days"] = cov["present_days"].fillna(0).astype(int)
cov["present_ok_days"] = cov["present_ok_days"].fillna(0).astype(int)
cov["expected_days"] = (cov["exp_max"] - cov["exp_min"]).dt.days.add(1).clip(lower=0).fillna(0).astype(int)
cov["missing_days_ok"] = (cov["expected_days"] - cov["present_ok_days"]).clip(lower=0)
cov["coverage_ratio_ok"] = np.where(cov["expected_days"] > 0, cov["present_ok_days"] / cov["expected_days"], np.nan)
coverage_by_ticker = cov[["ticker", "exp_min", "exp_max", "expected_days", "present_days", "present_ok_days", "missing_days_ok", "coverage_ratio_ok"]].copy()
coverage_by_ticker["ticker"] = coverage_by_ticker["ticker"].astype("string")

quality_summary_by_ticker = (
    events.groupby("ticker", dropna=False)["day_status"]
    .value_counts(dropna=False)
    .unstack(fill_value=0)
    .reset_index()
)
quality_summary_by_ticker["ticker"] = quality_summary_by_ticker["ticker"].astype("string")
quality_summary_by_ticker = quality_summary_by_ticker.merge(
    coverage_by_ticker[["ticker", "coverage_ratio_ok", "missing_days_ok"]],
    on="ticker",
    how="left",
)

# ===== Causes by ticker =====
records = []
for _, r in events.iterrows():
    ticker = r.get("ticker")
    sev = r.get("severity")
    for c in _parse_listlike(r.get("issues")):
        records.append({"ticker": ticker, "cause": c, "cause_type": "issue", "severity": sev})
    for c in _parse_listlike(r.get("warns")):
        records.append({"ticker": ticker, "cause": c, "cause_type": "warn", "severity": sev})

causes_df = pd.DataFrame(records)

# ===== Operational causes (integridad pipeline) =====
op_records = []
events_files = set(events["file"].dropna().astype(str).tolist()) if "file" in events.columns else set()
disk_files = set()
if manifest_files:
    disk_files = manifest_files
else:
    # Fallback: solo si no hay manifest, evita bloquear por defecto
    disk_files = set()

# 1) missing_in_disk (solo si tenemos manifest del lote objetivo)
if manifest_files:
    for f in sorted(events_files - disk_files):
        m = re.search(r"[/\\]([^/\\]+)[/\\]year=\d{4}[/\\]month=\d{2}[/\\]day=\d{2}[/\\]quotes\.parquet$", f)
        t = m.group(1) if m else np.nan
        op_records.append({"ticker": t, "cause": "missing_in_disk", "cause_type": "operational", "severity": "N/A"})

# 2) missing_in_events (manifest del lote - events_current)
if manifest_files:
    for f in sorted(disk_files - events_files):
        m = re.search(r"[/\\]([^/\\]+)[/\\]year=\d{4}[/\\]month=\d{2}[/\\]day=\d{2}[/\\]quotes\.parquet$", f)
        t = m.group(1) if m else np.nan
        op_records.append({"ticker": t, "cause": "missing_in_events", "cause_type": "operational", "severity": "N/A"})

# 3) retry pending files
for f in sorted(retry_files):
    m = re.search(r"[/\\]([^/\\]+)[/\\]year=\d{4}[/\\]month=\d{2}[/\\]day=\d{2}[/\\]quotes\.parquet$", f)
    t = m.group(1) if m else np.nan
    op_records.append({"ticker": t, "cause": "retry_pending", "cause_type": "operational", "severity": "N/A"})

# 4) retry frozen files (if provided)
for f in sorted(retry_frozen_files):
    m = re.search(r"[/\\]([^/\\]+)[/\\]year=\d{4}[/\\]month=\d{2}[/\\]day=\d{2}[/\\]quotes\.parquet$", f)
    t = m.group(1) if m else np.nan
    op_records.append({"ticker": t, "cause": "retry_frozen_exhausted", "cause_type": "operational", "severity": "N/A"})

if op_records:
    op_df = pd.DataFrame(op_records)
    causes_df = pd.concat([causes_df, op_df], ignore_index=True) if not causes_df.empty else op_df
if not causes_df.empty and "ticker" in causes_df.columns:
    causes_df["ticker"] = causes_df["ticker"].astype("string").str.strip()
    causes_df.loc[causes_df["ticker"] == "", "ticker"] = pd.NA

if causes_df.empty:
    causes_by_ticker = pd.DataFrame(columns=["ticker", "cause", "cause_type", "count", "definicion"])
else:
    causes_by_ticker = (
        causes_df.groupby(["ticker", "cause", "cause_type"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["ticker", "count"], ascending=[True, False])
    )
    causes_by_ticker["definicion"] = causes_by_ticker["cause"].map(CAUSE_LEGEND)

# Why ticker fails gate
q = quality_summary_by_ticker.copy()
for c in ["RETRY_PENDING", "HARD_FAIL", "SOFT_FAIL", "PASS"]:
    if c not in q.columns:
        q[c] = 0

q["ticker_gate_status"] = np.where(
    q["RETRY_PENDING"] > 0,
    "NO_CLOSE_RETRY_PENDING",
    np.where(
        q["HARD_FAIL"] > 0,
        "NO_CLOSE_HARD_FAIL_PRESENT",
        np.where(q["coverage_ratio_ok"].fillna(0) < MIN_COVERAGE_TO_PASS, "LOW_COVERAGE", "PASSING"),
    ),
)
q["ticker_review_status"] = np.where(
    q["RETRY_PENDING"] > 0,
    "REVIEW_PENDING_RETRY",
    np.where(
        q["HARD_FAIL"] > 0,
        "REVIEW_PENDING_HARD_FAIL",
        np.where(q["coverage_ratio_ok"].fillna(0) < MIN_COVERAGE_TO_PASS, "REVIEW_PENDING_LOW_COVERAGE", "REVIEW_COMPLETE_OK"),
    ),
)
q["REVIEW_QUEUE_PENDING"] = q["RETRY_PENDING"]
q["backtest_ml_ready"] = q["ticker_review_status"].eq("REVIEW_COMPLETE_OK")

root_cause_top = (
    causes_by_ticker.sort_values(["ticker", "count"], ascending=[True, False])
    .drop_duplicates(["ticker"], keep="first")[["ticker", "cause", "definicion", "count"]]
    if not causes_by_ticker.empty
    else pd.DataFrame(columns=["ticker", "cause", "definicion", "count"])
)
if not root_cause_top.empty and "ticker" in root_cause_top.columns:
    root_cause_top["ticker"] = root_cause_top["ticker"].astype("string")

ticker_diagnosis = (
    q.merge(
        coverage_by_ticker[
            [
                "ticker",
                "exp_min",
                "exp_max",
                "expected_days",
                "present_days",
                "present_ok_days",
            ]
        ],
        on="ticker",
        how="left",
    )
    .merge(root_cause_top, on="ticker", how="left")
)

# Completar causa cuando el ticker falla por cobertura pero no tiene causa tecnica en events.
low_cov_mask = (
    ticker_diagnosis["ticker_gate_status"].eq("LOW_COVERAGE")
    & ticker_diagnosis["cause"].isna()
)
ticker_diagnosis.loc[low_cov_mask, "cause"] = "low_coverage_no_validation_cause"
ticker_diagnosis.loc[low_cov_mask, "definicion"] = (
    "Cobertura insuficiente en ventana esperada; sin causa tecnica registrada en events_current."
)
ticker_diagnosis.loc[low_cov_mask, "count"] = 0

# ===== Outputs =====
coverage_by_ticker.to_csv(OUT_DIR / "coverage_by_ticker.csv", index=False)
quality_summary_by_ticker.to_csv(OUT_DIR / "quality_summary_by_ticker.csv", index=False)
causes_by_ticker.to_csv(OUT_DIR / "causes_by_ticker.csv", index=False)
ticker_diagnosis.to_csv(OUT_DIR / "ticker_diagnosis.csv", index=False)

run_summary = {
    "events_rows_dedup": int(len(events)),
    "tickers": int(events["ticker"].nunique(dropna=True)),
    "retry_pending_files": int(events["retry_pending"].sum()),
    "review_queue_pending_files": int(events["review_queue_pending"].sum()),
    "hard_fail_files": int((events["day_status"] == "HARD_FAIL").sum()),
    "review_queue_semantics": "accept_all_raw_then_review_and_redlist",
    "mean_coverage_ok": float(coverage_by_ticker["coverage_ratio_ok"].dropna().mean()) if len(coverage_by_ticker) else None,
    "gate_status": "NO_CLOSE_RETRY_PENDING" if int(events["retry_pending"].sum()) > 0 else ("NO_CLOSE_HARD_FAIL_PRESENT" if int((events["day_status"] == "HARD_FAIL").sum()) > 0 else "CLOSED_OK"),
    "acceptance_policy": "ACCEPT_ALL_RAW_REVIEW_LATER" if ACCEPT_ALL_RAW else "STRICT_GATE",
    "raw_dataset_status": ("RAW_ACCEPTED_REVIEW_PENDING" if ACCEPT_ALL_RAW and (int(events["retry_pending"].sum()) > 0 or int((events["day_status"] == "HARD_FAIL").sum()) > 0 or bool((coverage_by_ticker["coverage_ratio_ok"].fillna(0) < MIN_COVERAGE_TO_PASS).any())) else ("RAW_ACCEPTED_REVIEW_COMPLETE" if ACCEPT_ALL_RAW else ("BLOCKED_RETRY_PENDING" if int(events["retry_pending"].sum()) > 0 else ("BLOCKED_HARD_FAIL" if int((events["day_status"] == "HARD_FAIL").sum()) > 0 else "STRICT_CLOSED_OK")))),
    "min_coverage_to_pass": MIN_COVERAGE_TO_PASS,
    "out_dir": str(OUT_DIR),
}
(OUT_DIR / "run_summary.json").write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("Run summary:")
print(json.dumps(run_summary, indent=2, ensure_ascii=False))

print("\nTicker diagnosis (por que no pasa):")
show_cols = [
    c
    for c in [
        "ticker",
        "ticker_gate_status",
        "ticker_review_status",
        "backtest_ml_ready",
        "exp_min",
        "exp_max",
        "expected_days",
        "present_days",
        "present_ok_days",
        "missing_days_ok",
        "coverage_ratio_ok",
        "RETRY_PENDING",
        "HARD_FAIL",
        "SOFT_FAIL",
        "cause",
        "count",
        "definicion",
    ]
    if c in ticker_diagnosis.columns
]
diag_view = ticker_diagnosis[show_cols].sort_values(["ticker_gate_status", "coverage_ratio_ok"], ascending=[True, True])
if DIAG_HEAD_N not in (None, ""):
    try:
        n = int(DIAG_HEAD_N)
        diag_view = diag_view.head(max(n, 0))
    except Exception:
        pass
if DIAG_TRANSPOSE:
    diag_view = diag_view.T
try:
    display(diag_view)
except Exception:
    print(diag_view.to_string(index=False))

print("\nTop causas globales:")
if causes_by_ticker.empty:
    print("No causes found")
else:
    cglob = causes_by_ticker.groupby(["cause", "cause_type"], dropna=False)["count"].sum().reset_index().sort_values("count", ascending=False)
    cglob["definicion"] = cglob["cause"].map(CAUSE_LEGEND)
    try:
        display(cglob.head(20))
    except Exception:
        print(cglob.head(20).to_string(index=False))
