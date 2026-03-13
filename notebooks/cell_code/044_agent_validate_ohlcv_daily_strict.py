from pathlib import Path
import re
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pandas.errors import ParserError

# =========================
# Config (sobrescribible desde notebook)
# =========================
DATA_ROOT = Path(globals().get("DATA_ROOT", r"D:\ohlcv_daily"))
RUN_ID = str(globals().get("RUN_ID", datetime.now().strftime("%Y%m%d_%H%M%S_ohlcv_daily")))
RUN_BASE = Path(globals().get("RUN_BASE", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\ohlcv_daily_audit"))
RUN_DIR = Path(globals().get("RUN_DIR", RUN_BASE / RUN_ID))
RUN_DIR.mkdir(parents=True, exist_ok=True)

MAX_FILES = int(globals().get("MAX_FILES", 3000))
RESET_STATE = bool(globals().get("RESET_STATE", False))
RESCAN_ALL = bool(globals().get("RESCAN_ALL", False))
RETRY_POLICY = str(globals().get("RETRY_POLICY", "hard_only"))  # hard_only | hard_and_soft
MAX_RETRY_ATTEMPTS = int(globals().get("MAX_RETRY_ATTEMPTS", 3))
DISCOVERY_MODE = str(globals().get("DISCOVERY_MODE", "fast")).lower()  # fast | full

MAX_TS_DATE_MISMATCH_PCT = float(globals().get("MAX_TS_DATE_MISMATCH_PCT", 0.0))
CLOSEABLE_SOFT_FAIL_CAUSES = set(globals().get("CLOSEABLE_SOFT_FAIL_CAUSES", []))
EXPECTED_TICKERS_FILE = Path(
    globals().get(
        "EXPECTED_TICKERS_FILE",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet",
    )
)

STATE_FILE = Path(globals().get("STATE_FILE", RUN_DIR / "ohlcv_daily_agent_state.json"))
EVENTS_HISTORY_CSV = Path(globals().get("EVENTS_HISTORY_CSV", RUN_DIR / "ohlcv_daily_events_history.csv"))
EVENTS_CURRENT_CSV = Path(globals().get("EVENTS_CURRENT_CSV", RUN_DIR / "ohlcv_daily_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", RUN_DIR / "retry_queue_ohlcv_daily.csv"))
RETRY_CURRENT_CSV = Path(globals().get("RETRY_CURRENT_CSV", RUN_DIR / "retry_queue_ohlcv_daily_current.csv"))
RETRY_ATTEMPTS_CSV = Path(globals().get("RETRY_ATTEMPTS_CSV", RUN_DIR / "retry_attempts_ohlcv_daily.csv"))
RETRY_FROZEN_CSV = Path(globals().get("RETRY_FROZEN_CSV", RUN_DIR / "retry_frozen_ohlcv_daily.csv"))
BATCH_MANIFEST_CSV = Path(globals().get("BATCH_MANIFEST_CSV", RUN_DIR / "batch_manifest_ohlcv_daily.csv"))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", RUN_DIR / "run_config_ohlcv_daily.json"))
LIVE_STATUS_JSON = Path(globals().get("LIVE_STATUS_JSON", RUN_DIR / "live_status_ohlcv_daily.json"))

REQUIRED_COLS = ["ticker", "date", "year", "o", "h", "l", "c", "v", "vw", "n", "t"]
PARTITION_RE = re.compile(
    r"[\\/]ticker=(?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]day_aggs_(?P<ticker_file>[^_]+)_(?P<year_file>\d{4})\.parquet$",
    flags=re.IGNORECASE,
)

EVENT_COLUMNS = [
    "file",
    "severity",
    "issues",
    "warns",
    "action",
    "rows",
    "ticker_path",
    "year_path",
    "processed_at_utc",
    "run_id",
]


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except ParserError:
        try:
            return pd.read_csv(path, on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _append_events_csv(path: Path, out_df: pd.DataFrame) -> None:
    if out_df.empty:
        return
    write_header = not path.exists()
    out_df.to_csv(path, mode="a", header=write_header, index=False, encoding="utf-8")


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {"processed": {}, "run_id_last": None, "updated_at_utc": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"processed": {}, "run_id_last": None, "updated_at_utc": None}


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_sig(p: Path):
    st = p.stat()
    return f"{st.st_size}_{st.st_mtime_ns}"


def _iter_parquet_files(root: Path):
    for p in root.rglob("*.parquet"):
        if p.is_file():
            yield p


def _safe_parse_listlike(v):
    if isinstance(v, list):
        return [str(x) for x in v]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s.replace("'", '"'))
            if isinstance(arr, list):
                return [str(x) for x in arr]
        except Exception:
            pass
    return [s]


def _decide_action(severity: str) -> str:
    if severity == "HARD_FAIL":
        return "RETRY"
    if severity == "SOFT_FAIL":
        return "RETRY" if RETRY_POLICY == "hard_and_soft" else "REVIEW"
    return "CLOSE"


def _is_soft_closeable(row: dict) -> bool:
    if str(row.get("severity", "")) != "SOFT_FAIL":
        return False
    warns = set(_safe_parse_listlike(row.get("warns", [])))
    if not warns:
        return False
    return warns.issubset(CLOSEABLE_SOFT_FAIL_CAUSES)


def _assess_file(fp: Path) -> dict:
    parsed = PARTITION_RE.search(str(fp))
    ticker_path = None
    year_path = None
    issues = []
    warns = []

    if parsed:
        ticker_path = parsed.group("ticker")
        year_path = int(parsed.group("year"))
        if parsed.group("ticker") != parsed.group("ticker_file"):
            issues.append("ticker_mismatch_in_filename_vs_partition")
        if int(parsed.group("year")) != int(parsed.group("year_file")):
            issues.append("year_mismatch_in_filename_vs_partition")
    else:
        issues.append("invalid_partition_path")

    rows = None
    if fp.stat().st_size == 0:
        issues.append("zero_byte_file")

    if not issues:
        try:
            pf = pq.ParquetFile(fp)
            rows = int(pf.metadata.num_rows)
            schema_cols = set(pf.schema.names)
            miss = [c for c in REQUIRED_COLS if c not in schema_cols]
            if miss:
                issues.append("missing_required_columns")
                df = pd.DataFrame()
            else:
                # Cargar solo columnas contractuales para reducir RAM.
                df = pf.read(columns=REQUIRED_COLS).to_pandas()
        except Exception:
            issues.append("parquet_unreadable")
            df = pd.DataFrame()

        if "df" in locals() and not df.empty:
            if len(df) == 0:
                issues.append("zero_rows")
            else:
                if df["date"].isna().any():
                    issues.append("null_date")
                if df["ticker"].isna().any():
                    issues.append("null_ticker")
                if df["year"].isna().any():
                    issues.append("null_year")

                if df["date"].astype(str).duplicated().any():
                    issues.append("duplicate_date_rows")

                if ((df["o"] < 0) | (df["h"] < 0) | (df["l"] < 0) | (df["c"] < 0) | (df["vw"] < 0)).any():
                    issues.append("negative_prices")
                if ((df["v"] < 0) | (df["n"] < 0)).any():
                    issues.append("negative_volume_or_trades")

                bad_ohlc = ((df["h"] < df[["o", "c", "l"]].max(axis=1)) | (df["l"] > df[["o", "c", "h"]].min(axis=1))).sum()
                if int(bad_ohlc) > 0:
                    issues.append(f"ohlc_inconsistency_rows:{int(bad_ohlc)}")

                if ticker_path is not None and not (df["ticker"].astype(str) == str(ticker_path)).all():
                    issues.append("ticker_value_mismatch_vs_path")
                if year_path is not None and not (pd.to_numeric(df["year"], errors="coerce").fillna(-1).astype(int) == int(year_path)).all():
                    issues.append("year_value_mismatch_vs_path")

                try:
                    _d = pd.to_datetime(df["date"], errors="coerce")
                    if _d.isna().any():
                        issues.append("date_parse_error")
                except Exception:
                    issues.append("date_parse_error")

                try:
                    ny_dates = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
                    mismatch_n = int((ny_dates != df["date"].astype(str)).sum())
                    mismatch_pct = (mismatch_n / len(df) * 100.0) if len(df) else 0.0
                    if mismatch_pct > MAX_TS_DATE_MISMATCH_PCT:
                        warns.append(f"timestamp_date_mismatch_gt_threshold:{mismatch_pct:.4f}%")
                    elif mismatch_n > 0:
                        warns.append(f"timestamp_date_mismatch_rows:{mismatch_n}")
                except Exception:
                    warns.append("timestamp_parse_issue")

                if (df["v"] == 0).mean() > 0.20:
                    warns.append("high_zero_volume_ratio")
                if pd.to_datetime(df["date"], errors="coerce").is_monotonic_increasing is False:
                    warns.append("date_not_sorted")
        else:
            if not issues:
                issues.append("zero_rows")

    if issues:
        severity = "HARD_FAIL"
    elif warns:
        severity = "SOFT_FAIL"
    else:
        severity = "PASS"

    return {
        "file": str(fp),
        "severity": severity,
        "issues": issues,
        "warns": warns,
        "action": _decide_action(severity),
        "rows": rows,
        "ticker_path": ticker_path,
        "year_path": year_path,
        "processed_at_utc": _now_utc(),
        "run_id": RUN_ID,
    }


if not DATA_ROOT.exists():
    raise FileNotFoundError(f"DATA_ROOT no existe: {DATA_ROOT}")

if RESET_STATE and STATE_FILE.exists():
    STATE_FILE.unlink(missing_ok=True)

state = _load_state(STATE_FILE)
if RESET_STATE:
    state["processed"] = {}

discovery_complete = False
all_files_count = 0
candidates = []

if DISCOVERY_MODE == "full":
    all_files = [p for p in _iter_parquet_files(DATA_ROOT)]
    all_files = sorted(all_files, key=lambda p: p.stat().st_mtime, reverse=True)
    all_files_count = len(all_files)
    discovery_complete = True
    for p in all_files:
        sp = str(p)
        if RESCAN_ALL:
            candidates.append(p)
        else:
            sig = _file_sig(p)
            if state.get("processed", {}).get(sp) != sig:
                candidates.append(p)
    batch = candidates[:MAX_FILES]
else:
    batch = []
    for p in _iter_parquet_files(DATA_ROOT):
        all_files_count += 1
        sp = str(p)
        if RESCAN_ALL:
            batch.append(p)
        else:
            sig = _file_sig(p)
            if state.get("processed", {}).get(sp) != sig:
                batch.append(p)
        if len(batch) >= MAX_FILES:
            break
    candidates = batch.copy()
    discovery_complete = len(batch) < MAX_FILES

print(f"RUN_ID: {RUN_ID}")
print(f"DATA_ROOT: {DATA_ROOT}")
print(f"RUN_DIR: {RUN_DIR}")
print(f"discovery_mode: {DISCOVERY_MODE}")
print(f"all_files_scanned: {all_files_count} | candidates: {len(candidates)} | batch: {len(batch)} | discovery_complete: {discovery_complete}")
print(f"RETRY_POLICY: {RETRY_POLICY} | MAX_RETRY_ATTEMPTS: {MAX_RETRY_ATTEMPTS}")
print(f"MAX_TS_DATE_MISMATCH_PCT: {MAX_TS_DATE_MISMATCH_PCT}")
print(f"CLOSEABLE_SOFT_FAIL_CAUSES: {sorted(CLOSEABLE_SOFT_FAIL_CAUSES)}")

run_cfg = {
    "run_id": RUN_ID,
    "started_at_utc": _now_utc(),
    "data_root": str(DATA_ROOT),
    "run_dir": str(RUN_DIR),
    "max_files": int(MAX_FILES),
    "discovery_mode": DISCOVERY_MODE,
    "rescan_all": bool(RESCAN_ALL),
    "retry_policy": RETRY_POLICY,
    "max_retry_attempts": int(MAX_RETRY_ATTEMPTS),
    "max_ts_date_mismatch_pct": float(MAX_TS_DATE_MISMATCH_PCT),
    "closeable_soft_fail_causes": sorted(CLOSEABLE_SOFT_FAIL_CAUSES),
    "state_file": str(STATE_FILE),
    "events_history_csv": str(EVENTS_HISTORY_CSV),
    "events_current_csv": str(EVENTS_CURRENT_CSV),
    "retry_queue_csv": str(RETRY_QUEUE_CSV),
    "retry_current_csv": str(RETRY_CURRENT_CSV),
    "retry_attempts_csv": str(RETRY_ATTEMPTS_CSV),
    "retry_frozen_csv": str(RETRY_FROZEN_CSV),
    "batch_manifest_csv": str(BATCH_MANIFEST_CSV),
    "live_status_json": str(LIVE_STATUS_JSON),
}
RUN_CONFIG_JSON.write_text(json.dumps(run_cfg, ensure_ascii=False, indent=2), encoding="utf-8")

out_rows = []
for fp in batch:
    rec = _assess_file(fp)
    out_rows.append(rec)
    state.setdefault("processed", {})[str(fp)] = _file_sig(fp)

state["run_id_last"] = RUN_ID
state["updated_at_utc"] = _now_utc()
_save_state(STATE_FILE, state)

out_df = pd.DataFrame(out_rows, columns=EVENT_COLUMNS) if out_rows else pd.DataFrame(columns=EVENT_COLUMNS)
if not out_df.empty:
    out_df["issues"] = out_df["issues"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    out_df["warns"] = out_df["warns"].apply(lambda x: json.dumps(x, ensure_ascii=False))

_append_events_csv(EVENTS_HISTORY_CSV, out_df)

hist = _read_csv_safe(EVENTS_HISTORY_CSV)
if hist.empty:
    current = pd.DataFrame(columns=EVENT_COLUMNS)
else:
    if "processed_at_utc" in hist.columns:
        hist = hist.sort_values("processed_at_utc")
    current = hist.drop_duplicates(subset=["file"], keep="last").copy()

current.to_csv(EVENTS_CURRENT_CSV, index=False, encoding="utf-8")

if not out_df.empty:
    batch_manifest = out_df[["file"]].copy().drop_duplicates()
else:
    batch_manifest = pd.DataFrame(columns=["file"])
batch_manifest.to_csv(BATCH_MANIFEST_CSV, index=False, encoding="utf-8")
batch_files_set = set(batch_manifest["file"].astype(str).tolist()) if not batch_manifest.empty else set()

rq = current.copy()
if not rq.empty:
    for c in ["issues", "warns"]:
        if c in rq.columns:
            rq[c] = rq[c].apply(_safe_parse_listlike)
    if RETRY_POLICY == "hard_and_soft":
        rq = rq[rq["severity"].isin(["HARD_FAIL", "SOFT_FAIL"])].copy()
    else:
        rq = rq[rq["severity"] == "HARD_FAIL"].copy()

    if not rq.empty:
        soft_close_mask = rq.apply(lambda r: _is_soft_closeable(r.to_dict()), axis=1)
        if len(soft_close_mask) == len(rq):
            rq = rq[~soft_close_mask].copy()

    for c in ["issues", "warns"]:
        if c not in rq.columns:
            rq[c] = []
    if not rq.empty:
        rq["issues"] = rq["issues"].apply(lambda x: json.dumps(x, ensure_ascii=False))
        rq["warns"] = rq["warns"].apply(lambda x: json.dumps(x, ensure_ascii=False))

rq.to_csv(RETRY_QUEUE_CSV, index=False, encoding="utf-8")
rq.to_csv(RETRY_CURRENT_CSV, index=False, encoding="utf-8")

attempts_old = _read_csv_safe(RETRY_ATTEMPTS_CSV)
if attempts_old.empty:
    attempts_old = pd.DataFrame(columns=["file", "attempts", "last_processed_at_utc", "last_severity", "last_run_id", "status"])

attempts_map = {
    str(r["file"]): {
        "attempts": int(r.get("attempts", 0)),
        "last_processed_at_utc": r.get("last_processed_at_utc"),
        "last_severity": r.get("last_severity"),
        "last_run_id": r.get("last_run_id"),
        "status": r.get("status", "active"),
    }
    for _, r in attempts_old.iterrows()
}

for _, r in rq.iterrows():
    f = str(r["file"])
    # Incrementar intentos solo para archivos realmente re-evaluados en este run.
    if f not in batch_files_set:
        continue
    prev = attempts_map.get(f, {"attempts": 0})
    n = int(prev.get("attempts", 0)) + 1
    attempts_map[f] = {
        "attempts": n,
        "last_processed_at_utc": r.get("processed_at_utc"),
        "last_severity": r.get("severity"),
        "last_run_id": RUN_ID,
        "status": "frozen" if n > MAX_RETRY_ATTEMPTS else "active",
    }

attempts_df = pd.DataFrame([
    {
        "file": f,
        "attempts": d["attempts"],
        "last_processed_at_utc": d["last_processed_at_utc"],
        "last_severity": d["last_severity"],
        "last_run_id": d["last_run_id"],
        "status": d["status"],
    }
    for f, d in attempts_map.items()
])

if attempts_df.empty:
    frozen_df = attempts_df.copy()
else:
    frozen_df = attempts_df[attempts_df["status"] == "frozen"].copy()

attempts_df.to_csv(RETRY_ATTEMPTS_CSV, index=False, encoding="utf-8")
frozen_df.to_csv(RETRY_FROZEN_CSV, index=False, encoding="utf-8")

sev_counts = current.groupby("severity", dropna=False).size().reset_index(name="count") if not current.empty else pd.DataFrame(columns=["severity", "count"])

# Cobertura esperada (si hay archivo de tickers objetivo disponible)
expected_tickers_total = None
expected_tickers_observed = None
expected_tickers_missing = None
if EXPECTED_TICKERS_FILE.exists():
    try:
        exp = pd.read_parquet(EXPECTED_TICKERS_FILE, columns=["ticker"])
        exp_set = set(exp["ticker"].astype("string").str.strip().str.upper().dropna().tolist())
        expected_tickers_total = int(len(exp_set))
        obs_set = set()
        for p in DATA_ROOT.glob("ticker=*"):
            if p.is_dir():
                obs_set.add(p.name.split("=", 1)[1].strip().upper())
        expected_tickers_observed = int(len(exp_set & obs_set))
        expected_tickers_missing = int(len(exp_set - obs_set))
    except Exception:
        pass

live = {
    "run_id": RUN_ID,
    "updated_at_utc": _now_utc(),
    "all_files_total": int(all_files_count) if discovery_complete else None,
    "all_files_scanned": int(all_files_count),
    "discovery_complete": bool(discovery_complete),
    "candidates": int(len(candidates)),
    "batch_files": int(len(batch)),
    "events_current_rows": int(len(current)),
    "retry_pending_files": int(rq["file"].nunique()) if (not rq.empty and "file" in rq.columns) else int(len(rq)),
    "retry_frozen_files": int(len(frozen_df)),
    "severity_counts_current": {str(r["severity"]): int(r["count"]) for _, r in sev_counts.iterrows()},
    "coverage_expected_tickers": {
        "expected_total": expected_tickers_total,
        "observed_in_data_root": expected_tickers_observed,
        "missing_in_data_root": expected_tickers_missing,
    },
    "audit_scope_warning": (
        "partial_scan_max_files_limit"
        if (DISCOVERY_MODE == "fast" and len(batch) >= MAX_FILES)
        else None
    ),
    "paths": {
        "events_current_csv": str(EVENTS_CURRENT_CSV),
        "events_history_csv": str(EVENTS_HISTORY_CSV),
        "retry_current_csv": str(RETRY_CURRENT_CSV),
        "retry_attempts_csv": str(RETRY_ATTEMPTS_CSV),
        "retry_frozen_csv": str(RETRY_FROZEN_CSV),
        "state_file": str(STATE_FILE),
        "batch_manifest_csv": str(BATCH_MANIFEST_CSV),
    },
}
LIVE_STATUS_JSON.write_text(json.dumps(live, ensure_ascii=False, indent=2), encoding="utf-8")

print("\nResumen por severidad (current):")
if sev_counts.empty:
    print("(sin datos)")
else:
    print(sev_counts.to_string(index=False))

print("\nTop 10 causas (issues+warns) en current:")
if current.empty:
    print("(sin datos)")
else:
    rows = []
    for _, r in current.iterrows():
        for it in _safe_parse_listlike(r.get("issues", [])):
            rows.append(("issue", it))
        for wt in _safe_parse_listlike(r.get("warns", [])):
            rows.append(("warn", wt))
    if rows:
        cdf = pd.DataFrame(rows, columns=["cause_type", "cause"])
        top = cdf.groupby(["cause_type", "cause"], dropna=False).size().reset_index(name="count").sort_values("count", ascending=False).head(10)
        print(top.to_string(index=False))
    else:
        print("(sin causas)")

print("\nSaved artifacts:")
for p in [
    RUN_CONFIG_JSON,
    STATE_FILE,
    EVENTS_HISTORY_CSV,
    EVENTS_CURRENT_CSV,
    RETRY_QUEUE_CSV,
    RETRY_CURRENT_CSV,
    RETRY_ATTEMPTS_CSV,
    RETRY_FROZEN_CSV,
    BATCH_MANIFEST_CSV,
    LIVE_STATUS_JSON,
]:
    print("-", p)

if live.get("audit_scope_warning"):
    print("\nWARNING:", live["audit_scope_warning"], "| Sube MAX_FILES o usa DISCOVERY_MODE='full' para certificar cobertura completa.")
if expected_tickers_total is not None:
    print(
        f"\nExpected ticker coverage: expected={expected_tickers_total} observed={expected_tickers_observed} missing={expected_tickers_missing}"
    )
