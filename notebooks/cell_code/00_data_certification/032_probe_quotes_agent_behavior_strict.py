from pathlib import Path
import re
import json
from datetime import datetime, timezone
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from pandas.errors import ParserError

# ===== Configurable desde celda =====
EXPECTED_QUOTES_ROOT = Path(globals().get("EXPECTED_QUOTES_ROOT", r"D:\quotes"))
PROBE_ROOT = Path(globals().get("PROBE_ROOT", r"D:\quotes"))
MAX_FILES = int(globals().get("MAX_FILES", 20))
RESET_STATE = bool(globals().get("RESET_STATE", False))
RESCAN_ALL = bool(globals().get("RESCAN_ALL", False))

STATE_FILE = Path(
    globals().get(
        "STATE_FILE",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_strict_state.json",
    )
)
EVENTS_CSV = Path(
    globals().get(
        "EVENTS_CSV",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_strict_events.csv",
    )
)
GRANULAR_DIR = Path(
    globals().get(
        "GRANULAR_DIR",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\granular_strict",
    )
)
RETRY_POLICY = globals().get("RETRY_POLICY", "hard_only")  # hard_only | hard_and_soft
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_strict.csv"))
RETRY_QUEUE_PARQUET = Path(globals().get("RETRY_QUEUE_PARQUET", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_strict.parquet"))
RUN_ID = str(globals().get("RUN_ID", datetime.now().strftime("%Y%m%d_%H%M%S_quotes")))
MAX_RETRY_ATTEMPTS = int(globals().get("MAX_RETRY_ATTEMPTS", 3))
EVENTS_HISTORY_CSV = Path(globals().get("EVENTS_HISTORY_CSV", EVENTS_CSV))
EVENTS_CURRENT_CSV = Path(globals().get("EVENTS_CURRENT_CSV", EVENTS_CSV.with_name("quotes_agent_strict_events_current.csv")))
RETRY_CURRENT_CSV = Path(globals().get("RETRY_CURRENT_CSV", RETRY_QUEUE_CSV.with_name("retry_queue_quotes_strict_current.csv")))
RETRY_ATTEMPTS_CSV = Path(globals().get("RETRY_ATTEMPTS_CSV", RETRY_QUEUE_CSV.with_name("retry_attempts_quotes_strict.csv")))
RETRY_FROZEN_CSV = Path(globals().get("RETRY_FROZEN_CSV", RETRY_QUEUE_CSV.with_name("retry_frozen_quotes_strict.csv")))
BATCH_MANIFEST_CSV = Path(globals().get("BATCH_MANIFEST_CSV", EVENTS_CSV.with_name("batch_manifest_quotes_strict.csv")))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", EVENTS_CSV.with_name("run_config_quotes_strict.json")))
LIVE_STATUS_JSON = Path(globals().get("LIVE_STATUS_JSON", EVENTS_CSV.with_name("live_status_quotes_strict.json")))
DISCOVERY_STATE_JSON = Path(globals().get("DISCOVERY_STATE_JSON", EVENTS_CSV.with_name("quotes_discovery_state.json")))
DISCOVERED_INDEX_PARQUET = Path(globals().get("DISCOVERED_INDEX_PARQUET", EVENTS_CSV.with_name("quotes_discovered_index.parquet")))
PENDING_QUEUE_PARQUET = Path(globals().get("PENDING_QUEUE_PARQUET", EVENTS_CSV.with_name("quotes_pending_queue.parquet")))
FULL_RESCAN_EVERY_MIN = int(globals().get("FULL_RESCAN_EVERY_MIN", 180))
FORCE_FULL_RESCAN = bool(globals().get("FORCE_FULL_RESCAN", False))
AUTO_FULL_SCAN_ON_DRAIN = bool(globals().get("AUTO_FULL_SCAN_ON_DRAIN", True))
RECONCILIATION_JSON = Path(globals().get("RECONCILIATION_JSON", EVENTS_CSV.with_name("quotes_reconciliation_status.json")))

# Umbral estricto para mercado cruzado
MAX_CROSSED_RATIO_PCT = float(globals().get("MAX_CROSSED_RATIO_PCT", 0.01))
# Hard-fail operacional (independiente del umbral anterior)
HARD_FAIL_CROSSED_PCT = float(globals().get("HARD_FAIL_CROSSED_PCT", 5.0))
# Hard-fail por enterizacion masiva + cruces altos
HARD_FAIL_ASK_INTEGER_PCT = float(globals().get("HARD_FAIL_ASK_INTEGER_PCT", 95.0))
HARD_FAIL_ASK_INT_CROSSED_PCT = float(globals().get("HARD_FAIL_ASK_INT_CROSSED_PCT", 20.0))
# Por defecto, ningun SOFT_FAIL cierra tarea. Solo cierra si su warn principal
# esta en esta whitelist.
CLOSEABLE_SOFT_FAIL_CAUSES = set(globals().get("CLOSEABLE_SOFT_FAIL_CAUSES", []))

PARTITION_RE = re.compile(r"(?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]month=(?P<month>\d{2})[\\/]day=(?P<day>\d{2})[\\/]quotes\.parquet$")

RULES_DICTIONARY_STRICT = {
    "required_columns": [
        "timestamp",
        "bid_price",
        "ask_price",
        "bid_size",
        "ask_size",
    ],
    "hard_fail": {
        "parquet_unreadable": True,
        "zero_byte_file": True,
        "invalid_partition_path": True,
        "zero_rows": True,
        "missing_required_columns": True,
        "negative_prices_any_row": True,
        "crossed_ratio_gt_threshold": f"> {MAX_CROSSED_RATIO_PCT}%",
        "crossed_ratio_gt_hard_cap": f"> {HARD_FAIL_CROSSED_PCT}% (hard cap)",
        "ask_integer_with_crossed_anomaly": f"ask_integer_pct > {HARD_FAIL_ASK_INTEGER_PCT}% AND crossed_ratio_pct > {HARD_FAIL_ASK_INT_CROSSED_PCT}%",
    },
    "soft_fail": {
        "crossed_rows_present_but_under_threshold": True,
        "dtype_mismatch": True,
    },
}

EVENT_COLUMNS = [
    "file",
    "rows",
    "severity",
    "issues",
    "warns",
    "action",
    "crossed_rows",
    "crossed_ratio_pct",
    "negative_price_rows",
    "missing_required_cols",
    "dtype_mismatches",
    "ask_integer_pct",
    "bid_integer_pct",
    "ask_eq_round_bid_pct",
    "processed_at_utc",
    "run_id",
]

EXPECTED_DTYPES = {
    "timestamp": "int64",
    "bid_price": "float64",
    "ask_price": "float64",
    "bid_size": "int64",
    "ask_size": "int64",
}


def _read_events_csv(path: Path):
    if not path.exists():
        return pd.DataFrame(columns=EVENT_COLUMNS)
    try:
        df = pd.read_csv(path)
        return df.reindex(columns=EVENT_COLUMNS)
    except ParserError:
        try:
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
            return df.reindex(columns=EVENT_COLUMNS)
        except Exception:
            broken = path.with_suffix(path.suffix + ".broken")
            path.replace(broken)
            print(f"WARNING: malformed events csv moved to: {broken}")
            return pd.DataFrame(columns=EVENT_COLUMNS)
    except Exception:
        return pd.DataFrame(columns=EVENT_COLUMNS)


def _append_events_csv(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        return
    new_df = df.copy().reindex(columns=EVENT_COLUMNS)
    hist_df = _read_events_csv(path)
    merged = pd.concat([hist_df, new_df], ignore_index=True)
    merged = merged.reindex(columns=EVENT_COLUMNS)
    merged.to_csv(path, index=False, encoding="utf-8")


def _load_state(path: Path):
    if RESET_STATE:
        return {"processed_files": [], "total_processed": 0, "updated_utc": None, "last_scan_utc": None, "run_id_last": None}
    if not path.exists():
        return {"processed_files": [], "total_processed": 0, "updated_utc": None, "last_scan_utc": None, "run_id_last": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        data.setdefault("processed_files", [])
        data.setdefault("total_processed", len(data["processed_files"]))
        data.setdefault("last_scan_utc", None)
        data.setdefault("run_id_last", None)
        return data
    except Exception:
        return {"processed_files": [], "total_processed": 0, "updated_utc": None, "last_scan_utc": None, "run_id_last": None}


def _save_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_utc"] = datetime.now(timezone.utc).isoformat()
    state["last_scan_utc"] = state["updated_utc"]
    state["run_id_last"] = RUN_ID
    state["total_processed"] = len(state.get("processed_files", []))
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _safe_parse_listlike(v):
    if isinstance(v, list):
        return [str(x) for x in v if str(x) not in ("", "None")]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    try:
        import ast
        x = ast.literal_eval(s)
        if isinstance(x, list):
            return [str(i) for i in x if str(i) not in ("", "None")]
        return [str(x)]
    except Exception:
        return [s]


def _load_discovery_state(path: Path):
    if not path.exists():
        return {"last_full_scan_utc": None, "cycle_count": 0, "ticker_dir_mtime": {}}
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
        st.setdefault("last_full_scan_utc", None)
        st.setdefault("cycle_count", 0)
        st.setdefault("ticker_dir_mtime", {})
        return st
    except Exception:
        return {"last_full_scan_utc": None, "cycle_count": 0, "ticker_dir_mtime": {}}


def _save_discovery_state(path: Path, st: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(st, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_parquet_df(path: Path, columns: list):
    if not path.exists():
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_parquet(path)
        for c in columns:
            if c not in df.columns:
                df[c] = pd.NA
        return df[columns].copy()
    except Exception:
        return pd.DataFrame(columns=columns)


def _decide_action(severity: str):
    if severity == "HARD_FAIL":
        return "quarantine_and_retry"
    if severity == "SOFT_FAIL":
        return "warn_and_retry_later"
    return "count_coverage"


def _is_soft_closeable_row(row: pd.Series) -> bool:
    sev = str(row.get("severity", ""))
    if sev != "SOFT_FAIL":
        return False
    issues = _safe_parse_listlike(row.get("issues"))
    warns = _safe_parse_listlike(row.get("warns"))
    if issues:
        return False
    if not warns:
        return False
    # Cierra solo si TODOS los warns del row están en whitelist
    return all(w in CLOSEABLE_SOFT_FAIL_CAUSES for w in warns)


def _assess_file(fp: Path):
    issues = []
    warns = []
    crossed_rows = 0
    crossed_ratio_pct = 0.0
    negative_price_rows = 0
    missing_required_cols = []
    dtype_mismatches = []
    ask_integer_pct = 0.0
    bid_integer_pct = 0.0
    ask_eq_round_bid_pct = 0.0

    if fp.stat().st_size == 0:
        issues.append("zero_byte_file")

    rel_ok = False
    try:
        rel = fp.relative_to(EXPECTED_QUOTES_ROOT)
        rel_ok = True
    except Exception:
        rel = fp

    if not PARTITION_RE.search(str(rel if rel_ok else fp)):
        issues.append("invalid_partition_path")

    rows = None
    cols = []
    dtypes = {}

    try:
        # Read the parquet file directly, without dataset/partition inference from parent dirs.
        table = pq.ParquetFile(fp).read()
        rows = table.num_rows
        cols = list(table.schema.names)
        dtypes = {f.name: str(f.type) for f in table.schema}
    except Exception:
        issues.append("parquet_unreadable")
        return {
            "file": str(fp),
            "rows": rows,
            "severity": "HARD_FAIL",
            "issues": issues,
            "warns": warns,
            "action": _decide_action("HARD_FAIL"),
            "crossed_rows": crossed_rows,
            "crossed_ratio_pct": crossed_ratio_pct,
            "negative_price_rows": negative_price_rows,
            "missing_required_cols": missing_required_cols,
            "dtype_mismatches": dtype_mismatches,
            "ask_integer_pct": ask_integer_pct,
            "bid_integer_pct": bid_integer_pct,
            "ask_eq_round_bid_pct": ask_eq_round_bid_pct,
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": RUN_ID,
        }

    if rows is not None and rows < 1:
        issues.append("zero_rows")

    for c in RULES_DICTIONARY_STRICT["required_columns"]:
        if c not in cols:
            missing_required_cols.append(c)
    if missing_required_cols:
        issues.append("missing_required_columns")

    for c, exp in EXPECTED_DTYPES.items():
        if c in dtypes:
            got = dtypes[c]
            # mapeo simple arrow->pandas esperado
            if (c.endswith("_price") and not ("double" in got or "float" in got)) or (c in ["timestamp", "bid_size", "ask_size"] and "int" not in got):
                dtype_mismatches.append(f"{c}:{got}")
    if dtype_mismatches:
        warns.append("dtype_mismatch")

    try:
        df = table.select([c for c in ["bid_price", "ask_price"] if c in cols]).to_pandas()
        if {"bid_price", "ask_price"}.issubset(df.columns):
            bid = pd.to_numeric(df["bid_price"], errors="coerce")
            ask = pd.to_numeric(df["ask_price"], errors="coerce")
            valid = np.isfinite(bid) & np.isfinite(ask)
            bid = bid[valid]
            ask = ask[valid]
            base_n = max(int(len(bid)), 1)

            crossed_rows = int((bid > ask).sum())
            negative_price_rows = int(((bid < 0) | (ask < 0)).sum())
            crossed_ratio_pct = float((crossed_rows / base_n) * 100.0)
            ask_integer_pct = float(np.mean(np.isclose((ask % 1.0), 0.0)) * 100.0) if base_n > 0 else 0.0
            bid_integer_pct = float(np.mean(np.isclose((bid % 1.0), 0.0)) * 100.0) if base_n > 0 else 0.0
            ask_eq_round_bid_pct = float(np.mean(np.isclose(ask, np.round(bid))) * 100.0) if base_n > 0 else 0.0

            if negative_price_rows > 0:
                issues.append("negative_prices_any_row")

            if crossed_rows > 0:
                # hard cap operativo para evitar pasar archivos muy deteriorados aunque el umbral estricto sea mas laxo
                if crossed_ratio_pct > HARD_FAIL_CROSSED_PCT:
                    issues.append("crossed_ratio_gt_hard_cap")
                if crossed_ratio_pct > MAX_CROSSED_RATIO_PCT:
                    issues.append("crossed_ratio_gt_threshold")
                else:
                    warns.append("crossed_rows_present_but_under_threshold")

            if ask_integer_pct > HARD_FAIL_ASK_INTEGER_PCT and crossed_ratio_pct > HARD_FAIL_ASK_INT_CROSSED_PCT:
                issues.append("ask_integer_with_crossed_anomaly")
    except Exception:
        warns.append("soft_rule_eval_error")

    if issues:
        severity = "HARD_FAIL"
    elif warns:
        severity = "SOFT_FAIL"
    else:
        severity = "PASS"

    return {
        "file": str(fp),
        "rows": rows,
        "severity": severity,
        "issues": issues,
        "warns": warns,
        "action": _decide_action(severity),
        "crossed_rows": crossed_rows,
        "crossed_ratio_pct": crossed_ratio_pct,
        "negative_price_rows": negative_price_rows,
        "missing_required_cols": missing_required_cols,
        "dtype_mismatches": dtype_mismatches,
        "ask_integer_pct": ask_integer_pct,
        "bid_integer_pct": bid_integer_pct,
        "ask_eq_round_bid_pct": ask_eq_round_bid_pct,
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
    }


print(f"EXPECTED_QUOTES_ROOT: {EXPECTED_QUOTES_ROOT}")
print(f"PROBE_ROOT: {PROBE_ROOT}")
print(f"STATE_FILE: {STATE_FILE}")
print(f"EVENTS_CSV: {EVENTS_CSV}")
print(f"GRANULAR_DIR: {GRANULAR_DIR}")
print(f"MAX_FILES: {MAX_FILES} | RESET_STATE={RESET_STATE} | RESCAN_ALL={RESCAN_ALL}")
print(f"MAX_CROSSED_RATIO_PCT: {MAX_CROSSED_RATIO_PCT}")
print(f"HARD_FAIL_CROSSED_PCT: {HARD_FAIL_CROSSED_PCT}")
print(f"HARD_FAIL_ASK_INTEGER_PCT: {HARD_FAIL_ASK_INTEGER_PCT}")
print(f"HARD_FAIL_ASK_INT_CROSSED_PCT: {HARD_FAIL_ASK_INT_CROSSED_PCT}")
print(f"CLOSEABLE_SOFT_FAIL_CAUSES: {sorted(CLOSEABLE_SOFT_FAIL_CAUSES)}")
print(f"FULL_RESCAN_EVERY_MIN: {FULL_RESCAN_EVERY_MIN}")
print(f"FORCE_FULL_RESCAN: {FORCE_FULL_RESCAN}")
print(f"AUTO_FULL_SCAN_ON_DRAIN: {AUTO_FULL_SCAN_ON_DRAIN}")
print(f"RUN_ID: {RUN_ID}")
print("\nDiccionario de reglas (estricto):")
print(json.dumps(RULES_DICTIONARY_STRICT, indent=2, ensure_ascii=False))

# Persistencia de configuracion de corrida para consumo de agente 03/monitores
run_cfg = {
    "run_id": RUN_ID,
    "expected_quotes_root": str(EXPECTED_QUOTES_ROOT),
    "probe_root": str(PROBE_ROOT),
    "max_files": MAX_FILES,
    "reset_state": RESET_STATE,
    "rescan_all": RESCAN_ALL,
    "retry_policy": RETRY_POLICY,
    "max_crossed_ratio_pct": MAX_CROSSED_RATIO_PCT,
    "hard_fail_crossed_pct": HARD_FAIL_CROSSED_PCT,
    "hard_fail_ask_integer_pct": HARD_FAIL_ASK_INTEGER_PCT,
    "hard_fail_ask_int_crossed_pct": HARD_FAIL_ASK_INT_CROSSED_PCT,
    "closeable_soft_fail_causes": sorted(CLOSEABLE_SOFT_FAIL_CAUSES),
    "max_retry_attempts": MAX_RETRY_ATTEMPTS,
    "state_file": str(STATE_FILE),
    "events_history_csv": str(EVENTS_HISTORY_CSV),
    "events_current_csv": str(EVENTS_CURRENT_CSV),
    "retry_queue_csv": str(RETRY_QUEUE_CSV),
    "retry_current_csv": str(RETRY_CURRENT_CSV),
    "retry_frozen_csv": str(RETRY_FROZEN_CSV),
    "retry_attempts_csv": str(RETRY_ATTEMPTS_CSV),
    "batch_manifest_csv": str(BATCH_MANIFEST_CSV),
    "live_status_json": str(LIVE_STATUS_JSON),
    "discovery_state_json": str(DISCOVERY_STATE_JSON),
    "discovered_index_parquet": str(DISCOVERED_INDEX_PARQUET),
    "pending_queue_parquet": str(PENDING_QUEUE_PARQUET),
    "full_rescan_every_min": FULL_RESCAN_EVERY_MIN,
    "force_full_rescan": FORCE_FULL_RESCAN,
    "auto_full_scan_on_drain": AUTO_FULL_SCAN_ON_DRAIN,
    "reconciliation_json": str(RECONCILIATION_JSON),
}
RUN_CONFIG_JSON.parent.mkdir(parents=True, exist_ok=True)
RUN_CONFIG_JSON.write_text(json.dumps(run_cfg, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"RUN_CONFIG_JSON: {RUN_CONFIG_JSON}")
print(f"LIVE_STATUS_JSON: {LIVE_STATUS_JSON}")

if not PROBE_ROOT.exists():
    print("PROBE_ROOT not found")
else:
    state = _load_state(STATE_FILE)
    processed_set = set(state.get("processed_files", []))
    dstate = _load_discovery_state(DISCOVERY_STATE_JSON)
    dstate["cycle_count"] = int(dstate.get("cycle_count", 0)) + 1

    now_utc = datetime.now(timezone.utc)
    do_full_scan = bool(FORCE_FULL_RESCAN)
    last_full = dstate.get("last_full_scan_utc")
    if not do_full_scan:
        if not last_full:
            do_full_scan = True
        else:
            try:
                dt_last = datetime.fromisoformat(str(last_full).replace("Z", "+00:00"))
                do_full_scan = ((now_utc - dt_last).total_seconds() >= FULL_RESCAN_EVERY_MIN * 60)
            except Exception:
                do_full_scan = True

    ticker_dir_mtime = dstate.get("ticker_dir_mtime", {})
    scan_dirs = []
    try:
        ticker_dirs = [d for d in PROBE_ROOT.iterdir() if d.is_dir()]
    except Exception:
        ticker_dirs = []

    for td in ticker_dirs:
        key = str(td)
        try:
            mtime_ns = int(td.stat().st_mtime_ns)
        except Exception:
            mtime_ns = 0
        prev = ticker_dir_mtime.get(key)
        if do_full_scan or prev != mtime_ns:
            scan_dirs.append(td)
        ticker_dir_mtime[key] = mtime_ns

    discovered_cols = ["file", "mtime_ns", "size", "last_seen_utc"]
    discovered = _load_parquet_df(DISCOVERED_INDEX_PARQUET, discovered_cols)
    discovered_idx = {str(r["file"]): r for _, r in discovered.iterrows()} if not discovered.empty else {}

    changed_rows = []
    for td in scan_dirs:
        for p in td.rglob("quotes.parquet"):
            fp = str(p)
            try:
                stp = p.stat()
                mtime_ns = int(stp.st_mtime_ns)
                size = int(stp.st_size)
            except Exception:
                mtime_ns = 0
                size = -1
            prev = discovered_idx.get(fp)
            is_new_or_changed = (
                prev is None
                or int(prev.get("mtime_ns", -1)) != mtime_ns
                or int(prev.get("size", -1)) != size
            )
            changed_rows.append({"file": fp, "mtime_ns": mtime_ns, "size": size, "last_seen_utc": now_utc.isoformat()})
            if is_new_or_changed:
                reason = "new" if prev is None else "changed"
                changed_rows[-1]["_reason"] = reason

    # upsert discovered index
    if changed_rows:
        upd = pd.DataFrame(changed_rows)[["file", "mtime_ns", "size", "last_seen_utc"]]
        merged_disc = pd.concat([discovered, upd], ignore_index=True)
        merged_disc = merged_disc.sort_values("last_seen_utc").drop_duplicates(subset=["file"], keep="last")
    else:
        merged_disc = discovered.copy()
    DISCOVERED_INDEX_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    merged_disc.to_parquet(DISCOVERED_INDEX_PARQUET, index=False)

    # pending queue incremental
    pending_cols = ["file", "discovered_utc", "reason", "mtime_ns", "size"]
    pending_df = _load_parquet_df(PENDING_QUEUE_PARQUET, pending_cols)
    new_pending = []
    for r in changed_rows:
        if "_reason" in r:
            new_pending.append({
                "file": r["file"],
                "discovered_utc": now_utc.isoformat(),
                "reason": r["_reason"],
                "mtime_ns": r["mtime_ns"],
                "size": r["size"],
            })

    if RESCAN_ALL:
        # forzar cola con todo el index conocido
        new_pending.extend(
            [
                {
                    "file": str(f),
                    "discovered_utc": now_utc.isoformat(),
                    "reason": "rescan_all",
                    "mtime_ns": int(m),
                    "size": int(s),
                }
                for f, m, s in merged_disc[["file", "mtime_ns", "size"]].itertuples(index=False, name=None)
            ]
        )

    if new_pending:
        pending_df = pd.concat([pending_df, pd.DataFrame(new_pending)], ignore_index=True)
        pending_df = pending_df.sort_values("discovered_utc").drop_duplicates(subset=["file"], keep="last")

    # limpiar cola: archivos ya procesados y no cambiados (cuando reason no sea changed)
    if not pending_df.empty:
        pending_df = pending_df[~pending_df["file"].astype(str).isin(set())].copy()

    # seleccionar lote a procesar
    pending_df = pending_df.sort_values("discovered_utc")
    candidates = [Path(f) for f in pending_df["file"].astype(str).tolist() if Path(f).exists()]
    files = candidates[:MAX_FILES]

    # métricas operativas
    all_files = merged_disc["file"].astype(str).tolist() if not merged_disc.empty else []
    pending = [str(f) for f in candidates]

    # Si se drenó la cola, forzar full scan de reconciliación una vez (además del full periódico)
    drain_full_triggered = False
    drain_new_pending = 0
    if AUTO_FULL_SCAN_ON_DRAIN and (not do_full_scan) and len(pending) == 0:
        drain_full_triggered = True
        do_full_scan = True
        all_files_full = sorted(str(p) for p in PROBE_ROOT.rglob("quotes.parquet"))
        full_set = set(all_files_full)
        disc_set = set(all_files)
        missing_in_index = sorted(full_set - disc_set)

        if missing_in_index:
            now_iso = now_utc.isoformat()
            add = pd.DataFrame(
                {
                    "file": missing_in_index,
                    "discovered_utc": [now_iso] * len(missing_in_index),
                    "reason": ["drain_full_reconcile"] * len(missing_in_index),
                    "mtime_ns": [pd.NA] * len(missing_in_index),
                    "size": [pd.NA] * len(missing_in_index),
                }
            )
            pending_df = pd.concat([pending_df, add], ignore_index=True)
            pending_df = pending_df.sort_values("discovered_utc").drop_duplicates(subset=["file"], keep="last")
            drain_new_pending = len(missing_in_index)

        # reconstruir candidatos tras reconciliación
        pending_df = pending_df.sort_values("discovered_utc")
        candidates = [Path(f) for f in pending_df["file"].astype(str).tolist() if Path(f).exists()]
        files = candidates[:MAX_FILES]
        all_files = all_files_full
        pending = [str(f) for f in candidates]

    print(f"files_discovered_total: {len(all_files)}")
    print(f"files_already_processed: {len(processed_set)}")
    print(f"files_pending: {len(pending)}")
    print(f"files_to_process_now: {len(files)}")
    print(f"discovery_mode: {'FULL' if do_full_scan else 'INCREMENTAL'} | scan_dirs={len(scan_dirs)}")
    if drain_full_triggered:
        print(f"drain_reconciliation_full_scan: triggered | new_pending_detected={drain_new_pending}")

    # Manifest del lote objetivo de esta ejecucion (base para missing_in_events en agente 03)
    BATCH_MANIFEST_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"file": [str(f) for f in files]}).to_csv(BATCH_MANIFEST_CSV, index=False, encoding="utf-8")
    print(f"batch_manifest_saved: {BATCH_MANIFEST_CSV}")

    out_df = pd.DataFrame([_assess_file(fp) for fp in files])

    if not out_df.empty:
        _append_events_csv(EVENTS_HISTORY_CSV, out_df)

        close_mask = (out_df["severity"] == "PASS")
        if not out_df.empty:
            soft_closeable_mask = out_df.apply(_is_soft_closeable_row, axis=1)
            close_mask = close_mask | soft_closeable_mask
        close_files = set(out_df.loc[close_mask, "file"].astype(str).tolist())
        state["processed_files"] = list(processed_set.union(close_files))
        _save_state(STATE_FILE, state)

        # quitar procesados de cola incremental
        done_set = set(out_df["file"].astype(str).tolist())
        if not pending_df.empty:
            pending_df = pending_df[~pending_df["file"].astype(str).isin(done_set)].copy()

    # persistir cola + estado discovery
    PENDING_QUEUE_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    pending_df.to_parquet(PENDING_QUEUE_PARQUET, index=False)
    dstate["ticker_dir_mtime"] = ticker_dir_mtime
    if do_full_scan:
        dstate["last_full_scan_utc"] = now_utc.isoformat()
    _save_discovery_state(DISCOVERY_STATE_JSON, dstate)

    # Snapshot de reconciliación
    try:
        recon = {
            "run_id": RUN_ID,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "discovery_mode": "FULL" if do_full_scan else "INCREMENTAL",
            "drain_reconciliation_full_scan": bool(drain_full_triggered),
            "drain_new_pending_detected": int(drain_new_pending),
            "files_discovered_total": int(len(all_files)),
            "files_pending_total": int(len(pending)),
            "files_to_process_now": int(len(files)),
            "processed_files_total_state": int(state.get("total_processed", 0)) if isinstance(state, dict) else None,
            "pending_queue_parquet": str(PENDING_QUEUE_PARQUET),
            "discovered_index_parquet": str(DISCOVERED_INDEX_PARQUET),
            "discovery_state_json": str(DISCOVERY_STATE_JSON),
        }
        RECONCILIATION_JSON.parent.mkdir(parents=True, exist_ok=True)
        RECONCILIATION_JSON.write_text(json.dumps(recon, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"reconciliation_status_saved: {RECONCILIATION_JSON}")
    except Exception as ex:
        print(f"WARNING: no se pudo guardar RECONCILIATION_JSON: {ex}")

    try:
        display(out_df)
    except Exception:
        print(out_df.to_string(index=False))

    print("\nResumen por severidad (run):")
    if out_df.empty:
        print("No new files processed in this run.")
    else:
        try:
            display(out_df.groupby("severity", dropna=False).size().reset_index(name="count"))
        except Exception:
            print(out_df.groupby("severity", dropna=False).size())

    hist = _read_events_csv(EVENTS_HISTORY_CSV)
    if not hist.empty and "processed_at_utc" in hist.columns:
        hist["processed_at_utc"] = pd.to_datetime(hist["processed_at_utc"], errors="coerce")
        hist = hist.sort_values("processed_at_utc")
    current = hist.drop_duplicates(subset=["file"], keep="last") if (not hist.empty and "file" in hist.columns) else hist.copy()
    EVENTS_CURRENT_CSV.parent.mkdir(parents=True, exist_ok=True)
    current.to_csv(EVENTS_CURRENT_CSV, index=False, encoding="utf-8")

    print("\nResumen historico acumulado:")
    try:
        display(hist.groupby("severity", dropna=False).size().reset_index(name="count"))
    except Exception:
        print(hist.groupby("severity", dropna=False).size())

    print("\nResumen current (ultimo estado por file):")
    try:
        display(current.groupby("severity", dropna=False).size().reset_index(name="count"))
    except Exception:
        print(current.groupby("severity", dropna=False).size())

    if not current.empty:
        parsed = current.copy()
        ext = parsed["file"].str.extract(
            r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
        )
        parsed = pd.concat([parsed, ext], axis=1)

        errors_df = parsed[parsed["severity"] != "PASS"].copy()
        causes = []
        for _, r in errors_df.iterrows():
            issues = r.get("issues", [])
            warns = r.get("warns", [])
            if not isinstance(issues, list):
                issues = [str(issues)] if pd.notna(issues) and str(issues) else []
            if not isinstance(warns, list):
                warns = [str(warns)] if pd.notna(warns) and str(warns) else []
            for it in issues:
                causes.append({"cause_type": "issue", "cause": it, "severity": r.get("severity"), "file": r.get("file"), "ticker": r.get("ticker")})
            for wt in warns:
                causes.append({"cause_type": "warn", "cause": wt, "severity": r.get("severity"), "file": r.get("file"), "ticker": r.get("ticker")})

        causes_df = pd.DataFrame(causes)
        cause_summary = (
            causes_df.groupby(["cause_type", "cause"], dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
            if not causes_df.empty
            else pd.DataFrame(columns=["cause_type", "cause", "count"])
        )

        by_ticker = (
            parsed.groupby("ticker", dropna=False)
            .agg(
                files_total=("file", "count"),
                pass_count=("severity", lambda s: int((s == "PASS").sum())),
                soft_fail_count=("severity", lambda s: int((s == "SOFT_FAIL").sum())),
                hard_fail_count=("severity", lambda s: int((s == "HARD_FAIL").sum())),
                crossed_rows_total=("crossed_rows", "sum"),
            )
            .reset_index()
        )

        GRANULAR_DIR.mkdir(parents=True, exist_ok=True)
        errors_df.to_csv(GRANULAR_DIR / "quotes_agent_strict_errors_granular.csv", index=False, encoding="utf-8")
        causes_df.to_csv(GRANULAR_DIR / "quotes_agent_strict_errors_causes_granular.csv", index=False, encoding="utf-8")
        cause_summary.to_csv(GRANULAR_DIR / "quotes_agent_strict_errors_cause_summary.csv", index=False, encoding="utf-8")
        by_ticker.to_csv(GRANULAR_DIR / "quotes_agent_strict_summary_by_ticker.csv", index=False, encoding="utf-8")
        parsed.to_parquet(GRANULAR_DIR / "quotes_agent_strict_events_history.parquet", index=False)

        # Retry queue (strict) - current snapshot
        rq = parsed.copy()
        if RETRY_POLICY == "hard_and_soft":
            rq = rq[rq["severity"].isin(["HARD_FAIL", "SOFT_FAIL"])].copy()
            if not rq.empty:
                soft_closeable = rq.apply(_is_soft_closeable_row, axis=1)
                rq = rq[~soft_closeable].copy()
        else:
            rq = rq[rq["severity"] == "HARD_FAIL"].copy()

        rq_cols = [c for c in ["file", "severity", "issues", "warns", "action", "processed_at_utc"] if c in rq.columns]
        rq = rq[rq_cols].drop_duplicates(subset=["file"], keep="last")

        # Retry attempts + frozen queue
        if RETRY_ATTEMPTS_CSV.exists():
            try:
                attempts = pd.read_csv(RETRY_ATTEMPTS_CSV)
            except Exception:
                attempts = pd.DataFrame(columns=["file", "attempts", "last_processed_at_utc", "last_severity", "last_run_id", "status"])
        else:
            attempts = pd.DataFrame(columns=["file", "attempts", "last_processed_at_utc", "last_severity", "last_run_id", "status"])

        attempts = attempts.drop_duplicates(subset=["file"], keep="last") if not attempts.empty else attempts
        attempts_map = {r["file"]: r for _, r in attempts.iterrows()} if not attempts.empty else {}

        retry_rows = []
        for _, r in rq.iterrows():
            f = r.get("file")
            prev = attempts_map.get(f, {})
            prev_ts = prev.get("last_processed_at_utc")
            curr_ts = r.get("processed_at_utc")
            attempts_n = int(prev.get("attempts", 0))
            if str(prev_ts) != str(curr_ts):
                attempts_n += 1
            retry_rows.append({
                "file": f,
                "attempts": attempts_n,
                "last_processed_at_utc": curr_ts,
                "last_severity": r.get("severity"),
                "last_run_id": RUN_ID,
                "status": "frozen" if attempts_n > MAX_RETRY_ATTEMPTS else "active",
            })

        attempts_new = pd.DataFrame(retry_rows, columns=["file", "attempts", "last_processed_at_utc", "last_severity", "last_run_id", "status"])
        active_files = set(attempts_new.loc[attempts_new["status"] == "active", "file"].tolist()) if not attempts_new.empty else set()
        frozen_files = set(attempts_new.loc[attempts_new["status"] == "frozen", "file"].tolist()) if not attempts_new.empty else set()

        rq_current = rq[rq["file"].isin(active_files)].copy() if not rq.empty else rq.copy()
        rq_frozen = rq[rq["file"].isin(frozen_files)].copy() if not rq.empty else rq.copy()

        RETRY_QUEUE_CSV.parent.mkdir(parents=True, exist_ok=True)
        rq.to_csv(RETRY_QUEUE_CSV, index=False, encoding="utf-8")
        rq.to_parquet(RETRY_QUEUE_PARQUET, index=False)
        rq_current.to_csv(RETRY_CURRENT_CSV, index=False, encoding="utf-8")
        rq_frozen.to_csv(RETRY_FROZEN_CSV, index=False, encoding="utf-8")
        attempts_new.to_csv(RETRY_ATTEMPTS_CSV, index=False, encoding="utf-8")

        print("\nSaved strict retry queue:")
        print(RETRY_QUEUE_CSV)
        print(RETRY_QUEUE_PARQUET)
        print(RETRY_CURRENT_CSV)
        print(RETRY_ATTEMPTS_CSV)
        print(RETRY_FROZEN_CSV)

        print("\nSaved strict granular outputs:")
        print(GRANULAR_DIR / "quotes_agent_strict_errors_granular.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_errors_causes_granular.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_errors_cause_summary.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_summary_by_ticker.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_events_history.parquet")
        print(EVENTS_CURRENT_CSV)

    # Live status snapshot (ligero) para monitor instantaneo
    try:
        severity_counts = {}
        if not current.empty and "severity" in current.columns:
            _sev = current.groupby("severity", dropna=False).size().reset_index(name="count")
            for _, rr in _sev.iterrows():
                severity_counts[str(rr["severity"])] = int(rr["count"])

        retry_pending_count = 0
        if RETRY_CURRENT_CSV.exists():
            try:
                _rq = pd.read_csv(RETRY_CURRENT_CSV)
                retry_pending_count = int(_rq["file"].nunique()) if ("file" in _rq.columns and not _rq.empty) else int(len(_rq))
            except Exception:
                retry_pending_count = 0

        top_causes = []
        if not current.empty:
            cause_rows = []
            for _, rr in current.iterrows():
                for c in _safe_parse_listlike(rr.get("issues")):
                    cause_rows.append({"cause": c, "count": 1})
                for c in _safe_parse_listlike(rr.get("warns")):
                    cause_rows.append({"cause": c, "count": 1})
            if cause_rows:
                _c = (
                    pd.DataFrame(cause_rows)
                    .groupby("cause", dropna=False)["count"]
                    .sum()
                    .reset_index()
                    .sort_values("count", ascending=False)
                    .head(10)
                )
                top_causes = [{"cause": str(r["cause"]), "count": int(r["count"])} for _, r in _c.iterrows()]

        live = {
            "run_id": RUN_ID,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "probe_root": str(PROBE_ROOT),
            "max_files": int(MAX_FILES),
            "files_discovered_total": int(len(all_files)) if "all_files" in locals() else None,
            "files_pending": int(len(pending)) if "pending" in locals() else None,
            "files_processed_total_state": int(state.get("total_processed", 0)) if isinstance(state, dict) else None,
            "files_current_snapshot": int(current["file"].nunique()) if (not current.empty and "file" in current.columns) else int(len(current)),
            "severity_counts_current": severity_counts,
            "retry_pending_files_current": int(retry_pending_count),
            "top_causes_current": top_causes,
            "events_current_csv": str(EVENTS_CURRENT_CSV),
            "retry_current_csv": str(RETRY_CURRENT_CSV),
            "state_file": str(STATE_FILE),
            "batch_manifest_csv": str(BATCH_MANIFEST_CSV),
        }
        LIVE_STATUS_JSON.parent.mkdir(parents=True, exist_ok=True)
        LIVE_STATUS_JSON.write_text(json.dumps(live, indent=2, ensure_ascii=False), encoding="utf-8")
        print("\nSaved live status snapshot:")
        print(LIVE_STATUS_JSON)
    except Exception as ex:
        print(f"\nWARNING: no se pudo guardar LIVE_STATUS_JSON: {ex}")
