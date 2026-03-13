from pathlib import Path
import re
import json
from datetime import datetime, timezone
import pandas as pd
import pyarrow.parquet as pq
from pandas.errors import ParserError

# ===== Configurable desde celda =====
EXPECTED_QUOTES_ROOT = Path(globals().get("EXPECTED_QUOTES_ROOT", r"C:\TSIS_Data\data\quotes_p95"))
PROBE_ROOT = Path(globals().get("PROBE_ROOT", r"C:\TSIS_Data\data\quotes_p95"))
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

# Umbral estricto para mercado cruzado
MAX_CROSSED_RATIO_PCT = float(globals().get("MAX_CROSSED_RATIO_PCT", 0.01))

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
    "processed_at_utc",
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
        return {"processed_files": [], "total_processed": 0, "updated_utc": None}
    if not path.exists():
        return {"processed_files": [], "total_processed": 0, "updated_utc": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        data.setdefault("processed_files", [])
        data.setdefault("total_processed", len(data["processed_files"]))
        return data
    except Exception:
        return {"processed_files": [], "total_processed": 0, "updated_utc": None}


def _save_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_utc"] = datetime.now(timezone.utc).isoformat()
    state["total_processed"] = len(state.get("processed_files", []))
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _decide_action(severity: str):
    if severity == "HARD_FAIL":
        return "quarantine_and_retry"
    if severity == "SOFT_FAIL":
        return "warn_and_retry_later"
    return "count_coverage"


def _assess_file(fp: Path):
    issues = []
    warns = []
    crossed_rows = 0
    crossed_ratio_pct = 0.0
    negative_price_rows = 0
    missing_required_cols = []
    dtype_mismatches = []

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
        table = pq.read_table(fp)
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
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
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
            crossed_rows = int((df["bid_price"] > df["ask_price"]).sum())
            negative_price_rows = int(((df["bid_price"] < 0) | (df["ask_price"] < 0)).sum())
            crossed_ratio_pct = float((crossed_rows / max(int(rows or 0), 1)) * 100.0)

            if negative_price_rows > 0:
                issues.append("negative_prices_any_row")

            if crossed_rows > 0:
                if crossed_ratio_pct > MAX_CROSSED_RATIO_PCT:
                    issues.append("crossed_ratio_gt_threshold")
                else:
                    warns.append("crossed_rows_present_but_under_threshold")
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
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


print(f"EXPECTED_QUOTES_ROOT: {EXPECTED_QUOTES_ROOT}")
print(f"PROBE_ROOT: {PROBE_ROOT}")
print(f"STATE_FILE: {STATE_FILE}")
print(f"EVENTS_CSV: {EVENTS_CSV}")
print(f"GRANULAR_DIR: {GRANULAR_DIR}")
print(f"MAX_FILES: {MAX_FILES} | RESET_STATE={RESET_STATE} | RESCAN_ALL={RESCAN_ALL}")
print(f"MAX_CROSSED_RATIO_PCT: {MAX_CROSSED_RATIO_PCT}")
print("\nDiccionario de reglas (estricto):")
print(json.dumps(RULES_DICTIONARY_STRICT, indent=2, ensure_ascii=False))

if not PROBE_ROOT.exists():
    print("PROBE_ROOT not found")
else:
    state = _load_state(STATE_FILE)
    processed_set = set(state.get("processed_files", []))

    all_files = sorted(str(p) for p in PROBE_ROOT.rglob("quotes.parquet"))
    pending = all_files if RESCAN_ALL else [f for f in all_files if f not in processed_set]
    files = [Path(f) for f in pending[:MAX_FILES]]

    print(f"files_discovered_total: {len(all_files)}")
    print(f"files_already_processed: {len(processed_set)}")
    print(f"files_pending: {len(pending)}")
    print(f"files_to_process_now: {len(files)}")

    out_df = pd.DataFrame([_assess_file(fp) for fp in files])

    if not out_df.empty:
        _append_events_csv(EVENTS_CSV, out_df)
        state["processed_files"] = list(processed_set.union(set(out_df["file"].tolist())))
        _save_state(STATE_FILE, state)

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

    hist = _read_events_csv(EVENTS_CSV)
    print("\nResumen historico acumulado:")
    try:
        display(hist.groupby("severity", dropna=False).size().reset_index(name="count"))
    except Exception:
        print(hist.groupby("severity", dropna=False).size())

    if not hist.empty:
        parsed = hist.copy()
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

        # Retry queue (strict)
        rq = parsed.copy()
        if RETRY_POLICY == "hard_and_soft":
            rq = rq[rq["severity"].isin(["HARD_FAIL", "SOFT_FAIL"])].copy()
        else:
            rq = rq[rq["severity"] == "HARD_FAIL"].copy()

        rq_cols = [c for c in ["file", "severity", "issues", "warns", "action", "processed_at_utc"] if c in rq.columns]
        rq = rq[rq_cols].drop_duplicates(subset=["file"], keep="last")
        RETRY_QUEUE_CSV.parent.mkdir(parents=True, exist_ok=True)
        rq.to_csv(RETRY_QUEUE_CSV, index=False, encoding="utf-8")
        rq.to_parquet(RETRY_QUEUE_PARQUET, index=False)

        print("\nSaved strict retry queue:")
        print(RETRY_QUEUE_CSV)
        print(RETRY_QUEUE_PARQUET)

        print("\nSaved strict granular outputs:")
        print(GRANULAR_DIR / "quotes_agent_strict_errors_granular.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_errors_causes_granular.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_errors_cause_summary.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_summary_by_ticker.csv")
        print(GRANULAR_DIR / "quotes_agent_strict_events_history.parquet")
