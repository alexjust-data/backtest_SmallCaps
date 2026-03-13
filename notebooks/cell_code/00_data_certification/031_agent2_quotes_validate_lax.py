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
MAX_FILES = int(globals().get("MAX_FILES", 5))
RESET_STATE = bool(globals().get("RESET_STATE", False))
RESCAN_ALL = bool(globals().get("RESCAN_ALL", False))
STATE_FILE = Path(
    globals().get(
        "STATE_FILE",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_state.json",
    )
)
EVENTS_CSV = Path(
    globals().get(
        "EVENTS_CSV",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_events.csv",
    )
)
GRANULAR_DIR = Path(
    globals().get(
        "GRANULAR_DIR",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\granular",
    )
)
RETRY_POLICY = globals().get("RETRY_POLICY", "hard_only")  # hard_only | hard_and_warn
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_lax.csv"))
RETRY_QUEUE_PARQUET = Path(globals().get("RETRY_QUEUE_PARQUET", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_lax.parquet"))

REQUIRED_COLS = ["timestamp", "bid_price", "ask_price"]

HARD_FAILS = {"parquet_unreadable", "zero_byte_file", "invalid_partition_path"}
EVENT_COLUMNS = [
    "file",
    "rows",
    "severity",
    "issues",
    "warns",
    "action",
    "crossed_rows",
    "negative_price_rows",
    "processed_at_utc",
]

PARTITION_RE = re.compile(r"(?P<ticker>[^\\/]+)[\\/]year=(?P<year>\d{4})[\\/]month=(?P<month>\d{2})[\\/]day=(?P<day>\d{2})[\\/]quotes\.parquet$")


def _load_state(path: Path):
    if RESET_STATE:
        return {"processed_files": [], "total_processed": 0, "updated_utc": None}
    if not path.exists():
        return {"processed_files": [], "total_processed": 0, "updated_utc": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if "processed_files" not in data:
            data["processed_files"] = []
        if "total_processed" not in data:
            data["total_processed"] = len(data["processed_files"])
        return data
    except Exception:
        return {"processed_files": [], "total_processed": 0, "updated_utc": None}


def _save_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_utc"] = datetime.now(timezone.utc).isoformat()
    state["total_processed"] = len(state.get("processed_files", []))
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _append_events_csv(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        return

    new_df = df.copy().reindex(columns=EVENT_COLUMNS)
    hist_df = _read_events_csv(path)
    merged = pd.concat([hist_df, new_df], ignore_index=True)
    merged = merged.reindex(columns=EVENT_COLUMNS)
    merged.to_csv(path, index=False, encoding="utf-8")


def _read_events_csv(path: Path):
    if not path.exists():
        return pd.DataFrame(columns=EVENT_COLUMNS)
    try:
        df = pd.read_csv(path)
        return df.reindex(columns=EVENT_COLUMNS)
    except ParserError:
        try:
            # Fallback tolerante para histórico con filas mal formadas
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
            return df.reindex(columns=EVENT_COLUMNS)
        except Exception:
            broken = path.with_suffix(path.suffix + ".broken")
            path.replace(broken)
            print(f"WARNING: malformed events csv moved to: {broken}")
            return pd.DataFrame(columns=EVENT_COLUMNS)
    except Exception:
        return pd.DataFrame(columns=EVENT_COLUMNS)


def _decide_action(severity: str):
    if severity == "HARD_FAIL":
        return "enqueue_retry_and_continue"
    if severity in {"SOFT_FAIL", "PASS_WITH_WARN"}:
        return "warn_and_continue"
    return "count_coverage_and_continue"


def _assess_file(fp: Path):
    issues = []
    warns = []
    crossed_rows = 0
    negative_price_rows = 0

    if fp.stat().st_size == 0:
        issues.append("zero_byte_file")

    rel_ok = False
    try:
        rel = fp.relative_to(EXPECTED_QUOTES_ROOT)
        rel_ok = True
    except Exception:
        rel = fp

    m = PARTITION_RE.search(str(rel if rel_ok else fp))
    if not m:
        issues.append("invalid_partition_path")

    rows = None
    cols = []
    missing_cols = []

    try:
        table = pq.read_table(fp)
        rows = table.num_rows
        cols = list(table.schema.names)
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
            "negative_price_rows": negative_price_rows,
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    if rows is not None and rows < 1:
        issues.append("zero_rows")

    for c in REQUIRED_COLS:
        if c not in cols:
            missing_cols.append(c)
    if missing_cols:
        warns.append(f"missing_noncritical_columns:{','.join(missing_cols)}")

    # Soft rules (no bloqueantes)
    try:
        df = table.select([c for c in ["bid_price", "ask_price"] if c in cols]).to_pandas()
        if {"bid_price", "ask_price"}.issubset(df.columns):
            crossed_rows = int((df["bid_price"] > df["ask_price"]).sum())
            if crossed_rows > 0:
                warns.append(f"market_anomaly_rows:bid_gt_ask={crossed_rows}")
            negative_price_rows = int(((df["bid_price"] < 0) | (df["ask_price"] < 0)).sum())
            if negative_price_rows > 0:
                warns.append(f"market_anomaly_rows:negative_prices={negative_price_rows}")
    except Exception:
        warns.append("soft_rule_eval_error")

    if any(i in HARD_FAILS for i in issues):
        severity = "HARD_FAIL"
    elif issues or warns:
        severity = "PASS_WITH_WARN"
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
        "negative_price_rows": negative_price_rows,
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


print(f"EXPECTED_QUOTES_ROOT: {EXPECTED_QUOTES_ROOT}")
print(f"PROBE_ROOT: {PROBE_ROOT}")
print(f"STATE_FILE: {STATE_FILE}")
print(f"EVENTS_CSV: {EVENTS_CSV}")
print(f"GRANULAR_DIR: {GRANULAR_DIR}")
print(f"MAX_FILES: {MAX_FILES} | RESET_STATE={RESET_STATE} | RESCAN_ALL={RESCAN_ALL}")

if not PROBE_ROOT.exists():
    print("PROBE_ROOT not found")
else:
    state = _load_state(STATE_FILE)
    processed_set = set(state.get("processed_files", []))

    all_files = sorted(str(p) for p in PROBE_ROOT.rglob("quotes.parquet"))
    if RESCAN_ALL:
        pending = all_files
    else:
        pending = [f for f in all_files if f not in processed_set]
    files = [Path(f) for f in pending[:MAX_FILES]]

    print(f"files_discovered_total: {len(all_files)}")
    print(f"files_already_processed: {len(processed_set)}")
    print(f"files_pending: {len(pending)}")
    print(f"files_to_process_now: {len(files)}")

    results = [_assess_file(fp) for fp in files]
    out_df = pd.DataFrame(results)

    if not out_df.empty:
        _append_events_csv(EVENTS_CSV, out_df)
        state["processed_files"] = list(processed_set.union(set(out_df["file"].tolist())))
        _save_state(STATE_FILE, state)

    try:
        display(out_df)
    except Exception:
        print(out_df.to_string(index=False))

    print("\nResumen por severidad:")
    if out_df.empty:
        print("No new files processed in this run.")
    else:
        try:
            display(out_df.groupby("severity", dropna=False).size().reset_index(name="count"))
        except Exception:
            print(out_df.groupby("severity", dropna=False).size())

    if EVENTS_CSV.exists():
        hist = _read_events_csv(EVENTS_CSV)
        print("\nResumen historico acumulado:")
        try:
            display(hist.groupby("severity", dropna=False).size().reset_index(name="count"))
        except Exception:
            print(hist.groupby("severity", dropna=False).size())

        # ===== Analisis granular que deja el propio agente =====
        # Parse de particiones desde path para detalle por ticker/fecha
        parsed = hist.copy()
        ext = parsed["file"].str.extract(
            r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
        )
        parsed = pd.concat([parsed, ext], axis=1)
        if "crossed_rows" not in parsed.columns:
            parsed["crossed_rows"] = (
                parsed["warns"].astype(str).str.extract(r"bid_gt_ask=(\d+)")[0].fillna(0).astype(int)
            )
        if "negative_price_rows" not in parsed.columns:
            parsed["negative_price_rows"] = (
                parsed["warns"].astype(str).str.extract(r"negative_prices=(\d+)")[0].fillna(0).astype(int)
            )
        parsed["crossed_ratio_pct"] = (
            (parsed["crossed_rows"] / parsed["rows"].replace(0, pd.NA)) * 100.0
        ).fillna(0.0)

        print("\nDetalle granular (historico) - PASS_WITH_WARN:")
        warn_df = parsed[parsed["severity"] == "PASS_WITH_WARN"].copy()
        cols = ["ticker", "year", "month", "day", "rows", "crossed_rows", "crossed_ratio_pct", "warns", "file"]
        warn_df = warn_df[cols].sort_values(["ticker", "year", "month", "day", "crossed_rows"], ascending=[True, True, True, True, False])
        try:
            display(warn_df)
        except Exception:
            print(warn_df.to_string(index=False))

        print("\nResumen granular por ticker (historico):")
        by_ticker = (
            parsed.groupby("ticker", dropna=False)
            .agg(
                files_total=("file", "count"),
                pass_count=("severity", lambda s: int((s == "PASS").sum())),
                pass_with_warn_count=("severity", lambda s: int((s == "PASS_WITH_WARN").sum())),
                hard_fail_count=("severity", lambda s: int((s == "HARD_FAIL").sum())),
                crossed_rows_total=("crossed_rows", "sum"),
            )
            .reset_index()
        )
        try:
            display(by_ticker)
        except Exception:
            print(by_ticker.to_string(index=False))

        print("\nResumen granular por causa de warning (historico):")
        by_warn = (
            parsed[parsed["severity"] == "PASS_WITH_WARN"]
            .groupby("warns", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        try:
            display(by_warn)
        except Exception:
            print(by_warn.to_string(index=False))

        # Persistencia de analisis granular para no depender de memoria en notebook
        GRANULAR_DIR.mkdir(parents=True, exist_ok=True)
        warn_df.to_csv(GRANULAR_DIR / "quotes_agent_warn_detail.csv", index=False, encoding="utf-8")
        by_ticker.to_csv(GRANULAR_DIR / "quotes_agent_summary_by_ticker.csv", index=False, encoding="utf-8")
        by_warn.to_csv(GRANULAR_DIR / "quotes_agent_summary_by_warn.csv", index=False, encoding="utf-8")

        # Artefactos de errores granulares (todo lo no PASS)
        errors_df = parsed[parsed["severity"] != "PASS"].copy()
        err_cols = [
            "ticker",
            "year",
            "month",
            "day",
            "rows",
            "severity",
            "issues",
            "warns",
            "crossed_rows",
            "negative_price_rows",
            "crossed_ratio_pct",
            "action",
            "processed_at_utc",
            "file",
        ]
        errors_df = errors_df[[c for c in err_cols if c in errors_df.columns]].sort_values(
            ["ticker", "year", "month", "day", "severity"], ascending=[True, True, True, True, True]
        )

        cause_rows = []
        for _, r in errors_df.iterrows():
            issues = r.get("issues", [])
            warns = r.get("warns", [])
            if not isinstance(issues, list):
                issues = [str(issues)] if pd.notna(issues) and str(issues) else []
            if not isinstance(warns, list):
                warns = [str(warns)] if pd.notna(warns) and str(warns) else []
            for it in issues:
                cause_rows.append(
                    {
                        "file": r.get("file"),
                        "ticker": r.get("ticker"),
                        "year": r.get("year"),
                        "month": r.get("month"),
                        "day": r.get("day"),
                        "severity": r.get("severity"),
                        "cause_type": "issue",
                        "cause": it,
                        "processed_at_utc": r.get("processed_at_utc"),
                    }
                )
            for wt in warns:
                cause_rows.append(
                    {
                        "file": r.get("file"),
                        "ticker": r.get("ticker"),
                        "year": r.get("year"),
                        "month": r.get("month"),
                        "day": r.get("day"),
                        "severity": r.get("severity"),
                        "cause_type": "warn",
                        "cause": wt,
                        "processed_at_utc": r.get("processed_at_utc"),
                    }
                )

        causes_df = pd.DataFrame(cause_rows)
        if not causes_df.empty:
            cause_summary_df = (
                causes_df.groupby(["cause_type", "cause"], dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
        else:
            cause_summary_df = pd.DataFrame(columns=["cause_type", "cause", "count"])

        errors_df.to_csv(GRANULAR_DIR / "quotes_agent_errors_granular.csv", index=False, encoding="utf-8")
        causes_df.to_csv(GRANULAR_DIR / "quotes_agent_errors_causes_granular.csv", index=False, encoding="utf-8")
        cause_summary_df.to_csv(GRANULAR_DIR / "quotes_agent_errors_cause_summary.csv", index=False, encoding="utf-8")
        parsed.to_parquet(GRANULAR_DIR / "quotes_agent_events_history.parquet", index=False)

        # Retry queue (lax)
        rq = parsed.copy()
        if RETRY_POLICY == "hard_and_warn":
            rq = rq[rq["severity"].isin(["HARD_FAIL", "PASS_WITH_WARN"])].copy()
        else:
            rq = rq[rq["severity"] == "HARD_FAIL"].copy()

        rq_cols = [c for c in ["file", "severity", "issues", "warns", "action", "processed_at_utc"] if c in rq.columns]
        rq = rq[rq_cols].drop_duplicates(subset=["file"], keep="last")
        RETRY_QUEUE_CSV.parent.mkdir(parents=True, exist_ok=True)
        rq.to_csv(RETRY_QUEUE_CSV, index=False, encoding="utf-8")
        rq.to_parquet(RETRY_QUEUE_PARQUET, index=False)

        print("\nSaved retry queue:")
        print(RETRY_QUEUE_CSV)
        print(RETRY_QUEUE_PARQUET)

        print("\nSaved granular outputs:")
        print(GRANULAR_DIR / "quotes_agent_warn_detail.csv")
        print(GRANULAR_DIR / "quotes_agent_summary_by_ticker.csv")
        print(GRANULAR_DIR / "quotes_agent_summary_by_warn.csv")
        print(GRANULAR_DIR / "quotes_agent_errors_granular.csv")
        print(GRANULAR_DIR / "quotes_agent_errors_causes_granular.csv")
        print(GRANULAR_DIR / "quotes_agent_errors_cause_summary.csv")
        print(GRANULAR_DIR / "quotes_agent_events_history.parquet")
