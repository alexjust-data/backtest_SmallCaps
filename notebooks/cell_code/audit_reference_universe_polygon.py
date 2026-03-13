from pathlib import Path
import json
import pandas as pd


REFERENCE_ROOT = Path(globals().get("REFERENCE_ROOT", r"D:\reference"))
RUN_DIR = REFERENCE_ROOT / "_run"
AUDIT_CSV = RUN_DIR / "download_reference_universe_polygon.audit.csv"
ERRORS_CSV = RUN_DIR / "download_reference_universe_polygon.errors.csv"
PROGRESS_JSON = RUN_DIR / "download_reference_universe_polygon.progress.json"
TOP_N = int(globals().get("TOP_N", 30))
SHOW_HEAD_TAIL = bool(globals().get("SHOW_HEAD_TAIL", True))


def _safe_count_dirs(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.iterdir() if p.is_dir())


def _safe_count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for p in path.rglob("*") if p.is_file())


if not AUDIT_CSV.exists():
    raise FileNotFoundError(AUDIT_CSV)

audit = pd.read_csv(AUDIT_CSV)
errors = pd.read_csv(ERRORS_CSV) if ERRORS_CSV.exists() and ERRORS_CSV.stat().st_size > 0 else pd.DataFrame()
progress = json.loads(PROGRESS_JSON.read_text(encoding="utf-8")) if PROGRESS_JSON.exists() else {}

print("=== PATHS USADOS ===")
print("REFERENCE_ROOT:", REFERENCE_ROOT)
print("RUN_DIR:", RUN_DIR)
print("AUDIT_CSV:", AUDIT_CSV)
print("ERRORS_CSV:", ERRORS_CSV)
print("PROGRESS_JSON:", PROGRESS_JSON)

print("\n=== PROGRESS ===")
print(json.dumps(progress, indent=2, ensure_ascii=False))

if "status" not in audit.columns:
    raise ValueError("Audit CSV sin columna status")

for col in ["dataset", "status", "msg"]:
    if col in audit.columns:
        audit[col] = audit[col].astype("string")

if "http_status" in audit.columns:
    audit["http_status"] = pd.to_numeric(audit["http_status"], errors="coerce")

summary = (
    audit.groupby(["dataset", "status", "http_status", "msg"], dropna=False)
    .size()
    .reset_index(name="count")
    .sort_values(["dataset", "status", "http_status", "msg"], na_position="last")
    .reset_index(drop=True)
)

dataset_counts = (
    audit.groupby("dataset", dropna=False)
    .agg(
        rows=("dataset", "size"),
        ok=("status", lambda s: int((s == "ok").sum())),
        resume_skip=("status", lambda s: int(s.astype("string").str.startswith("resume-skip", na=False).sum())),
        error=("status", lambda s: int((s == "error").sum())),
        distinct_tickers=("ticker", lambda s: int(pd.Series(s).dropna().astype("string").nunique())),
    )
    .reset_index()
)
dataset_counts["ok_pct"] = (100.0 * dataset_counts["ok"] / dataset_counts["rows"]).round(2)
dataset_counts["error_pct"] = (100.0 * dataset_counts["error"] / dataset_counts["rows"]).round(2)

overview_404 = (
    audit[(audit["dataset"] == "overview") & (audit["http_status"] == 404)]
    .loc[:, ["ticker", "request_date", "http_status", "msg", "out_file"]]
    .sort_values(["ticker", "request_date"], na_position="last")
    .reset_index(drop=True)
)

events_200 = (
    audit[(audit["dataset"] == "events") & (audit["http_status"] == 200)]
    .loc[:, ["ticker", "request_date", "http_status", "rows_saved", "msg", "out_file"]]
    .sort_values(["ticker"], na_position="last")
    .reset_index(drop=True)
)

events_404 = (
    audit[(audit["dataset"] == "events") & (audit["http_status"] == 404)]
    .loc[:, ["ticker", "request_date", "http_status", "rows_saved", "msg", "out_file"]]
    .sort_values(["ticker"], na_position="last")
    .reset_index(drop=True)
)

cap_hits = audit[audit["msg"].astype("string").str.contains("hit_page_cap", case=False, na=False)].copy()

storage_inventory = pd.DataFrame(
    [
        {"dataset": "overview", "dir_count": _safe_count_dirs(REFERENCE_ROOT / "overview"), "file_count": _safe_count_files(REFERENCE_ROOT / "overview")},
        {"dataset": "events", "dir_count": _safe_count_dirs(REFERENCE_ROOT / "events"), "file_count": _safe_count_files(REFERENCE_ROOT / "events")},
        {"dataset": "splits", "dir_count": _safe_count_dirs(REFERENCE_ROOT / "splits"), "file_count": _safe_count_files(REFERENCE_ROOT / "splits")},
        {"dataset": "dividends", "dir_count": _safe_count_dirs(REFERENCE_ROOT / "dividends"), "file_count": _safe_count_files(REFERENCE_ROOT / "dividends")},
        {"dataset": "all_tickers", "dir_count": 0, "file_count": _safe_count_files(REFERENCE_ROOT / "all_tickers")},
        {"dataset": "ticker_types", "dir_count": 0, "file_count": _safe_count_files(REFERENCE_ROOT / "ticker_types")},
        {"dataset": "exchanges", "dir_count": 0, "file_count": _safe_count_files(REFERENCE_ROOT / "exchanges")},
    ]
)

print("\n=== SUMMARY POR DATASET/STATUS ===")
display(summary)

print("\n=== COUNTS POR DATASET ===")
display(dataset_counts)

print("\n=== INVENTARIO DE STORAGE ===")
display(storage_inventory)

print("\n=== OVERVIEW 404 ===")
display(overview_404.head(TOP_N))

print("\n=== EVENTS 200 (muestra) ===")
display(events_200.head(TOP_N))

print("\n=== EVENTS 404 / NO_EVENTS (muestra) ===")
display(events_404.head(TOP_N))

print("\n=== PAGE CAP HITS ===")
display(cap_hits.head(TOP_N))

if not errors.empty:
    print("\n=== ERRORS CSV ===")
    display(errors.head(TOP_N))

if SHOW_HEAD_TAIL:
    print("\n=== AUDIT HEAD(5) ===")
    display(audit.head(5))
    print("\n=== AUDIT TAIL(5) ===")
    display(audit.tail(5))
