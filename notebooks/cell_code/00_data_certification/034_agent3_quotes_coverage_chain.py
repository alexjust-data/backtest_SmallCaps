from pathlib import Path
import re
import json
import pandas as pd
import numpy as np

# =========================
# Config (puedes sobreescribir desde la celda)
# =========================
QUOTES_ROOT = Path(globals().get("QUOTES_ROOT", r"C:\TSIS_Data\data\quotes_p95"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_strict_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\retry_queue_quotes_strict_current.csv"))
OFFICIAL_LIFECYCLE_CSV = Path(globals().get("OFFICIAL_LIFECYCLE_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\official_lifecycle_compiled.csv"))

OUT_DIR = Path(globals().get("OUT_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\agent03_outputs"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_FROM = globals().get("DATE_FROM", None)  # ej "2019-01-01"
DATE_TO = globals().get("DATE_TO", None)      # ej "2025-12-31"
MAX_TICKERS = globals().get("MAX_TICKERS", None)
MAX_TICKERS = int(MAX_TICKERS) if MAX_TICKERS not in (None, "") else None

COVERAGE_OK_STATUSES = set(globals().get("COVERAGE_OK_STATUSES", ["PASS", "SOFT_FAIL"]))


def _safe_read_csv(path: Path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")


def _parse_file_to_day(series: pd.Series):
    pat = re.compile(r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$")
    ext = series.astype(str).str.extract(pat)
    ext["date"] = pd.to_datetime(ext["year"] + "-" + ext["month"] + "-" + ext["day"], errors="coerce")
    return ext


def _build_expected_window(events_df: pd.DataFrame, lifecycle_csv: Path):
    obs = events_df.groupby("ticker", dropna=False).agg(obs_min=("date", "min"), obs_max=("date", "max")).reset_index()

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
    life2["list_date"] = pd.to_datetime(life2["list_date"], errors="coerce")
    if "delist_date" in life2.columns:
        life2["delist_date"] = pd.to_datetime(life2["delist_date"], errors="coerce")
    else:
        life2["delist_date"] = pd.NaT
    life2["delist_date"] = life2["delist_date"].fillna(pd.Timestamp.today().normalize())

    expected = obs.merge(life2[["ticker", "list_date", "delist_date"]], on="ticker", how="left")
    expected["exp_min"] = expected["list_date"].fillna(expected["obs_min"])
    expected["exp_max"] = expected["delist_date"].fillna(expected["obs_max"])

    # acotar por observado para evitar ventanas absurdas
    expected["exp_min"] = expected[["exp_min", "obs_min"]].max(axis=1)
    expected["exp_max"] = expected[["exp_max", "obs_max"]].min(axis=1)

    return expected[["ticker", "exp_min", "exp_max", "obs_min", "obs_max"]]


# =========================
# B) Leer eventos de agente 02 y deduplicar
# =========================
events = _safe_read_csv(EVENTS_CSV)
if events.empty:
    raise RuntimeError(f"No events found: {EVENTS_CSV}")

if "processed_at_utc" in events.columns:
    events["processed_at_utc"] = pd.to_datetime(events["processed_at_utc"], errors="coerce")
    events = events.sort_values("processed_at_utc")

if "file" in events.columns:
    events = events.drop_duplicates(subset=["file"], keep="last")

events = events.reset_index(drop=True)
parts = _parse_file_to_day(events["file"])
events = pd.concat([events, parts], axis=1)

if DATE_FROM is not None:
    events = events[events["date"] >= pd.to_datetime(DATE_FROM)]
if DATE_TO is not None:
    events = events[events["date"] <= pd.to_datetime(DATE_TO)]

if MAX_TICKERS is not None:
    keep = events["ticker"].dropna().unique()[:MAX_TICKERS]
    events = events[events["ticker"].isin(keep)]

# =========================
# C) Estado por día con retry pending
# =========================
retry_df = _safe_read_csv(RETRY_QUEUE_CSV)
retry_files = set(retry_df["file"].dropna().tolist()) if (not retry_df.empty and "file" in retry_df.columns) else set()

events["retry_pending"] = events["file"].isin(retry_files)
events["day_status"] = np.where(events["retry_pending"], "RETRY_PENDING", events["severity"])

# =========================
# D) Ventana esperada y cobertura
# =========================
expected = _build_expected_window(events, OFFICIAL_LIFECYCLE_CSV)

present = events.groupby("ticker", dropna=False)["date"].nunique().rename("present_days").reset_index()
present_ok = (
    events[events["day_status"].isin(COVERAGE_OK_STATUSES)]
    .groupby("ticker", dropna=False)["date"]
    .nunique()
    .rename("present_ok_days")
    .reset_index()
)

cov = expected.merge(present, on="ticker", how="left").merge(present_ok, on="ticker", how="left")
cov["present_days"] = cov["present_days"].fillna(0).astype(int)
cov["present_ok_days"] = cov["present_ok_days"].fillna(0).astype(int)
cov["expected_days"] = (cov["exp_max"] - cov["exp_min"]).dt.days.add(1).clip(lower=0).fillna(0).astype(int)
cov["missing_days_ok"] = (cov["expected_days"] - cov["present_ok_days"]).clip(lower=0)
cov["coverage_ratio_ok"] = np.where(cov["expected_days"] > 0, cov["present_ok_days"] / cov["expected_days"], np.nan)

coverage_by_ticker = cov[[
    "ticker", "exp_min", "exp_max", "expected_days", "present_days", "present_ok_days", "missing_days_ok", "coverage_ratio_ok"
]].copy()

quality_summary_by_ticker = (
    events.groupby("ticker", dropna=False)["day_status"]
    .value_counts(dropna=False)
    .unstack(fill_value=0)
    .reset_index()
)
quality_summary_by_ticker = quality_summary_by_ticker.merge(
    coverage_by_ticker[["ticker", "coverage_ratio_ok", "missing_days_ok"]],
    on="ticker",
    how="left",
)

# missing days explicito por ticker
missing_rows = []
for _, r in coverage_by_ticker.iterrows():
    t = r["ticker"]
    if pd.isna(r["exp_min"]) or pd.isna(r["exp_max"]):
        continue
    full = pd.date_range(r["exp_min"], r["exp_max"], freq="D")
    seen = set(
        events.loc[
            (events["ticker"] == t) & (events["day_status"].isin(COVERAGE_OK_STATUSES)),
            "date",
        ].dropna().dt.normalize()
    )
    for d in full:
        if d.normalize() not in seen:
            missing_rows.append({"ticker": t, "date": d})

missing_days_by_ticker = pd.DataFrame(missing_rows)

# =========================
# F) Gate de cadena
# =========================
retry_pending_files = int(events["retry_pending"].sum())
hard_fails = int((events["day_status"] == "HARD_FAIL").sum())

gate_status = "NO_CLOSE_RETRY_PENDING" if retry_pending_files > 0 else "CLOSED_OK"
if retry_pending_files == 0 and hard_fails > 0:
    gate_status = "NO_CLOSE_HARD_FAIL_PRESENT"

run_summary = {
    "events_rows_dedup": int(len(events)),
    "tickers": int(events["ticker"].nunique(dropna=True)),
    "retry_pending_files": retry_pending_files,
    "hard_fail_files": hard_fails,
    "mean_coverage_ok": float(coverage_by_ticker["coverage_ratio_ok"].dropna().mean()) if len(coverage_by_ticker) else None,
    "gate_status": gate_status,
    "out_dir": str(OUT_DIR),
}

# =========================
# Salidas
# =========================
coverage_by_ticker.to_csv(OUT_DIR / "coverage_by_ticker.csv", index=False)
coverage_by_ticker.to_parquet(OUT_DIR / "coverage_by_ticker.parquet", index=False)

missing_days_by_ticker.to_csv(OUT_DIR / "missing_days_by_ticker.csv", index=False)
missing_days_by_ticker.to_parquet(OUT_DIR / "missing_days_by_ticker.parquet", index=False)

quality_summary_by_ticker.to_csv(OUT_DIR / "quality_summary_by_ticker.csv", index=False)
quality_summary_by_ticker.to_parquet(OUT_DIR / "quality_summary_by_ticker.parquet", index=False)

(OUT_DIR / "run_summary.json").write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("Run summary:")
print(json.dumps(run_summary, indent=2, ensure_ascii=False))

print("\nTop 10 menor cobertura:")
try:
    display(coverage_by_ticker.sort_values("coverage_ratio_ok", ascending=True).head(10))
except Exception:
    print(coverage_by_ticker.sort_values("coverage_ratio_ok", ascending=True).head(10).to_string(index=False))

print("\nTop 10 calidad (fails/retry):")
sort_cols = [c for c in ["RETRY_PENDING", "HARD_FAIL", "SOFT_FAIL", "PASS_WITH_WARN"] if c in quality_summary_by_ticker.columns]
if sort_cols:
    qv = quality_summary_by_ticker.sort_values(sort_cols, ascending=False).head(10)
else:
    qv = quality_summary_by_ticker.head(10)
try:
    display(qv)
except Exception:
    print(qv.to_string(index=False))
