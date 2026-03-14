from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

try:
    import ipywidgets as widgets
    from IPython.display import display, clear_output
except Exception:
    widgets = None
    display = None
    clear_output = None

RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EXPECTED_CSV = Path(globals().get("EXPECTED_CSV", RUN_DIR / "inputs" / "tasks_trades_ticks.csv"))
TRADES_ROOT = Path(globals().get("TRADES_ROOT", r"D:\trades_ticks"))
OHLCV_1M_ROOT = Path(globals().get("OHLCV_1M_ROOT", r"D:\ohlcv_1m"))
DAILY_ROOT = Path(globals().get("DAILY_ROOT", r"D:\ohlcv_daily"))
OUT_DIR = Path(globals().get("OUT_DIR", RUN_DIR / "agent03_trades_ticks_outputs"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
TOP_N = int(globals().get("TOP_N", 20))
CONTEXT_DAYS = int(globals().get("CONTEXT_DAYS", 20))
MAX_PLOT_POINTS = int(globals().get("MAX_PLOT_POINTS", 50000))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 4)))

DOWNLOAD_EVENTS_CSV = RUN_DIR / "download_events_trades_ticks_current.csv"
VALIDATION_EVENTS_CSV = RUN_DIR / "trades_ticks_agent_events_current.csv"
EXPECTED_VS_FOUND_CSV = RUN_DIR / "expected_vs_found_trades_ticks.csv"
RUN_SUMMARY_JSON = OUT_DIR / "run_summary.json"

STATUS_ORDER = [
    "PASS",
    "SOFT_FAIL",
    "HARD_FAIL",
    "DOWNLOADED_EMPTY",
    "DOWNLOAD_FAIL",
    "EXPECTED_MISSING",
]
STATUS_COLOR = {
    "PASS": "#4c956c",
    "SOFT_FAIL": "#f4a259",
    "HARD_FAIL": "#bc4749",
    "DOWNLOADED_EMPTY": "#577590",
    "DOWNLOAD_FAIL": "#6d597a",
    "EXPECTED_MISSING": "#9e9e9e",
}


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")


def _parse_listlike(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan", "None"):
        return []
    try:
        x = ast.literal_eval(s)
        if isinstance(x, list):
            return [str(i) for i in x]
        return [str(x)]
    except Exception:
        return [s]


def _load_daily_context(ticker: str, center_date: pd.Timestamp) -> pd.DataFrame:
    years = sorted({center_date.year - 1, center_date.year, center_date.year + 1})
    parts = []
    for year in years:
        fp = DAILY_ROOT / f"ticker={ticker}" / f"year={year:04d}" / f"day_aggs_{ticker}_{year:04d}.parquet"
        if not fp.exists():
            continue
        try:
            pf = pq.ParquetFile(fp)
            df = pf.read().to_pandas()
            parts.append(df)
        except Exception:
            continue
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out


def _load_ohlcv_1m_day(ticker: str, day: pd.Timestamp) -> pd.DataFrame:
    fp = OHLCV_1M_ROOT / f"ticker={ticker}" / f"year={day.year:04d}" / f"month={day.month:02d}" / f"minute_aggs_{ticker}_{day.year:04d}_{day.month:02d}.parquet"
    if not fp.exists():
        return pd.DataFrame()
    try:
        pf = pq.ParquetFile(fp)
        cols = [c for c in ["ticker", "ts_utc", "date", "o", "h", "l", "c", "v", "vw", "n"] if c in pf.schema.names]
        df = pf.read(columns=cols).to_pandas()
    except Exception:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"] == day.normalize()].copy()
    if df.empty:
        return df
    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    return df.sort_values("ts_utc").reset_index(drop=True)


def _load_intraday_trades(path: Path) -> pd.DataFrame:
    pf = pq.ParquetFile(path)
    cols = [c for c in ["ticker", "date", "timestamp", "price", "size", "exchange"] if c in pf.schema.names]
    df = pf.read(columns=cols).to_pandas()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def _normalize() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    expected = _safe_read_csv(EXPECTED_CSV)
    if expected.empty:
        raise RuntimeError(f"No expected tasks CSV: {EXPECTED_CSV}")
    expected["ticker"] = expected["ticker"].astype(str).str.upper().str.strip()
    expected["date"] = expected["date"].astype(str).str.strip()
    expected["task_key"] = expected.get("task_key", expected["ticker"] + "|" + expected["date"] + "|market")

    download = _safe_read_csv(DOWNLOAD_EVENTS_CSV)
    if not download.empty:
        download["ticker"] = download["ticker"].astype(str).str.upper().str.strip()
        download["date"] = download["date"].astype(str).str.strip()
        download["task_key"] = download.get("task_key", download["ticker"] + "|" + download["date"] + "|" + download.get("session", pd.Series(["market"] * len(download))).astype(str))
        if "processed_at_utc" in download.columns:
            download["processed_at_utc"] = pd.to_datetime(download["processed_at_utc"], errors="coerce")
            download = download.sort_values("processed_at_utc").drop_duplicates("task_key", keep="last")

    valid = _safe_read_csv(VALIDATION_EVENTS_CSV)
    if not valid.empty:
        valid["ticker"] = valid["ticker"].astype(str).str.upper().str.strip()
        valid["date"] = valid["date"].astype(str).str.strip()
        valid["task_key"] = valid.get("task_key", valid["ticker"] + "|" + valid["date"] + "|" + valid.get("session", pd.Series(["market"] * len(valid))).astype(str))
        if "processed_at_utc" in valid.columns:
            valid["processed_at_utc"] = pd.to_datetime(valid["processed_at_utc"], errors="coerce")
            valid = valid.sort_values("processed_at_utc").drop_duplicates("task_key", keep="last")

    evf = _safe_read_csv(EXPECTED_VS_FOUND_CSV)
    return expected, download, valid, evf


def _build_task_status(expected: pd.DataFrame, download: pd.DataFrame, valid: pd.DataFrame) -> pd.DataFrame:
    base = expected[[c for c in ["task_key", "ticker", "date", "session", "expected_file"] if c in expected.columns]].copy()
    if "session" not in base.columns:
        base["session"] = "market"

    dkeep = [c for c in ["task_key", "status", "rows", "file", "error", "processed_at_utc"] if c in download.columns]
    vkeep = [c for c in ["task_key", "severity", "rows", "file", "issues", "warns", "duplicate_excess_ratio_pct", "duplicate_group_ratio_pct", "adjacent_exact_repeats", "nonpositive_price_rows", "negative_size_rows", "processed_at_utc"] if c in valid.columns]

    out = base.merge(download[dkeep].rename(columns={"status": "download_status", "rows": "download_rows", "file": "download_file", "processed_at_utc": "download_processed_at_utc"}), on="task_key", how="left")
    out = out.merge(valid[vkeep].rename(columns={"rows": "validated_rows", "file": "validated_file", "processed_at_utc": "validated_processed_at_utc"}), on="task_key", how="left")

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["issues_list"] = out.get("issues", pd.Series([None] * len(out))).apply(_parse_listlike)
    out["warns_list"] = out.get("warns", pd.Series([None] * len(out))).apply(_parse_listlike)
    out["file_path"] = out["validated_file"].fillna(out["download_file"]).fillna(out.get("expected_file"))
    out["file_exists"] = out["file_path"].fillna("").astype(str).apply(lambda s: Path(s).exists() if s else False)

    def final_status(r: pd.Series) -> str:
        sev = str(r.get("severity", ""))
        dst = str(r.get("download_status", ""))
        if sev in ("PASS", "SOFT_FAIL", "HARD_FAIL"):
            return sev
        if dst == "DOWNLOADED_EMPTY":
            return "DOWNLOADED_EMPTY"
        if dst == "DOWNLOAD_FAIL":
            return "DOWNLOAD_FAIL"
        return "EXPECTED_MISSING"

    out["final_status"] = out.apply(final_status, axis=1)
    out["review_queue_flag"] = out["final_status"].isin(["SOFT_FAIL", "HARD_FAIL"])
    out["hard_fail_flag"] = out["final_status"].eq("HARD_FAIL")
    out["empty_flag"] = out["final_status"].eq("DOWNLOADED_EMPTY")
    out["pass_ok_flag"] = out["final_status"].isin(["PASS", "SOFT_FAIL"])
    return out


def _save_outputs(task_df: pd.DataFrame) -> None:
    task_df.to_csv(OUT_DIR / "task_status_detail.csv", index=False)

    by_ticker = task_df.pivot_table(index="ticker", columns="final_status", values="task_key", aggfunc="count", fill_value=0)
    for status in STATUS_ORDER:
        if status not in by_ticker.columns:
            by_ticker[status] = 0
    by_ticker = by_ticker[STATUS_ORDER].reset_index()
    by_ticker["expected_tasks"] = by_ticker[STATUS_ORDER].sum(axis=1)
    by_ticker["found_files"] = by_ticker[["PASS", "SOFT_FAIL", "HARD_FAIL"]].sum(axis=1)
    by_ticker["empty_ratio"] = np.where(by_ticker["expected_tasks"] > 0, by_ticker["DOWNLOADED_EMPTY"] / by_ticker["expected_tasks"], np.nan)
    by_ticker["problem_ratio"] = np.where(by_ticker["expected_tasks"] > 0, (by_ticker["SOFT_FAIL"] + by_ticker["HARD_FAIL"] + by_ticker["DOWNLOAD_FAIL"] + by_ticker["EXPECTED_MISSING"]) / by_ticker["expected_tasks"], np.nan)
    by_ticker = by_ticker.sort_values(["problem_ratio", "empty_ratio", "ticker"], ascending=[False, False, True])
    by_ticker.to_csv(OUT_DIR / "status_by_ticker.csv", index=False)

    by_date = task_df.pivot_table(index="date", columns="final_status", values="task_key", aggfunc="count", fill_value=0)
    for status in STATUS_ORDER:
        if status not in by_date.columns:
            by_date[status] = 0
    by_date = by_date[STATUS_ORDER].reset_index().sort_values("date")
    by_date.to_csv(OUT_DIR / "status_by_date.csv", index=False)

    cause_rows = []
    for _, r in task_df.iterrows():
        if r["final_status"] not in ["SOFT_FAIL", "HARD_FAIL"]:
            continue
        for cause in r["issues_list"] + r["warns_list"]:
            cause_rows.append({"ticker": r["ticker"], "date": r["date"], "final_status": r["final_status"], "cause": cause})
    pd.DataFrame(cause_rows).to_csv(OUT_DIR / "task_causes_detail.csv", index=False)


def _plot_status_counts(task_df: pd.DataFrame) -> None:
    counts = task_df["final_status"].value_counts().reindex(STATUS_ORDER, fill_value=0)
    plt.figure(figsize=(10, 4))
    plt.bar(counts.index, counts.values, color=[STATUS_COLOR[s] for s in counts.index])
    plt.title("Trades ticks: conteo de tareas por estado final")
    plt.ylabel("tasks")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    plt.show()


def _plot_ticker_status(task_df: pd.DataFrame) -> None:
    pivot = task_df.pivot_table(index="ticker", columns="final_status", values="task_key", aggfunc="count", fill_value=0)
    for s in STATUS_ORDER:
        if s not in pivot.columns:
            pivot[s] = 0
    pivot = pivot[STATUS_ORDER]
    pivot["problem_score"] = pivot[["HARD_FAIL", "SOFT_FAIL", "DOWNLOAD_FAIL", "EXPECTED_MISSING", "DOWNLOADED_EMPTY"]].sum(axis=1)
    pivot = pivot.sort_values(["problem_score", "DOWNLOADED_EMPTY", "HARD_FAIL", "SOFT_FAIL"], ascending=False).head(TOP_N)
    if pivot.empty:
        return
    plt.figure(figsize=(12, max(4, 0.45 * len(pivot))))
    left = np.zeros(len(pivot))
    y = np.arange(len(pivot))
    for s in STATUS_ORDER:
        vals = pivot[s].values
        plt.barh(y, vals, left=left, label=s, color=STATUS_COLOR[s])
        left += vals
    plt.yticks(y, pivot.index)
    plt.gca().invert_yaxis()
    plt.title(f"Top {len(pivot)} tickers por carga de hallazgos")
    plt.xlabel("tasks")
    plt.legend()
    plt.tight_layout()
    plt.show()


def _plot_calendar(task_df: pd.DataFrame) -> None:
    tlist = task_df.groupby("ticker").size().sort_values(ascending=False).head(TOP_N).index.tolist()
    cal = task_df[task_df["ticker"].isin(tlist)].copy()
    if cal.empty:
        return
    status_to_int = {s: i for i, s in enumerate(STATUS_ORDER)}
    cal["status_code"] = cal["final_status"].map(status_to_int)
    cal = cal.sort_values(["ticker", "date"])
    fig_h = max(4, 0.45 * len(tlist))
    plt.figure(figsize=(14, fig_h))
    for i, ticker in enumerate(tlist):
        d = cal[cal["ticker"] == ticker]
        plt.scatter(d["date"], np.full(len(d), i), c=d["status_code"], cmap=plt.matplotlib.colors.ListedColormap([STATUS_COLOR[s] for s in STATUS_ORDER]), vmin=0, vmax=len(STATUS_ORDER)-1, s=80, marker='s')
    plt.yticks(range(len(tlist)), tlist)
    plt.title("Mapa granular ticker x fecha de estados finales")
    plt.xlabel("date")
    plt.ylabel("ticker")
    plt.tight_layout()
    plt.show()


def _plot_empty_context(task_df: pd.DataFrame) -> None:
    empties = task_df[task_df["final_status"] == "DOWNLOADED_EMPTY"].copy()
    if empties.empty:
        return
    top = empties["ticker"].value_counts().head(min(TOP_N, 10)).index.tolist()
    rows = []
    for ticker in top:
        d = empties[empties["ticker"] == ticker]
        rows.append({
            "ticker": ticker,
            "empty_tasks": int(len(d)),
            "date_min": d["date"].min(),
            "date_max": d["date"].max(),
        })
    df = pd.DataFrame(rows)
    if display is not None:
        display(df)
    else:
        print(df.to_string(index=False))


def _plot_intraday_case(row: pd.Series) -> None:
    fp = Path(str(row.get("file_path", "")))
    if not fp.exists():
        print("No existe parquet intradia para este caso.")
        return
    try:
        df = _load_intraday_trades(fp)
    except Exception as ex:
        print(f"No se pudo leer parquet intradia: {ex}")
        return
    if df.empty:
        print("Parquet intradia vacio.")
        return
    if len(df) > MAX_PLOT_POINTS:
        df = df.iloc[:MAX_PLOT_POINTS].copy()
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=False)
    axes[0].plot(df["timestamp"], df["price"], linewidth=0.8, color="#1d3557")
    axes[0].set_title(f"Trades intradia | {row['ticker']} | {pd.Timestamp(row['date']).date()} | {row['final_status']}")
    axes[0].set_ylabel("price")

    minute = df.set_index("timestamp").resample("1min").agg(price_last=("price", "last"), size_sum=("size", "sum")).reset_index()
    axes[1].bar(minute["timestamp"], minute["size_sum"].fillna(0), width=0.0008, color="#457b9d")
    axes[1].set_ylabel("size per min")
    axes[1].set_xlabel("timestamp")
    plt.tight_layout()
    plt.show()

    exch = df["exchange"].value_counts().head(12)
    plt.figure(figsize=(8, 3.5))
    plt.bar(exch.index.astype(str), exch.values, color="#8d99ae")
    plt.title("Distribucion de prints por exchange")
    plt.xlabel("exchange")
    plt.ylabel("rows")
    plt.tight_layout()
    plt.show()


def _plot_ohlcv_1m_day(row: pd.Series) -> bool:
    ticker = str(row["ticker"])
    d = pd.Timestamp(row["date"])
    day = _load_ohlcv_1m_day(ticker, d)
    if day.empty:
        print("Sin ohlcv_1m local para la fecha exacta. Fallback a daily.")
        return False
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    axes[0].plot(day["ts_utc"], day["c"], linewidth=1.0, color="#1d3557")
    axes[0].fill_between(day["ts_utc"], day["l"], day["h"], color="#a8dadc", alpha=0.4)
    axes[0].set_title(f"OHLCV 1m del dia seleccionado | {ticker} | {d.date()}")
    axes[0].set_ylabel("close / range")
    axes[1].bar(day["ts_utc"], day["v"].fillna(0), width=0.0008, color="#457b9d")
    axes[1].set_ylabel("volume")
    axes[1].set_xlabel("ts_utc")
    plt.tight_layout()
    plt.show()
    if display is not None:
        display(day[[c for c in ["ticker", "ts_utc", "o", "h", "l", "c", "v", "n"] if c in day.columns]].head(20))
    return True


def _plot_daily_context(row: pd.Series) -> None:
    ticker = str(row["ticker"])
    d = pd.Timestamp(row["date"])
    daily = _load_daily_context(ticker, d)
    if daily.empty:
        print("Sin contexto daily local para este ticker.")
        return
    lo = d - pd.Timedelta(days=CONTEXT_DAYS)
    hi = d + pd.Timedelta(days=CONTEXT_DAYS)
    w = daily[(daily["date"] >= lo) & (daily["date"] <= hi)].copy()
    if w.empty:
        print("Sin ventana daily alrededor de la fecha seleccionada.")
        return
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    axes[0].plot(w["date"], w["c"], marker="o", linewidth=1.2, color="#2a9d8f")
    axes[0].axvline(d, color="#e76f51", linestyle="--", linewidth=1.2)
    axes[0].set_title(f"Contexto daily ?{CONTEXT_DAYS}d | {ticker} | {d.date()}")
    axes[0].set_ylabel("close")
    axes[1].bar(w["date"], w["v"].fillna(0), color="#264653")
    axes[1].axvline(d, color="#e76f51", linestyle="--", linewidth=1.2)
    axes[1].set_ylabel("volume")
    axes[1].set_xlabel("date")
    plt.tight_layout()
    plt.show()

    exact = daily[daily["date"] == d]
    if display is not None:
        display(exact if not exact.empty else pd.DataFrame([{"ticker": ticker, "date": d, "note": "selected date absent in daily context"}]))


def _build_explorer(task_df: pd.DataFrame) -> None:
    print("\n=== EXPLORADOR GRANULAR POR TICKER / FECHA / ESTADO ===")
    print("Objetivo: abrir un caso especifico y ver si el problema es vacio real, fallo de descarga o anomalia intradia leve/fuerte.")

    if widgets is None or display is None or clear_output is None:
        print("ipywidgets no disponible. Usa task_status_detail.csv y selecciona ticker/date manualmente.")
        return

    status_w = widgets.Dropdown(options=["ALL"] + STATUS_ORDER, value="ALL", description="status")
    ticker_w = widgets.Dropdown(options=[], description="ticker")
    date_w = widgets.Dropdown(options=[], description="date")
    out = widgets.Output()
    state = {"syncing": False}

    def _filtered_df() -> pd.DataFrame:
        df = task_df.copy()
        if status_w.value != "ALL":
            df = df[df["final_status"] == status_w.value]
        if ticker_w.value:
            df = df[df["ticker"] == ticker_w.value]
        return df.sort_values("date")

    def _refresh_tickers(*_args):
        state["syncing"] = True
        try:
            df = task_df if status_w.value == "ALL" else task_df[task_df["final_status"] == status_w.value]
            tickers = sorted(df["ticker"].dropna().astype(str).unique().tolist())
            ticker_w.options = tickers if tickers else [""]
            ticker_w.value = tickers[0] if tickers else ""
        finally:
            state["syncing"] = False

    def _refresh_dates(*_args):
        state["syncing"] = True
        try:
            df = _filtered_df()
            dates = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in df["date"].dropna().sort_values().unique().tolist()]
            date_w.options = dates if dates else [""]
            date_w.value = dates[0] if dates else ""
        finally:
            state["syncing"] = False

    def _render(*_args):
        if state["syncing"]:
            return
        with out:
            clear_output(wait=True)
            if not ticker_w.value or not date_w.value:
                print("Sin seleccion disponible.")
                return
            row = task_df[(task_df["ticker"] == ticker_w.value) & (task_df["date"] == pd.Timestamp(date_w.value))]
            if row.empty:
                print("Caso no encontrado.")
                return
            row = row.iloc[0]
            show_cols = [c for c in ["ticker", "date", "final_status", "download_status", "severity", "download_rows", "validated_rows", "duplicate_excess_ratio_pct", "adjacent_exact_repeats", "file_path"] if c in row.index]
            detail = pd.DataFrame([{c: row[c] for c in show_cols}]).T.reset_index()
            detail.columns = ["field", "value"]
            display(detail)
            if row["final_status"] in ["PASS", "SOFT_FAIL", "HARD_FAIL"] and row.get("file_exists", False):
                _plot_intraday_case(row)
            elif row["final_status"] == "DOWNLOADED_EMPTY":
                found_1m = _plot_ohlcv_1m_day(row)
                if not found_1m:
                    _plot_daily_context(row)
            else:
                _plot_daily_context(row)

    def _on_status_change(change):
        _refresh_tickers()
        _refresh_dates()

    def _on_ticker_change(change):
        _refresh_dates()

    def _on_date_change(change):
        _render()

    status_w.observe(_on_status_change, names="value")
    ticker_w.observe(_on_ticker_change, names="value")
    date_w.observe(_on_date_change, names="value")
    _refresh_tickers()
    _refresh_dates()
    display(widgets.VBox([widgets.HBox([status_w, ticker_w, date_w]), out]))
    _render()


expected_df, download_df, valid_df, evf_df = _normalize()
task_df = _build_task_status(expected_df, download_df, valid_df)
_save_outputs(task_df)

print("=== TRADES TICKS GRANULAR REVIEW ===")
if RUN_SUMMARY_JSON.exists():
    print(json.dumps(json.loads(RUN_SUMMARY_JSON.read_text(encoding="utf-8")), indent=2, ensure_ascii=False))

status_counts = task_df["final_status"].value_counts().reindex(STATUS_ORDER, fill_value=0).rename_axis("final_status").reset_index(name="tasks")
if display is not None:
    display(status_counts)
else:
    print(status_counts.to_string(index=False))

_plot_status_counts(task_df)
_plot_ticker_status(task_df)
_plot_calendar(task_df)
_plot_empty_context(task_df)

by_ticker_path = OUT_DIR / "status_by_ticker.csv"
by_date_path = OUT_DIR / "status_by_date.csv"
if by_ticker_path.exists():
    by_ticker = pd.read_csv(by_ticker_path)
    top_problem = by_ticker.sort_values(["problem_ratio", "empty_ratio", "ticker"], ascending=[False, False, True]).head(TOP_N)
    print("\n=== TOP TICKERS POR PROBLEM RATIO / EMPTY RATIO ===")
    if display is not None:
        display(top_problem)
    else:
        print(top_problem.to_string(index=False))

if by_date_path.exists():
    by_date = pd.read_csv(by_date_path)
    by_date["date"] = pd.to_datetime(by_date["date"], errors="coerce")
    problem_cols = [c for c in ["SOFT_FAIL", "HARD_FAIL", "DOWNLOAD_FAIL", "EXPECTED_MISSING", "DOWNLOADED_EMPTY"] if c in by_date.columns]
    top_dates = by_date.assign(problem_tasks=by_date[problem_cols].sum(axis=1)).sort_values(["problem_tasks", "date"], ascending=[False, True]).head(TOP_N)
    print("\n=== FECHAS CON MAYOR CONCENTRACION DE HALLAZGOS ===")
    if display is not None:
        display(top_dates)
    else:
        print(top_dates.to_string(index=False))

_build_explorer(task_df)
