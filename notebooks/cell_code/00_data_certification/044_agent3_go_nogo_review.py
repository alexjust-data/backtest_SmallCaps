from pathlib import Path
import ast
import json
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    from IPython.display import display
except Exception:
    display = None


RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", RUN_DIR / "quotes_agent_strict_events_current.csv"))
RETRY_QUEUE_CSV = Path(globals().get("RETRY_QUEUE_CSV", RUN_DIR / "retry_queue_quotes_strict_current.csv"))
RETRY_FROZEN_CSV = Path(globals().get("RETRY_FROZEN_CSV", RUN_DIR / "retry_frozen_quotes_strict.csv"))
RUN_CONFIG_JSON = Path(globals().get("RUN_CONFIG_JSON", RUN_DIR / "run_config_quotes_strict.json"))
OUT_DIR = Path(globals().get("OUT_DIR", RUN_DIR / "agent03_outputs"))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 5)))
TOP_FILES = int(globals().get("TOP_FILES", 20))
TOP_TICKERS = int(globals().get("TOP_TICKERS", 15))
TOP_MONTHS = int(globals().get("TOP_MONTHS", 18))


def _show_df(df: pd.DataFrame):
    with pd.option_context("display.max_rows", 200, "display.max_columns", 50):
        if display is not None:
            display(df)
        else:
            print(df.to_string(index=False))


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()


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


def _parse_file_parts(series: pd.Series) -> pd.DataFrame:
    pat = re.compile(r"[/\\](?P<ticker>[^/\\]+)[/\\]year=(?P<year>\d{4})[/\\]month=(?P<month>\d{2})[/\\]day=(?P<day>\d{2})[/\\]quotes\.parquet$")
    ext = series.astype(str).str.extract(pat)
    ext["date"] = pd.to_datetime(ext["year"] + "-" + ext["month"] + "-" + ext["day"], errors="coerce")
    ext["year_month"] = ext["date"].dt.to_period("M").astype("string")
    return ext


def _build_problem_flags(ev: pd.DataFrame, max_cross: float, hard_cross: float) -> pd.DataFrame:
    out = ev.copy()
    out["crossed_ratio_pct"] = pd.to_numeric(out.get("crossed_ratio_pct"), errors="coerce").fillna(0.0)
    out["crossed_rows"] = pd.to_numeric(out.get("crossed_rows"), errors="coerce").fillna(0.0)
    out["rows"] = pd.to_numeric(out.get("rows"), errors="coerce").fillna(0.0)
    out["issue_list"] = out.get("issues", pd.Series([None] * len(out))).apply(_parse_listlike)
    out["warn_list"] = out.get("warns", pd.Series([None] * len(out))).apply(_parse_listlike)
    out["cross_any"] = out["crossed_rows"] > 0
    out["cross_soft"] = out["warn_list"].apply(lambda xs: "crossed_rows_present_but_under_threshold" in set(xs))
    out["cross_hard"] = out["issue_list"].apply(lambda xs: "crossed_ratio_gt_threshold" in set(xs))
    out["cross_hard_cap"] = out["issue_list"].apply(lambda xs: "crossed_ratio_gt_hard_cap" in set(xs))
    out["ask_integer_anomaly"] = out["issue_list"].apply(lambda xs: "ask_integer_with_crossed_anomaly" in set(xs))
    out["cross_bucket"] = np.select(
        [
            out["crossed_ratio_pct"].eq(0),
            (out["crossed_ratio_pct"] > 0) & (out["crossed_ratio_pct"] < 0.10),
            (out["crossed_ratio_pct"] >= 0.10) & (out["crossed_ratio_pct"] < 0.50),
            (out["crossed_ratio_pct"] >= 0.50) & (out["crossed_ratio_pct"] < max_cross),
            (out["crossed_ratio_pct"] >= max_cross) & (out["crossed_ratio_pct"] < hard_cross),
            out["crossed_ratio_pct"] >= hard_cross,
        ],
        [
            "0%",
            "(0,0.10%)",
            "[0.10,0.50%)",
            f"[0.50,{max_cross:.2f}%)",
            f"[{max_cross:.2f},{hard_cross:.2f}%)",
            f">= {hard_cross:.2f}%",
        ],
        default="OTHER",
    )
    return out


events = _safe_read_csv(EVENTS_CSV)
retry_df = _safe_read_csv(RETRY_QUEUE_CSV)
retry_frozen_df = _safe_read_csv(RETRY_FROZEN_CSV)

if events.empty:
    raise RuntimeError(f"No events found: {EVENTS_CSV}")

cfg = {}
if RUN_CONFIG_JSON.exists():
    cfg = json.loads(RUN_CONFIG_JSON.read_text(encoding="utf-8"))

max_cross = float(cfg.get("max_crossed_ratio_pct", 0.8))
hard_cross = float(cfg.get("hard_fail_crossed_pct", 5.0))

parts = _parse_file_parts(events["file"])
events = pd.concat([events.reset_index(drop=True), parts], axis=1)
events = events[events["ticker"].notna() & events["date"].notna()].copy()
events["ticker"] = events["ticker"].astype("string")
events["severity"] = events.get("severity", pd.Series(["UNKNOWN"] * len(events))).astype("string")
events = _build_problem_flags(events, max_cross=max_cross, hard_cross=hard_cross)

retry_files = set(retry_df["file"].dropna().astype(str).tolist()) if (not retry_df.empty and "file" in retry_df.columns) else set()
retry_frozen_files = set(retry_frozen_df["file"].dropna().astype(str).tolist()) if (not retry_frozen_df.empty and "file" in retry_frozen_df.columns) else set()
events["retry_pending_flag"] = events["file"].astype(str).isin(retry_files)
events["retry_frozen_flag"] = events["file"].astype(str).isin(retry_frozen_files)

coverage_csv = OUT_DIR / "coverage_by_ticker.csv"
diagnosis_csv = OUT_DIR / "ticker_diagnosis.csv"
coverage_df = _safe_read_csv(coverage_csv)
diagnosis_df = _safe_read_csv(diagnosis_csv)

print("=== GO / NO-GO REVIEW ===")
print("Objetivo: concentrar la decision operativa en los 2 problemas dominantes del run.")
print("Problema 1: severidad de crossed bid>ask.")
print("Problema 2: volumen operativo de retry_pending / retry_frozen_exhausted.")
print(f"Umbral estricto de la corrida: {max_cross:.2f}%")
print(f"Hard cap operativo de la corrida: {hard_cross:.2f}%")

# 1) Resumen de severidad crossed bid/ask
cross_summary = pd.DataFrame(
    [
        {"metric": "files_total", "value": int(len(events))},
        {"metric": "files_cross_any", "value": int(events["cross_any"].sum())},
        {"metric": "files_cross_soft", "value": int(events["cross_soft"].sum())},
        {"metric": "files_cross_hard_threshold", "value": int(events["cross_hard"].sum())},
        {"metric": "files_cross_hard_cap", "value": int(events["cross_hard_cap"].sum())},
        {"metric": "files_ask_integer_anomaly", "value": int(events["ask_integer_anomaly"].sum())},
        {"metric": "weighted_crossed_ratio_pct", "value": float(events["crossed_rows"].sum() / events["rows"].replace(0, np.nan).sum() * 100.0)},
        {"metric": "p90_crossed_ratio_pct", "value": float(events["crossed_ratio_pct"].quantile(0.90))},
        {"metric": "p95_crossed_ratio_pct", "value": float(events["crossed_ratio_pct"].quantile(0.95))},
        {"metric": "p99_crossed_ratio_pct", "value": float(events["crossed_ratio_pct"].quantile(0.99))},
        {"metric": "p100_crossed_ratio_pct", "value": float(events["crossed_ratio_pct"].max())},
    ]
)
print("\nResumen severidad bid>ask:")
_show_df(cross_summary)

bucket_order = [
    "0%",
    "(0,0.10%)",
    "[0.10,0.50%)",
    f"[0.50,{max_cross:.2f}%)",
    f"[{max_cross:.2f},{hard_cross:.2f}%)",
    f">= {hard_cross:.2f}%",
]
bucket_df = (
    events.groupby("cross_bucket", dropna=False)
    .agg(
        files=("file", "count"),
        tickers=("ticker", "nunique"),
        crossed_rows=("crossed_rows", "sum"),
        rows=("rows", "sum"),
    )
    .reset_index()
)
bucket_df["weighted_crossed_ratio_pct"] = np.where(bucket_df["rows"] > 0, bucket_df["crossed_rows"] / bucket_df["rows"] * 100.0, np.nan)
bucket_df["cross_bucket"] = pd.Categorical(bucket_df["cross_bucket"], categories=bucket_order, ordered=True)
bucket_df = bucket_df.sort_values("cross_bucket")

print("\nGranularidad de severidad bid>ask por buckets:")
_show_df(bucket_df)

plt.figure(figsize=FIGSIZE)
plt.bar(bucket_df["cross_bucket"].astype(str), bucket_df["files"])
plt.axvline(3.5, color="orange", linestyle="--", linewidth=1.2)
plt.axvline(4.5, color="red", linestyle="--", linewidth=1.2)
plt.title("Distribucion de archivos por bucket de crossed_ratio_pct")
plt.xlabel("bucket crossed_ratio_pct")
plt.ylabel("files")
plt.xticks(rotation=25, ha="right")
plt.tight_layout()
plt.show()

top_cross_files = events.sort_values(["crossed_ratio_pct", "crossed_rows"], ascending=[False, False]).copy()
top_cross_files = top_cross_files[
    ["ticker", "date", "severity", "crossed_ratio_pct", "crossed_rows", "rows", "retry_pending_flag", "retry_frozen_flag", "file"]
].head(TOP_FILES)
print(f"\nTop {TOP_FILES} archivos mas problematicos por crossed_ratio_pct:")
_show_df(top_cross_files)

# 2) Retry pipeline pressure
retry_summary = pd.DataFrame(
    [
        {"metric": "retry_pending_files", "value": int(events["retry_pending_flag"].sum())},
        {"metric": "retry_frozen_files", "value": int(events["retry_frozen_flag"].sum())},
        {"metric": "retry_pending_unique_tickers", "value": int(events.loc[events["retry_pending_flag"], "ticker"].nunique())},
        {"metric": "retry_frozen_unique_tickers", "value": int(events.loc[events["retry_frozen_flag"], "ticker"].nunique())},
    ]
)
print("\nPresion operativa de retry:")
_show_df(retry_summary)

retry_ticker = (
    events.groupby("ticker", dropna=False)
    .agg(
        files=("file", "count"),
        retry_pending=("retry_pending_flag", "sum"),
        retry_frozen=("retry_frozen_flag", "sum"),
        hard_fail=("cross_hard", "sum"),
        hard_cap=("cross_hard_cap", "sum"),
        soft_cross=("cross_soft", "sum"),
        max_crossed_ratio_pct=("crossed_ratio_pct", "max"),
        p95_crossed_ratio_pct=("crossed_ratio_pct", lambda s: float(pd.Series(s).quantile(0.95))),
    )
    .reset_index()
)
if not coverage_df.empty and "ticker" in coverage_df.columns:
    retry_ticker = retry_ticker.merge(
        coverage_df[["ticker", "coverage_ratio_ok", "missing_days_ok"]],
        on="ticker",
        how="left",
    )
retry_ticker = retry_ticker.sort_values(
    ["retry_frozen", "retry_pending", "hard_fail", "max_crossed_ratio_pct"],
    ascending=[False, False, False, False],
)
print(f"\nTop {TOP_TICKERS} tickers mas problematicos para estudio profundo:")
_show_df(retry_ticker.head(TOP_TICKERS))

retry_monthly = (
    events.assign(
        retry_pending=np.where(events["retry_pending_flag"], 1, 0),
        retry_frozen=np.where(events["retry_frozen_flag"], 1, 0),
        hard_fail=np.where(events["cross_hard"], 1, 0),
    )
    .groupby("year_month", dropna=False)[["retry_pending", "retry_frozen", "hard_fail"]]
    .sum()
    .reset_index()
    .sort_values("year_month")
)
retry_monthly = retry_monthly.tail(TOP_MONTHS)
print(f"\nSerie temporal mensual de problemas operativos (ultimos {TOP_MONTHS} meses visibles):")
_show_df(retry_monthly)

if not retry_monthly.empty:
    fig_h = max(8, min(0.28 * len(retry_monthly), 40))
    plt.figure(figsize=(max(FIGSIZE[0], 14), fig_h))
    y = np.arange(len(retry_monthly))
    plt.barh(y, retry_monthly["retry_pending"], label="retry_pending")
    plt.barh(y, retry_monthly["retry_frozen"], left=retry_monthly["retry_pending"], label="retry_frozen")
    plt.barh(
        y,
        retry_monthly["hard_fail"],
        left=retry_monthly["retry_pending"] + retry_monthly["retry_frozen"],
        label="hard_fail_cross",
    )
    plt.yticks(y, retry_monthly["year_month"])
    plt.gca().invert_yaxis()
    plt.title("Carga operativa mensual: retry_pending + retry_frozen + hard_fail")
    plt.xlabel("files")
    plt.ylabel("month")
    plt.legend()
    plt.tight_layout()
    plt.show()

# 3) Heatmap: top tickers x month for severe cases
severe_for_heatmap = events[
    events["cross_hard"] | events["cross_hard_cap"] | events["retry_pending_flag"] | events["retry_frozen_flag"]
].copy()
top_tickers_heat = (
    severe_for_heatmap.groupby("ticker")["file"].count().sort_values(ascending=False).head(TOP_TICKERS).index.tolist()
)
heat = (
    severe_for_heatmap[severe_for_heatmap["ticker"].isin(top_tickers_heat)]
    .pivot_table(index="ticker", columns="year_month", values="file", aggfunc="count", fill_value=0)
)
if not heat.empty:
    heat = heat.loc[top_tickers_heat]
    heat = heat.reindex(sorted(heat.columns), axis=1)
    heat_plot = heat.T
    plt.figure(figsize=(max(12, heat_plot.shape[1] * 0.75), max(10, min(0.28 * heat_plot.shape[0], 42))))
    plt.imshow(heat_plot.values, aspect="auto", cmap="YlOrRd")
    plt.colorbar(label="files problematicos")
    plt.yticks(np.arange(len(heat_plot.index)), heat_plot.index)
    plt.xticks(np.arange(len(heat_plot.columns)), heat_plot.columns, rotation=90)
    plt.title("Heatmap mes x ticker de archivos problematicos severos/operativos")
    plt.xlabel("ticker")
    plt.ylabel("month")
    plt.tight_layout()
    plt.show()

print("\nLectura tecnica sugerida para decision GO / NO-GO:")
print("1. Si el problema dominante esta en buckets < umbral y el weighted_crossed_ratio_pct sigue bajo, el dano es mas compatible con microestructura que con corrupcion masiva.")
print("2. Si retry_frozen_exhausted domina sobre retry_pending, el retry ya no esta resolviendo gran parte del problema.")
print("3. Los archivos de estudio profundo deben salir de la tabla de top archivos y de los tickers con mayor retry_frozen / hard_fail / max_crossed_ratio_pct.")
print("4. La heatmap ayuda a decidir si el dano es persistente por ticker o concentrado en meses concretos.")
