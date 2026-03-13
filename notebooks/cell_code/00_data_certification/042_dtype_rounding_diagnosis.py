from pathlib import Path
import ast
import numpy as np
import pandas as pd
from IPython.display import display

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None


RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", RUN_DIR / "quotes_agent_strict_events_current.csv"))
HEAD_N = int(globals().get("HEAD_N", 20))
MAX_FILES_ANALYZE = int(globals().get("MAX_FILES_ANALYZE", 200))


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


def _ensure_ticker(df):
    out = df.copy()
    if "ticker" not in out.columns and "file" in out.columns:
        ext = out["file"].astype(str).str.extract(
            r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$"
        )
        out = pd.concat([out, ext], axis=1)
    return out


def _pct_int(s: pd.Series) -> float:
    if s.empty:
        return np.nan
    x = pd.to_numeric(s, errors="coerce")
    x = x[np.isfinite(x)]
    if x.empty:
        return np.nan
    return float(np.mean(np.isclose(x % 1.0, 0.0)) * 100.0)


def _safe_pct(mask: pd.Series) -> float:
    if mask.empty:
        return np.nan
    return float(np.mean(mask) * 100.0)


if pq is None:
    raise RuntimeError("pyarrow no disponible en este entorno.")
if not EVENTS_CSV.exists():
    raise FileNotFoundError(f"No existe EVENTS_CSV: {EVENTS_CSV}")

try:
    ev = pd.read_csv(EVENTS_CSV)
except Exception:
    ev = pd.read_csv(EVENTS_CSV, engine="python", on_bad_lines="skip")
ev = _ensure_ticker(ev)

mask_dtype = ev.get("warns", pd.Series([None] * len(ev))).apply(lambda x: "dtype_mismatch" in set(_parse_listlike(x)))
cand = ev[mask_dtype].copy()
if cand.empty:
    print("No hay archivos con dtype_mismatch en esta corrida.")
    raise SystemExit

cand = cand.dropna(subset=["file"]).drop_duplicates(subset=["file"]).head(MAX_FILES_ANALYZE)

print("=== DTYPE ROUNDING DIAGNOSIS ===")
print("Que es: diagnóstico para diferenciar 'solo formato int' vs degradación real de ask_price.")
print("Que ver: %ask entero vs %bid entero, %crossed, %spread_negativo y ask==round(bid).")
print(f"Archivos analizados: {len(cand)} (MAX_FILES_ANALYZE={MAX_FILES_ANALYZE})")

rows = []
for _, r in cand.iterrows():
    fp = Path(str(r["file"]))
    if not fp.exists():
        continue
    try:
        pf = pq.ParquetFile(fp)
        cols = set(pf.schema_arrow.names)
        need = [c for c in ["bid_price", "ask_price"] if c in cols]
        if len(need) < 2:
            continue
        d = pq.read_table(fp, columns=need).to_pandas()
        d = d.dropna(subset=["bid_price", "ask_price"])
        if d.empty:
            continue

        bid = pd.to_numeric(d["bid_price"], errors="coerce")
        ask = pd.to_numeric(d["ask_price"], errors="coerce")
        ok = np.isfinite(bid) & np.isfinite(ask)
        bid = bid[ok]
        ask = ask[ok]
        if bid.empty:
            continue

        spread = ask - bid
        crossed = bid > ask
        eq_round = np.isclose(ask, np.round(bid))

        rows.append(
            {
                "ticker": r.get("ticker"),
                "file": str(fp),
                "rows": int(len(bid)),
                "ask_integer_pct": _pct_int(ask),
                "bid_integer_pct": _pct_int(bid),
                "ask_eq_round_bid_pct": _safe_pct(pd.Series(eq_round)),
                "crossed_pct": _safe_pct(pd.Series(crossed)),
                "spread_zero_pct": _safe_pct(pd.Series(np.isclose(spread, 0.0))),
                "spread_negative_pct": _safe_pct(pd.Series(spread < 0.0)),
                "spread_mean": float(np.nanmean(spread)),
                "spread_p01": float(np.nanpercentile(spread, 1)),
                "spread_p50": float(np.nanpercentile(spread, 50)),
            }
        )
    except Exception:
        continue

files_df = pd.DataFrame(rows)
if files_df.empty:
    print("No se pudieron calcular métricas en los archivos candidatos.")
    raise SystemExit

for c in [
    "ask_integer_pct",
    "bid_integer_pct",
    "ask_eq_round_bid_pct",
    "crossed_pct",
    "spread_zero_pct",
    "spread_negative_pct",
    "spread_mean",
    "spread_p01",
    "spread_p50",
]:
    files_df[c] = pd.to_numeric(files_df[c], errors="coerce").round(4)

print("\nResumen global:")
global_df = pd.DataFrame(
    [
        {
            "files": int(len(files_df)),
            "rows_total": int(files_df["rows"].sum()),
            "ask_integer_pct_mean": round(float(files_df["ask_integer_pct"].mean()), 4),
            "bid_integer_pct_mean": round(float(files_df["bid_integer_pct"].mean()), 4),
            "ask_eq_round_bid_pct_mean": round(float(files_df["ask_eq_round_bid_pct"].mean()), 4),
            "crossed_pct_mean": round(float(files_df["crossed_pct"].mean()), 4),
            "spread_negative_pct_mean": round(float(files_df["spread_negative_pct"].mean()), 4),
        }
    ]
)
display(global_df)

print("\nTop ticker riesgo (head):")
ticker_df = (
    files_df.groupby("ticker", dropna=False)
    .agg(
        files=("file", "count"),
        rows=("rows", "sum"),
        ask_integer_pct=("ask_integer_pct", "mean"),
        bid_integer_pct=("bid_integer_pct", "mean"),
        ask_eq_round_bid_pct=("ask_eq_round_bid_pct", "mean"),
        crossed_pct=("crossed_pct", "mean"),
        spread_negative_pct=("spread_negative_pct", "mean"),
    )
    .reset_index()
)
ticker_df["risk_score"] = (
    ticker_df["crossed_pct"].fillna(0)
    + ticker_df["spread_negative_pct"].fillna(0)
    + 0.5 * (ticker_df["ask_eq_round_bid_pct"].fillna(0))
)
ticker_df = ticker_df.sort_values(["risk_score", "crossed_pct"], ascending=[False, False]).head(HEAD_N).reset_index(drop=True)
display(ticker_df)

print("\nTop archivos riesgo (head):")
files_view = files_df.copy()
files_view["risk_score"] = (
    files_view["crossed_pct"].fillna(0)
    + files_view["spread_negative_pct"].fillna(0)
    + 0.5 * files_view["ask_eq_round_bid_pct"].fillna(0)
)
files_view = files_view.sort_values(["risk_score", "crossed_pct"], ascending=[False, False]).head(HEAD_N).reset_index(drop=True)
display(files_view)
