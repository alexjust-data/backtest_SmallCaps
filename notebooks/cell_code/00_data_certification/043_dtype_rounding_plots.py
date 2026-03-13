from pathlib import Path
import ast
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None


RUN_DIR = Path(globals().get("RUN_DIR", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit"))
EVENTS_CSV = Path(globals().get("EVENTS_CSV", RUN_DIR / "quotes_agent_strict_events_current.csv"))
MAX_FILES_ANALYZE = int(globals().get("MAX_FILES_ANALYZE", 300))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 4)))
TOP_N_WORST = int(globals().get("TOP_N_WORST", 20))


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
    x = pd.to_numeric(s, errors="coerce")
    x = x[np.isfinite(x)]
    if x.empty:
        return np.nan
    return float(np.mean(np.isclose(x % 1.0, 0.0)) * 100.0)


if pq is None:
    raise RuntimeError("pyarrow no disponible.")
if not EVENTS_CSV.exists():
    raise FileNotFoundError(f"No existe EVENTS_CSV: {EVENTS_CSV}")

try:
    ev = pd.read_csv(EVENTS_CSV)
except Exception:
    ev = pd.read_csv(EVENTS_CSV, engine="python", on_bad_lines="skip")
ev = _ensure_ticker(ev)

mask_dtype = ev.get("warns", pd.Series([None] * len(ev))).apply(lambda x: "dtype_mismatch" in set(_parse_listlike(x)))
cand = ev[mask_dtype].dropna(subset=["file"]).drop_duplicates(subset=["file"]).head(MAX_FILES_ANALYZE).copy()
if cand.empty:
    print("No hay archivos dtype_mismatch en esta corrida.")
    df = pd.DataFrame()

rows = []
if not cand.empty:
    for _, r in cand.iterrows():
        fp = Path(str(r["file"]))
        if not fp.exists():
            continue
        try:
            pf = pq.ParquetFile(fp)
            cols = set(pf.schema_arrow.names)
            if not {"bid_price", "ask_price"}.issubset(cols):
                continue
            d = pq.read_table(fp, columns=["bid_price", "ask_price"]).to_pandas().dropna()
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
                    "ask_integer_pct": float(np.mean(np.isclose(ask % 1.0, 0.0)) * 100.0),
                    "bid_integer_pct": _pct_int(bid),
                    "ask_eq_round_bid_pct": float(np.mean(eq_round) * 100.0),
                    "crossed_pct": float(np.mean(crossed) * 100.0),
                    "spread_negative_pct": float(np.mean(spread < 0.0) * 100.0),
                }
            )
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        print("No se pudieron calcular métricas.")
    else:
        for c in ["ask_integer_pct", "bid_integer_pct", "ask_eq_round_bid_pct", "crossed_pct", "spread_negative_pct"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(4)

        print("=== DTYPE ROUNDING PLOTS ===")
        print("Que es: vista grafica para validar si la enterizacion de ask_price se asocia a cruces bid>ask.")
        print("Que ver: histogramas + scatter; si se concentra en valores altos de crossed_pct, hay degradacion real.")
        print(f"files_analizados={len(df)}")

        # Lectura automatica (corrida concreta + criterio general)
        low_cross = int((df["crossed_pct"] < 5).sum())
        mid_cross = int(((df["crossed_pct"] >= 5) & (df["crossed_pct"] < 70)).sum())
        high_cross = int((df["crossed_pct"] >= 70).sum())

        low_int = int((df["ask_integer_pct"] < 30).sum())
        mid_int = int(((df["ask_integer_pct"] >= 30) & (df["ask_integer_pct"] < 95)).sum())
        high_int = int((df["ask_integer_pct"] >= 95).sum())

        critical_scatter = int(((df["ask_integer_pct"] >= 95) & (df["crossed_pct"] >= 70)).sum())
        critical_scatter_pct = (critical_scatter / len(df) * 100.0) if len(df) else 0.0

        print("\nLectura directa de tus 3 graficos:")
        print("\n1. Histograma crossed_pct")
        print("- Lectura general: muchos archivos cerca de 0% suelen ser sanos; un grupo grande en zona alta indica dano severo.")

        print("\n2. Histograma ask_integer_pct")
        print("- Lectura general: si aparece un bloque grande cerca de 100%, ask_price esta enterizado de forma masiva.")

        print("\n3. Scatter ask_integer_pct vs crossed_pct")
        print("- Lectura general: cuando la nube se concentra arriba-derecha, la enterizacion de ask se asocia a mas cruces bid>ask.")

        # 1) Hist crossed
        plt.figure(figsize=FIGSIZE)
        plt.hist(df["crossed_pct"].dropna(), bins=30)
        plt.title("Histograma crossed_pct por archivo")
        plt.xlabel("crossed_pct")
        plt.ylabel("files")
        plt.tight_layout()
        plt.show()

        # 2) Hist ask integer
        plt.figure(figsize=FIGSIZE)
        plt.hist(df["ask_integer_pct"].dropna(), bins=30)
        plt.title("Histograma ask_integer_pct por archivo")
        plt.xlabel("ask_integer_pct")
        plt.ylabel("files")
        plt.tight_layout()
        plt.show()

        # 3) Scatter
        plt.figure(figsize=FIGSIZE)
        plt.scatter(df["ask_integer_pct"], df["crossed_pct"], s=10, alpha=0.7)
        plt.title("Scatter: ask_integer_pct vs crossed_pct")
        plt.xlabel("ask_integer_pct")
        plt.ylabel("crossed_pct")
        plt.tight_layout()
        plt.show()

        # 4) Top peores
        worst = df.sort_values("crossed_pct", ascending=False).head(TOP_N_WORST).copy()
        labels = worst["ticker"].fillna("?").astype(str) + " | " + worst["file"].str.extract(r"day=(\d{2})\\quotes\.parquet$", expand=False).fillna("??")
        plt.figure(figsize=(12, 6))
        plt.barh(range(len(worst)), worst["crossed_pct"].values)
        plt.yticks(range(len(worst)), labels)
        plt.gca().invert_yaxis()
        plt.title(f"Top {TOP_N_WORST} archivos peores por crossed_pct")
        plt.xlabel("crossed_pct")
        plt.tight_layout()
        plt.show()

        # Salida minima (1-2 lineas ejemplo)
        print("\nEjemplo 2 archivos peores:")
        print(worst[["ticker", "file", "ask_integer_pct", "ask_eq_round_bid_pct", "crossed_pct"]].head(2).to_string(index=False))
