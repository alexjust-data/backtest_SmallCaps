from pathlib import Path
import ast
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

EVENTS_CSV = Path(globals().get("EVENTS_CSV", r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\polygon_realtime_audit\quotes_agent_strict_events_current.csv"))
FIGSIZE = tuple(globals().get("FIGSIZE", (12, 5)))
TOP_CAUSES = int(globals().get("TOP_CAUSES", 12))
ZERO_ZOOM_MAX_PCT = float(globals().get("ZERO_ZOOM_MAX_PCT", 0.10))  # zoom de 0% a 0.10%


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


def _show_df(df: pd.DataFrame):
    with pd.option_context("display.float_format", lambda x: f"{x:.6f}"):
        try:
            display(df)
        except Exception:
            print(df.to_string(index=False, float_format=lambda x: f"{x:.6f}"))


if not EVENTS_CSV.exists():
    raise FileNotFoundError(f"No existe EVENTS_CSV: {EVENTS_CSV}")

try:
    ev = pd.read_csv(EVENTS_CSV)
except Exception:
    ev = pd.read_csv(EVENTS_CSV, engine="python", on_bad_lines="skip")

required = ["file", "rows", "crossed_rows", "crossed_ratio_pct"]
missing = [c for c in required if c not in ev.columns]
if missing:
    raise RuntimeError(f"Faltan columnas en EVENTS_CSV: {missing}")

for c in ["rows", "crossed_rows", "crossed_ratio_pct"]:
    ev[c] = pd.to_numeric(ev[c], errors="coerce").fillna(0)

ext = ev["file"].astype(str).str.extract(r"\\(?P<ticker>[^\\]+)\\year=(?P<year>\d{4})\\month=(?P<month>\d{2})\\day=(?P<day>\d{2})\\quotes\.parquet$")
ev = pd.concat([ev, ext], axis=1)

files_total = int(len(ev))
files_with_cross = int((ev["crossed_rows"] > 0).sum())
rows_total = float(ev["rows"].sum())
crossed_total = float(ev["crossed_rows"].sum())
weighted_cross_pct = (crossed_total / rows_total * 100.0) if rows_total > 0 else np.nan

q = ev["crossed_ratio_pct"].quantile([0.0, 0.5, 0.9, 0.95, 0.99, 1.0]).to_dict()

summary = pd.DataFrame([
    {"metric": "files_total", "value": files_total},
    {"metric": "files_with_cross", "value": files_with_cross},
    {"metric": "files_with_cross_pct", "value": (files_with_cross / files_total * 100.0) if files_total else np.nan},
    {"metric": "rows_total", "value": rows_total},
    {"metric": "crossed_rows_total", "value": crossed_total},
    {"metric": "weighted_crossed_ratio_pct", "value": weighted_cross_pct},
    {"metric": "p00_file_crossed_ratio_pct", "value": q.get(0.0, np.nan)},
    {"metric": "p50_file_crossed_ratio_pct", "value": q.get(0.5, np.nan)},
    {"metric": "p90_file_crossed_ratio_pct", "value": q.get(0.9, np.nan)},
    {"metric": "p95_file_crossed_ratio_pct", "value": q.get(0.95, np.nan)},
    {"metric": "p99_file_crossed_ratio_pct", "value": q.get(0.99, np.nan)},
    {"metric": "p100_file_crossed_ratio_pct", "value": q.get(1.0, np.nan)},
])

print("\nMetricas globales de desviacion bid>ask:")
_show_df(summary)

# Granularidad exacta cerca de 0
zero = int((ev["crossed_ratio_pct"] == 0).sum())
gt0 = int((ev["crossed_ratio_pct"] > 0).sum())
band_edges = [0, 0.001, 0.005, 0.01, 0.05, 0.10, 0.50, 1.0, np.inf]  # en %
band_labels = [
    "0-0.001%",
    "0.001-0.005%",
    "0.005-0.01%",
    "0.01-0.05%",
    "0.05-0.10%",
    "0.10-0.50%",
    "0.50-1.00%",
    ">1.00%",
]
tmp = ev.loc[ev["crossed_ratio_pct"] > 0, "crossed_ratio_pct"].copy()
b = pd.cut(tmp, bins=band_edges, labels=band_labels, include_lowest=True, right=False)
band_df = b.value_counts(dropna=False).rename_axis("rango_crossed_ratio_pct").reset_index(name="files")
band_df["pct_files_total"] = np.where(files_total > 0, band_df["files"] / files_total * 100.0, np.nan)

print("\nGranularidad cerca de 0 (archivos):")
print(f"crossed_ratio_pct == 0: {zero} files")
print(f"crossed_ratio_pct  > 0: {gt0} files")
_show_df(band_df)

# Histograma global de crossed_ratio_pct (vista completa)
plt.figure(figsize=FIGSIZE)
vals = ev["crossed_ratio_pct"].astype(float)
plt.hist(vals, bins=60)
for k, color in [(0.5, "orange"), (0.95, "red"), (0.99, "purple")]:
    v = q.get(k)
    if pd.notna(v):
        plt.axvline(v, linestyle="--", linewidth=1.2, color=color, label=f"p{int(k*100)}={v:.4f}%")
plt.title("Distribucion file-level de crossed_ratio_pct (bid>ask)")
plt.xlabel("crossed_ratio_pct")
plt.ylabel("files")
plt.legend()
plt.tight_layout()
plt.show()

# Histograma zoom cerca de cero (sin truncar cola extrema en el grafico completo)
vals_zoom = vals[(vals >= 0) & (vals <= ZERO_ZOOM_MAX_PCT)]
plt.figure(figsize=FIGSIZE)
plt.hist(vals_zoom, bins=80)
plt.title(f"Zoom near zero: crossed_ratio_pct en [0, {ZERO_ZOOM_MAX_PCT}%]")
plt.xlabel("crossed_ratio_pct")
plt.ylabel("files")
plt.tight_layout()
plt.show()

# Rango por ticker (min-max + mediana)
by_t = (
    ev.groupby("ticker", dropna=False)["crossed_ratio_pct"]
    .agg(min_ratio="min", p50_ratio="median", max_ratio="max", files="count")
    .reset_index()
    .sort_values("max_ratio", ascending=False)
)

y = np.arange(len(by_t))
fig_h = max(8, min(0.28 * len(by_t), 40))
plt.figure(figsize=(max(FIGSIZE[0], 14), fig_h))
plt.hlines(y, by_t["min_ratio"], by_t["max_ratio"], linewidth=2)
plt.scatter(by_t["p50_ratio"], y, s=25, label="p50")
plt.yticks(y, by_t["ticker"].astype(str))
plt.gca().invert_yaxis()
plt.title("Rango de crossed_ratio_pct por ticker (min-max, punto=p50)")
plt.xlabel("crossed_ratio_pct")
plt.ylabel("ticker")
plt.legend()
plt.tight_layout()
plt.show()

# Granularidad por causa (conteo y rango)
cause_rows = []
for _, r in ev.iterrows():
    ratio = float(r.get("crossed_ratio_pct", 0) or 0)
    for c in _parse_listlike(r.get("issues")):
        cause_rows.append({"cause": c, "cause_type": "issue", "crossed_ratio_pct": ratio})
    for c in _parse_listlike(r.get("warns")):
        cause_rows.append({"cause": c, "cause_type": "warn", "crossed_ratio_pct": ratio})

ca = pd.DataFrame(cause_rows)
if ca.empty:
    print("\nNo hay causas registradas en issues/warns para esta corrida.")
else:
    csum = (
        ca.groupby(["cause", "cause_type"], dropna=False)
        .agg(
            count=("crossed_ratio_pct", "count"),
            min_ratio=("crossed_ratio_pct", "min"),
            p50_ratio=("crossed_ratio_pct", "median"),
            p95_ratio=("crossed_ratio_pct", lambda s: float(pd.Series(s).quantile(0.95))),
            max_ratio=("crossed_ratio_pct", "max"),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )

    print("\nCausas globales (conteo y rango de desviacion):")
    print("")
    print("Leyenda de causas globales (atributos):")
    cause_legend_lines = [
        "- cause : Nombre de la causa detectada en issues/warns.",
        "- cause_type : Tipo de causa (issue = fallo, warn = advertencia).",
        "- count : Numero de ocurrencias de esa causa en los archivos evaluados.",
        "- min_ratio : Minimo crossed_ratio_pct observado para esa causa.",
        "- p50_ratio : Mediana crossed_ratio_pct observado para esa causa.",
        "- p95_ratio : Percentil 95 de crossed_ratio_pct para esa causa.",
        "- max_ratio : Maximo crossed_ratio_pct observado para esa causa.",
    ]
    for line in cause_legend_lines:
        print(line)
    _show_df(csum)

    top = csum.head(TOP_CAUSES)

    # Histograma de conteos por causa
    plt.figure(figsize=(max(FIGSIZE[0], 12), FIGSIZE[1]))
    plt.barh(top["cause"] + " [" + top["cause_type"] + "]", top["count"])
    plt.gca().invert_yaxis()
    plt.title(f"Top {TOP_CAUSES} causas globales (conteo)")
    plt.xlabel("count")
    plt.ylabel("cause")
    plt.tight_layout()
    plt.show()

    # Rango por causa (min-max + p50)
    y = np.arange(len(top))
    plt.figure(figsize=(max(FIGSIZE[0], 12), FIGSIZE[1]))
    plt.hlines(y, top["min_ratio"], top["max_ratio"], linewidth=2)
    plt.scatter(top["p50_ratio"], y, s=25, label="p50")
    plt.yticks(y, top["cause"] + " [" + top["cause_type"] + "]")
    plt.gca().invert_yaxis()
    plt.title(f"Rango de crossed_ratio_pct por causa (Top {TOP_CAUSES})")
    plt.xlabel("crossed_ratio_pct")
    plt.ylabel("cause")
    plt.legend()
    plt.tight_layout()
    plt.show()
