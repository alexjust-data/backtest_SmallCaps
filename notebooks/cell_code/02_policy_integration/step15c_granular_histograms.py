from pathlib import Path
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from IPython.display import display, Markdown, Image as IPyImage

print("[15C] inicializando...")
PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
REPAIR_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "repair_queue"

queue_candidates = sorted(
    REPAIR_ROOT.glob("step15_repair_queue_*/repair_queue_v1.parquet"),
    key=lambda p: p.stat().st_mtime,
)
if not queue_candidates:
    raise FileNotFoundError("No existe repair_queue_v1.parquet (ejecuta Paso 15)")

queue_fp = queue_candidates[-1]
queue = pl.read_parquet(queue_fp)
print(f"[15C] queue: {queue_fp}")

p2 = queue.filter(pl.col("priority_bucket") == "P2")
p3 = queue.filter(pl.col("priority_bucket") == "P3")
print(f"[15C] tamaños -> P2={p2.height}, P3={p3.height}")
if p2.height == 0 and p3.height == 0:
    raise RuntimeError("No hay registros P2/P3 en la cola actual")

out_dir = REPAIR_ROOT / f"step15c_histograms_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
out_dir.mkdir(parents=True, exist_ok=True)


def _summary(df: pl.DataFrame, label: str) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame({"group": [label], "n": [0]})
    return pl.DataFrame({
        "group": [label],
        "n": [df.height],
        "months_min": [int(df["overlap_months_total"].min())],
        "months_p25": [float(df["overlap_months_total"].quantile(0.25))],
        "months_p50": [float(df["overlap_months_total"].quantile(0.50))],
        "months_p75": [float(df["overlap_months_total"].quantile(0.75))],
        "months_max": [int(df["overlap_months_total"].max())],
        "days_min": [int(df["overlap_quote_days_total"].min())],
        "days_p25": [float(df["overlap_quote_days_total"].quantile(0.25))],
        "days_p50": [float(df["overlap_quote_days_total"].quantile(0.50))],
        "days_p75": [float(df["overlap_quote_days_total"].quantile(0.75))],
        "days_max": [int(df["overlap_quote_days_total"].max())],
        "gap_min": [float(df["year_gap_o_first_minus_q_last"].min())],
        "gap_p50": [float(df["year_gap_o_first_minus_q_last"].quantile(0.50))],
        "gap_max": [float(df["year_gap_o_first_minus_q_last"].max())],
    })


summary = pl.concat([_summary(p2, "P2"), _summary(p3, "P3")], how="vertical_relaxed")
display(Markdown(f"### Paso 15C | input: `{queue_fp}`"))
display(summary)


def _granular_bins(max_value: int):
    return np.arange(-0.5, max_value + 1.5, 1.0)


def _plot_group(df: pl.DataFrame, label: str):
    if df.height == 0:
        display(Markdown(f"#### {label}: sin registros"))
        return

    print(f"[15C] graficando {label}...")
    m = df["overlap_months_total"].drop_nulls().to_numpy()
    d = df["overlap_quote_days_total"].drop_nulls().to_numpy()
    g = df["year_gap_o_first_minus_q_last"].drop_nulls().to_numpy()

    fig, axes = plt.subplots(1, 3, figsize=(18, 4.8))

    axes[0].hist(m, bins=_granular_bins(int(m.max())), color="#4f7ea8", alpha=0.9)
    axes[0].set_title(f"{label} | overlap_months_total")
    axes[0].set_xlabel("months")
    axes[0].set_ylabel("tickers")
    axes[0].grid(alpha=0.2)

    axes[1].hist(d, bins=_granular_bins(int(d.max())), color="#e07a1f", alpha=0.9)
    axes[1].set_title(f"{label} | overlap_quote_days_total")
    axes[1].set_xlabel("days")
    axes[1].set_ylabel("tickers")
    axes[1].grid(alpha=0.2)

    bins_gap = np.arange(np.floor(g.min()) - 0.5, np.ceil(g.max()) + 1.5, 1.0)
    axes[2].hist(g, bins=bins_gap, color="#5b8f5b", alpha=0.9)
    axes[2].set_title(f"{label} | year_gap_o_first_minus_q_last")
    axes[2].set_xlabel("years")
    axes[2].set_ylabel("tickers")
    axes[2].grid(alpha=0.2)

    fig.suptitle(f"Paso 15C | Distribución granular {label}", y=1.03)
    fig.tight_layout()

    fp = out_dir / f"{label.lower()}_granular_hist.png"
    fig.savefig(fp, dpi=140)
    plt.close(fig)

    display(Markdown(f"#### {label} - histogramas granulares"))
    display(IPyImage(filename=str(fp)))

    tail = (
        df.sort(["overlap_months_total", "overlap_quote_days_total", "year_gap_o_first_minus_q_last"])
        .select([
            "ticker",
            "repair_cause",
            "priority_bucket",
            "overlap_months_total",
            "overlap_quote_days_total",
            "year_gap_o_first_minus_q_last",
        ])
        .head(25)
    )
    display(Markdown(f"#### {label} - 25 casos más débiles"))
    display(tail)


_plot_group(p2, "P2")
_plot_group(p3, "P3")

summary_fp = out_dir / "step15c_summary.parquet"
summary.write_parquet(summary_fp)

print(f"[15C] terminado. salida={out_dir}")
display(Markdown(f"Guardado: `{out_dir}`"))