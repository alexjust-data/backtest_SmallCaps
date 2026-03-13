from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import base64

import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import display, Markdown, HTML


PROJECT_ROOT = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps")
REF_ROOT = PROJECT_ROOT / "data" / "reference"
RUN_ROOT = PROJECT_ROOT / "runs" / "data_quality" / "00_data_certification" / "coverage_diagnostics"
RUN_DIR = RUN_ROOT / f"step00f_coverage_diag_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
RUN_DIR.mkdir(parents=True, exist_ok=True)


def _latest_multisource_run_dir() -> Path | None:
    base = PROJECT_ROOT / "runs" / "data_quality" / "00_data_certification" / "official_multisource"
    if not base.exists():
        return None
    runs = sorted([p for p in base.glob("step00e_official_multisource_*") if p.is_dir()], key=lambda p: p.stat().st_mtime)
    return runs[-1] if runs else None


def _pick_input_files() -> tuple[Path, Path, Path]:
    latest = _latest_multisource_run_dir()
    if latest is not None:
        events = latest / "official_ticker_events.multisource.validated.csv"
        lifecycle = latest / "official_lifecycle_compiled.multisource.csv"
        gaps = latest / "official_multisource_gaps_by_ticker.csv"
        if events.exists() and lifecycle.exists() and gaps.exists():
            return events, lifecycle, gaps

    # fallback a reference
    events = REF_ROOT / "official_ticker_events.multisource.validated.csv"
    lifecycle = REF_ROOT / "official_lifecycle_compiled.multisource.csv"
    gaps = REF_ROOT / "official_multisource_gaps_by_ticker.csv"
    if not (events.exists() and lifecycle.exists() and gaps.exists()):
        raise FileNotFoundError(
            "No encontré inputs multisource. Ejecuta primero step00e_multisource_reconcile_coverage.py"
        )
    return events, lifecycle, gaps


def _read_universe() -> list[str]:
    p = REF_ROOT / "tickers_universe.txt"
    if not p.exists():
        raise FileNotFoundError(f"No existe universo: {p}")
    return sorted({x.strip().upper() for x in p.read_text(encoding="utf-8").splitlines() if x.strip()})


def _bool_col(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].astype(str).str.lower().isin(["true", "1", "t", "yes"])


def main() -> None:
    events_fp, lifecycle_fp, gaps_fp = _pick_input_files()
    universe = _read_universe()

    e = pd.read_csv(events_fp)
    l = pd.read_csv(lifecycle_fp)
    g = pd.read_csv(gaps_fp)

    # Normalize
    e["ticker"] = e["ticker"].astype(str).str.upper().str.strip()
    e["event_type"] = e["event_type"].astype(str).str.lower().str.strip()
    l["ticker"] = l["ticker"].astype(str).str.upper().str.strip()
    l["list_date"] = pd.to_datetime(l["list_date"], errors="coerce")
    l["delist_date"] = pd.to_datetime(l["delist_date"], errors="coerce")

    # Coverage core metrics
    universe_n = len(set(universe))
    tickers_any = e["ticker"].nunique()
    tickers_listed = e.loc[e["event_type"] == "listed", "ticker"].nunique()
    tickers_delisted = e.loc[e["event_type"].isin(["delisted", "halted", "suspended"]), "ticker"].nunique()
    listed_set = set(e.loc[e["event_type"] == "listed", "ticker"])
    delisted_set = set(e.loc[e["event_type"].isin(["delisted", "halted", "suspended"]), "ticker"])
    tickers_both = len(listed_set & delisted_set)
    lifecycle_rows = len(l)
    lifecycle_with_delist = int(l["delist_date"].notna().sum())
    lifecycle_without_delist = int(l["delist_date"].isna().sum())

    summary = {
        "universe_tickers": universe_n,
        "tickers_with_any_event": int(tickers_any),
        "tickers_with_listed_event": int(tickers_listed),
        "tickers_with_delist_signal_event": int(tickers_delisted),
        "tickers_with_both_listed_and_delisted": int(tickers_both),
        "lifecycle_rows": int(lifecycle_rows),
        "lifecycle_with_delist_date": int(lifecycle_with_delist),
        "lifecycle_without_delist_date": int(lifecycle_without_delist),
        "coverage_any_event_pct": round(100.0 * tickers_any / universe_n, 2),
        "coverage_listed_event_pct": round(100.0 * tickers_listed / universe_n, 2),
        "coverage_delist_signal_event_pct": round(100.0 * tickers_delisted / universe_n, 2),
        "coverage_lifecycle_delist_date_pct": round(100.0 * lifecycle_with_delist / universe_n, 2),
        "events_rows": int(len(e)),
    }

    # Gap aggregates
    gap_cols = [
        "gap_missing_any_event",
        "gap_missing_listed_event",
        "gap_missing_delist_signal_event",
        "gap_missing_lifecycle_list_date",
        "gap_missing_lifecycle_delist_date",
    ]
    gap_counts = {}
    for c in gap_cols:
        if c in g.columns:
            gap_counts[c] = int(_bool_col(g, c).sum())
        else:
            gap_counts[c] = None

    # Distributions
    ev_per_ticker = (
        e.groupby("ticker", as_index=False)["event_type"]
        .count()
        .rename(columns={"event_type": "events_count"})
        .sort_values("events_count", ascending=False)
    )
    source_counts = (
        e.groupby("source_name", as_index=False)
        .size()
        .rename(columns={"size": "rows"})
        .sort_values("rows", ascending=False)
    )
    event_type_counts = (
        e.groupby("event_type", as_index=False)
        .size()
        .rename(columns={"size": "rows"})
        .sort_values("rows", ascending=False)
    )

    if "list_date" in l.columns and "delist_date" in l.columns:
        duration_df = l[l["list_date"].notna() & l["delist_date"].notna()].copy()
        duration_df["duration_days"] = (duration_df["delist_date"] - duration_df["list_date"]).dt.days
        duration_df = duration_df[duration_df["duration_days"] >= 0]
    else:
        duration_df = pd.DataFrame(columns=["ticker", "duration_days"])

    # Save detailed tables
    ev_per_ticker.to_csv(RUN_DIR / "events_per_ticker.csv", index=False)
    source_counts.to_csv(RUN_DIR / "source_counts.csv", index=False)
    event_type_counts.to_csv(RUN_DIR / "event_type_counts.csv", index=False)
    duration_df.to_csv(RUN_DIR / "lifecycle_duration_days.csv", index=False)

    # Plot 1: core coverage bars
    plt.figure(figsize=(11, 5))
    labels = ["Any event", "Listed", "Delist signal", "Lifecycle delist_date"]
    vals = [
        summary["tickers_with_any_event"],
        summary["tickers_with_listed_event"],
        summary["tickers_with_delist_signal_event"],
        summary["lifecycle_with_delist_date"],
    ]
    pcts = [
        summary["coverage_any_event_pct"],
        summary["coverage_listed_event_pct"],
        summary["coverage_delist_signal_event_pct"],
        summary["coverage_lifecycle_delist_date_pct"],
    ]
    bars = plt.bar(labels, vals, color=["#4C78A8", "#72B7B2", "#F58518", "#E45756"])
    plt.axhline(universe_n, color="gray", linestyle="--", linewidth=1, label=f"Universe={universe_n}")
    for b, v, p in zip(bars, vals, pcts):
        plt.text(b.get_x() + b.get_width() / 2, v + max(universe_n * 0.005, 5), f"{v}\n({p}%)", ha="center", va="bottom")
    plt.title("Coverage by Ticker (absolute + % universe)")
    plt.ylabel("Tickers")
    plt.legend()
    plt.tight_layout()
    plt.savefig(RUN_DIR / "coverage_core_bars.png", dpi=150)
    plt.close()

    # Plot 2: gap bars
    plt.figure(figsize=(12, 5))
    g_labels = [
        "Missing any event",
        "Missing listed",
        "Missing delist signal",
        "Missing lifecycle list_date",
        "Missing lifecycle delist_date",
    ]
    g_vals = [gap_counts.get(c, 0) or 0 for c in gap_cols]
    bars = plt.bar(g_labels, g_vals, color="#9C755F")
    for b, v in zip(bars, g_vals):
        plt.text(b.get_x() + b.get_width() / 2, v + max(universe_n * 0.005, 5), str(v), ha="center", va="bottom")
    plt.title("Gap Counts by Rule")
    plt.ylabel("Tickers")
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(RUN_DIR / "gaps_bars.png", dpi=150)
    plt.close()

    # Plot 3: histogram events per ticker
    plt.figure(figsize=(10, 5))
    x = ev_per_ticker["events_count"].values
    if len(x) > 0:
        plt.hist(x, bins=min(30, max(5, len(set(x)))), color="#54A24B", alpha=0.85)
    plt.title("Histogram: Number of events per ticker")
    plt.xlabel("Events per ticker")
    plt.ylabel("Tickers")
    plt.tight_layout()
    plt.savefig(RUN_DIR / "hist_events_per_ticker.png", dpi=150)
    plt.close()

    # Plot 4: lifecycle duration distribution (if available)
    plt.figure(figsize=(10, 5))
    if not duration_df.empty:
        d = duration_df["duration_days"].values
        plt.hist(d, bins=40, color="#B279A2", alpha=0.85)
        plt.title("Histogram: Lifecycle duration (list_date -> delist_date)")
        plt.xlabel("Days listed")
        plt.ylabel("Tickers")
    else:
        plt.text(0.5, 0.5, "No ticker with both list_date and delist_date", ha="center", va="center")
        plt.title("Lifecycle duration unavailable")
        plt.axis("off")
    plt.tight_layout()
    plt.savefig(RUN_DIR / "hist_lifecycle_duration_days.png", dpi=150)
    plt.close()

    # Plot 5: top 20 tickers by evidence count
    top20 = ev_per_ticker.head(20).copy()
    plt.figure(figsize=(10, 7))
    if not top20.empty:
        plt.barh(top20["ticker"][::-1], top20["events_count"][::-1], color="#FF9DA6")
        plt.title("Top 20 tickers by event evidence count")
        plt.xlabel("Events")
        plt.ylabel("Ticker")
    else:
        plt.text(0.5, 0.5, "No data", ha="center", va="center")
        plt.axis("off")
    plt.tight_layout()
    plt.savefig(RUN_DIR / "top20_tickers_by_events.png", dpi=150)
    plt.close()

    out = {
        "summary": summary,
        "gap_counts": gap_counts,
        "inputs": {
            "events_csv": str(events_fp),
            "lifecycle_csv": str(lifecycle_fp),
            "gaps_csv": str(gaps_fp),
        },
        "run_dir": str(RUN_DIR),
    }
    (RUN_DIR / "coverage_diagnostics_summary.json").write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")

    print("RUN_DIR:", RUN_DIR)
    print("SUMMARY:", summary)
    print("GAP_COUNTS:", gap_counts)
    print("Saved plots:")
    plot_files = [
        "coverage_core_bars.png",
        "gaps_bars.png",
        "hist_events_per_ticker.png",
        "hist_lifecycle_duration_days.png",
        "top20_tickers_by_events.png",
    ]
    for p in plot_files:
        print(" -", RUN_DIR / p)

    # Inline notebook rendering (when executed via exec(...) inside Jupyter/IPython)
    try:
        display(Markdown("### Coverage Diagnostics Summary"))
        display(pd.DataFrame([summary]))
        display(Markdown("### Gap Counts"))
        display(pd.DataFrame([gap_counts]))
        display(Markdown("### Plots (2 columnas)"))

        cards = []
        for p in plot_files:
            fp = RUN_DIR / p
            if not fp.exists():
                continue
            b64 = base64.b64encode(fp.read_bytes()).decode("ascii")
            cards.append(
                f"""
                <div style="width:49%; box-sizing:border-box; padding:6px;">
                  <div style="font-weight:600; font-size:12px; margin-bottom:6px;">{p}</div>
                  <img src="data:image/png;base64,{b64}" style="width:100%; height:auto; border:1px solid #ddd; border-radius:6px;" />
                </div>
                """
            )
        if cards:
            html = f"""
            <div style="display:flex; flex-wrap:wrap; justify-content:space-between; align-items:flex-start;">
              {''.join(cards)}
            </div>
            """
            display(HTML(html))
    except Exception:
        # Non-notebook context: ignore inline display errors.
        pass


if __name__ == "__main__":
    main()
