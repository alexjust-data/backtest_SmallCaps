# STEP 16A v2 - EVENT INDEX BUILDER (P2/P3 FULL COVERAGE, quotes-only)
from pathlib import Path
from datetime import datetime, timezone

import polars as pl
from IPython.display import display, Markdown

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
DATA_ROOT = Path("C:/TSIS_Data/data")

# -----------------------------
# Config
# -----------------------------
MIN_TICKS_DAY = 20
EVENT_SCORE_THRESHOLD = 2.0
INCLUSION_MODE = "p2p3_only"  # options: p2p3_only | all_with_p2p3

RUN_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "event_index"
RUN_DIR = RUN_ROOT / f"step16_event_index_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_p2p3full"
RUN_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Inputs
# -----------------------------
repair_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "repair_queue"
repair_dirs = sorted(repair_root.glob("step15_repair_queue_*"), key=lambda p: p.stat().st_mtime)
if not repair_dirs:
    raise FileNotFoundError("No existe step15_repair_queue_* (ejecuta Paso 15)")
repair_dir = repair_dirs[-1]

queue_fp = repair_dir / "repair_queue_v1.parquet"
promoted_fp = repair_dir / "step15_first_pass_promoted_tickers.parquet"
if not queue_fp.exists():
    raise FileNotFoundError(f"No existe {queue_fp}")

v3_root = PROJECT_ROOT / "runs" / "data_quality" / "03_time_coverage_v3"
v3_dirs = sorted(v3_root.glob("*_prefilter_v3_multi_era"), key=lambda p: p.stat().st_mtime)
if not v3_dirs:
    raise FileNotFoundError("No existe *_prefilter_v3_multi_era")
v3_dir = v3_dirs[-1]
eligible_fp = v3_dir / "03_universe_eligible_v3.parquet"
if not eligible_fp.exists():
    raise FileNotFoundError(f"No existe {eligible_fp}")

eligible = pl.read_parquet(eligible_fp).select("ticker").unique().with_columns(pl.lit("eligible_v3").alias("source_group"))
queue = pl.read_parquet(queue_fp).select(["ticker", "priority_bucket", "repair_cause"]).unique()

if promoted_fp.exists():
    promoted = pl.read_parquet(promoted_fp).select("ticker").unique().with_columns(pl.lit(True).alias("promoted_window_shift"))
else:
    promoted = pl.DataFrame({"ticker": [], "promoted_window_shift": []}, schema={"ticker": pl.Utf8, "promoted_window_shift": pl.Boolean})

# Build candidate set with explicit inclusion mode
p23_queue = queue.filter(pl.col("priority_bucket").is_in(["P2", "P3"])).with_columns(pl.lit("repair_queue_residual").alias("source_group"))

if INCLUSION_MODE == "p2p3_only":
    candidate = (
        p23_queue
        .join(promoted, on="ticker", how="left")
        .with_columns([
            pl.col("promoted_window_shift").fill_null(False),
            pl.col("priority_bucket").fill_null("P2"),
            pl.col("repair_cause").fill_null("unknown"),
        ])
    )
else:
    base = (
        eligible
        .join(queue, on="ticker", how="left")
        .join(promoted, on="ticker", how="left")
        .with_columns([
            pl.col("promoted_window_shift").fill_null(False),
            pl.col("priority_bucket").fill_null("P0"),
            pl.col("repair_cause").fill_null("none"),
        ])
    )
    residual = (
        p23_queue
        .join(base.select("ticker"), on="ticker", how="anti")
        .join(promoted, on="ticker", how="left")
        .with_columns([
            pl.col("promoted_window_shift").fill_null(False),
            pl.col("priority_bucket").fill_null("P2"),
            pl.col("repair_cause").fill_null("unknown"),
        ])
    )
    candidate = pl.concat([base, residual], how="vertical_relaxed")

candidate = (
    candidate
    .select(["ticker", "source_group", "priority_bucket", "repair_cause", "promoted_window_shift"])
    .unique(subset=["ticker"])
    .sort(["priority_bucket", "ticker"])
)

candidate_fp = RUN_DIR / "step16a_candidate_universe.parquet"
candidate.write_parquet(candidate_fp)

# -----------------------------
# Quotes sources
# -----------------------------
quote_roots = [
    DATA_ROOT / "quotes_p95_2004_2018",
    DATA_ROOT / "quotes_p95_2019_2025",
    DATA_ROOT / "quotes_p95",
]


def iter_quote_day_files(ticker: str):
    seen = set()
    for root in quote_roots:
        tdir = root / ticker
        if not tdir.exists():
            continue
        for y in sorted(tdir.glob("year=*")):
            if not y.is_dir():
                continue
            for m in sorted(y.glob("month=*")):
                if not m.is_dir():
                    continue
                for d in sorted(m.glob("day=*")):
                    fp = d / "quotes.parquet"
                    if fp.exists() and fp not in seen:
                        seen.add(fp)
                        yield fp


def day_metrics_from_quotes(fp: Path):
    parts = fp.parts
    year = int([p for p in parts if p.startswith("year=")][0].split("=", 1)[1])
    month = int([p for p in parts if p.startswith("month=")][0].split("=", 1)[1])
    day = int([p for p in parts if p.startswith("day=")][0].split("=", 1)[1])

    # Some partitions are empty/corrupt with no columns; skip safely.
    try:
        q0 = pl.read_parquet(fp)
    except Exception:
        return None

    needed = {"bid_price", "ask_price"}
    if q0.height == 0 or not needed.issubset(set(q0.columns)):
        return None

    q = (
        q0.select(["bid_price", "ask_price"])
          .with_columns(((pl.col("bid_price") + pl.col("ask_price")) / 2.0).alias("mid"))
          .filter(pl.col("mid").is_finite())
    )

    n = q.height
    if n < MIN_TICKS_DAY:
        return None

    s = q.select([
        pl.col("mid").first().alias("mid_open"),
        pl.col("mid").last().alias("mid_close"),
        pl.col("mid").min().alias("mid_min"),
        pl.col("mid").max().alias("mid_max"),
    ]).to_dicts()[0]

    if s["mid_open"] is None or s["mid_open"] <= 0:
        return None

    return {
        "year": year,
        "month": month,
        "day": day,
        "date": f"{year:04d}-{month:02d}-{day:02d}",
        "n_ticks": int(n),
        "mid_open": float(s["mid_open"]),
        "mid_close": float(s["mid_close"]),
        "mid_min": float(s["mid_min"]),
        "mid_max": float(s["mid_max"]),
        "day_return": float((s["mid_close"] - s["mid_open"]) / s["mid_open"]),
        "day_range": float((s["mid_max"] - s["mid_min"]) / s["mid_open"]),
    }


rows = []
total = candidate.height
for i, rec in enumerate(candidate.iter_rows(named=True), start=1):
    tk = rec["ticker"]
    if i % 100 == 0 or i == 1 or i == total:
        print(f"[16A_v2] ticker {i}/{total}: {tk}")

    day_rows = []
    for fp in iter_quote_day_files(tk):
        m = day_metrics_from_quotes(fp)
        if m is None:
            continue
        m["ticker"] = tk
        m["priority_bucket"] = rec.get("priority_bucket", "P0")
        m["repair_cause"] = rec.get("repair_cause", "none")
        m["source_group"] = rec.get("source_group", "unknown")
        m["promoted_window_shift"] = bool(rec.get("promoted_window_shift", False))
        day_rows.append(m)

    if not day_rows:
        continue

    tdf = pl.DataFrame(day_rows).sort("date")

    tdf = tdf.with_columns([
        ((pl.col("day_range") - pl.col("day_range").median()) / (pl.col("day_range").std().fill_null(1e-9) + 1e-9)).alias("z_range"),
        ((pl.col("n_ticks") - pl.col("n_ticks").median()) / (pl.col("n_ticks").std().fill_null(1e-9) + 1e-9)).alias("z_ticks"),
        (pl.col("day_return").abs() / (pl.col("day_return").abs().median().fill_null(1e-9) + 1e-9)).alias("return_scale"),
    ])

    tdf = tdf.with_columns([
        (0.50 * pl.col("z_range") + 0.30 * pl.col("z_ticks") + 0.20 * pl.col("return_scale")).alias("event_score")
    ])

    tdf = tdf.with_columns([
        (
            (pl.col("event_score") >= EVENT_SCORE_THRESHOLD)
            | (pl.col("day_range") >= 0.12)
            | (pl.col("day_return").abs() >= 0.15)
        ).alias("is_event_day")
    ])

    rows.append(tdf)

if not rows:
    raise RuntimeError("STEP 16A v2 no generó filas. Revisa universo/quotes.")

out = pl.concat(rows, how="vertical_relaxed")
out = out.with_columns(pl.col("event_score").rank("dense", descending=True).over("ticker").alias("event_rank_in_ticker"))

# Defensive dedupe by daily key
out = (
    out.sort(["ticker", "date", "n_ticks", "event_score"], descending=[False, False, True, True])
       .unique(subset=["ticker", "date"], keep="first")
       .sort(["ticker", "date"])
)

event_fp = RUN_DIR / "step16_event_index_quotes_only.parquet"
out.write_parquet(event_fp)

summary = out.group_by(["priority_bucket"]).agg([
    pl.len().alias("n_days"),
    pl.col("ticker").n_unique().alias("n_tickers"),
    pl.col("is_event_day").sum().alias("n_event_days"),
    pl.col("event_score").mean().alias("event_score_mean"),
    pl.col("day_range").quantile(0.95).alias("day_range_p95"),
    pl.col("day_return").abs().quantile(0.95).alias("abs_return_p95"),
]).sort("priority_bucket")
summary_fp = RUN_DIR / "step16_event_index_summary.parquet"
summary.write_parquet(summary_fp)

top = (
    out.filter(pl.col("is_event_day") == True)
       .sort(["event_score", "day_range"], descending=True)
       .select(["ticker", "date", "priority_bucket", "repair_cause", "source_group", "n_ticks", "day_range", "day_return", "event_score", "event_rank_in_ticker"])
       .head(50)
)
top_fp = RUN_DIR / "step16_top_event_days.parquet"
top.write_parquet(top_fp)

print("=== STEP 16A v2 - EVENT INDEX BUILDER P2/P3 FULL ===")
print("mode:", INCLUSION_MODE)
print("candidate_universe:", candidate_fp)
print("event_index:", event_fp)
print("summary:", summary_fp)
print("top_events:", top_fp)
print("n_rows:", out.height)
print("n_tickers:", out.select(pl.col("ticker").n_unique()).item())
print("n_event_days:", out.filter(pl.col("is_event_day") == True).height)

display(Markdown(f"### Step 16A v2 output dir `{RUN_DIR}`"))
display(summary)
display(top.head(20))