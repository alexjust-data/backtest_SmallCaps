from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import polars as pl

from build_official_lifecycle import build_lifecycle, validate_events


REQUIRED_COLS = [
    "ticker",
    "cik",
    "event_type",
    "event_date",
    "source_name",
    "source_doc_type",
    "source_url",
    "source_title",
    "notes",
]


def _load_universe(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Universe tickers file not found: {path}")
    txt = path.read_text(encoding="utf-8")
    out = {x.strip().upper() for x in txt.splitlines() if x.strip()}
    if not out:
        raise ValueError(f"Universe tickers file is empty: {path}")
    return out


def _expand_glob(glob_expr: str) -> list[Path]:
    if not glob_expr.strip():
        return []
    paths = sorted(Path().glob(glob_expr))
    return [p for p in paths if p.is_file()]


def _read_any_table(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    if path.suffix.lower() in {".csv", ".txt"}:
        return pl.read_csv(path, ignore_errors=True)
    raise ValueError(f"Unsupported file extension: {path}")


def _pick_col(columns: list[str], candidates: Iterable[str]) -> str | None:
    lc = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in lc:
            return lc[c.lower()]
    return None


def _normalize_event_type_expr(expr: pl.Expr) -> pl.Expr:
    e = expr.cast(pl.Utf8).str.to_lowercase().str.strip_chars()
    return (
        pl.when(
            e.str.contains("delist")
            | e.str.contains(r"\bform\s*25\b")
            | e.str.contains(r"\b25\b")
            | e.str.contains("deletion")
            | e.str.contains("remove")
            | e.str.contains("suspend")
            | e.str.contains("halt")
        )
        .then(pl.lit("delisted"))
        .when(
            e.str.contains("list")
            | e.str.contains("add")
            | e.str.contains("admission")
            | e.str.contains("ipo")
            | e.str.contains("new")
        )
        .then(pl.lit("listed"))
        .otherwise(pl.lit(None))
    )


def _source_rows_from_feed(path: Path, source_name: str) -> pl.DataFrame:
    raw = _read_any_table(path)
    if raw.height == 0:
        return pl.DataFrame(schema={c: pl.Utf8 for c in REQUIRED_COLS})

    cols = raw.columns
    ticker_col = _pick_col(cols, ["ticker", "symbol", "issue_symbol", "act_symbol", "security_symbol"])
    event_type_col = _pick_col(cols, ["event_type", "event", "action", "status", "event_code", "reason"])
    event_date_col = _pick_col(
        cols,
        [
            "event_date",
            "date",
            "effective_date",
            "trade_date",
            "notice_date",
            "listing_date",
            "delist_date",
            "timestamp",
        ],
    )
    cik_col = _pick_col(cols, ["cik", "issuer_cik"])
    url_col = _pick_col(cols, ["source_url", "url", "notice_url", "link"])
    title_col = _pick_col(cols, ["source_title", "title", "headline", "description"])

    if ticker_col is None or event_type_col is None or event_date_col is None:
        # File skipped because it does not have enough signal to infer events.
        return pl.DataFrame(schema={c: pl.Utf8 for c in REQUIRED_COLS})

    base = raw.with_columns(
        [
            pl.col(ticker_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("ticker"),
            _normalize_event_type_expr(pl.col(event_type_col)).alias("event_type"),
            pl.col(event_date_col).cast(pl.Utf8).str.slice(0, 10).alias("event_date"),
            (
                pl.col(cik_col).cast(pl.Utf8).str.strip_chars() if cik_col is not None else pl.lit("", dtype=pl.Utf8)
            ).alias("cik"),
            (
                pl.col(url_col).cast(pl.Utf8).str.strip_chars()
                if url_col is not None
                else pl.lit("https://www.nasdaqtrader.com/", dtype=pl.Utf8)
                if source_name == "NASDAQ"
                else pl.lit("https://ftp.nyse.com/cta_symbol_files/", dtype=pl.Utf8)
            ).alias("source_url"),
            (
                pl.col(title_col).cast(pl.Utf8).str.strip_chars()
                if title_col is not None
                else pl.lit(f"{source_name} feed row", dtype=pl.Utf8)
            ).alias("source_title"),
            pl.col(event_type_col).cast(pl.Utf8).str.strip_chars().alias("source_doc_type"),
            pl.lit(source_name, dtype=pl.Utf8).alias("source_name"),
            pl.lit(f"AUTO_PARSED_FROM_FILE={path.as_posix()}", dtype=pl.Utf8).alias("notes"),
        ]
    )

    return (
        base.select(REQUIRED_COLS)
        .filter(pl.col("ticker").is_not_null() & (pl.col("ticker") != ""))
        .filter(pl.col("event_type").is_not_null())
        .filter(pl.col("event_date").is_not_null() & (pl.col("event_date") != ""))
    )


def _load_sec_events(sec_csv: Path) -> pl.DataFrame:
    if not sec_csv.exists():
        raise FileNotFoundError(f"SEC events CSV not found: {sec_csv}")
    df = pl.read_csv(sec_csv)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"SEC CSV missing required columns: {missing}")
    return df.select(REQUIRED_COLS)


def _coverage_tables(universe: set[str], events_v: pl.DataFrame, lifecycle: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    uni_df = pl.DataFrame({"ticker": sorted(universe)})
    ev = events_v.with_columns(pl.col("ticker").cast(pl.Utf8).str.to_uppercase())

    any_ev = ev.select("ticker").unique().with_columns(pl.lit(True).alias("has_any_event"))
    listed_ev = (
        ev.filter(pl.col("event_type") == "listed")
        .select("ticker")
        .unique()
        .with_columns(pl.lit(True).alias("has_listed_event"))
    )
    delist_ev = (
        ev.filter(pl.col("event_type").is_in(["delisted", "suspended", "halted"]))
        .select("ticker")
        .unique()
        .with_columns(pl.lit(True).alias("has_delist_signal_event"))
    )

    lc = lifecycle.with_columns(
        [
            pl.col("ticker").cast(pl.Utf8).str.to_uppercase(),
            pl.col("list_date").is_not_null().alias("has_lifecycle_list_date"),
            pl.col("delist_date").is_not_null().alias("has_lifecycle_delist_date"),
        ]
    ).select(["ticker", "has_lifecycle_list_date", "has_lifecycle_delist_date"])

    gaps = (
        uni_df.join(any_ev, on="ticker", how="left")
        .join(listed_ev, on="ticker", how="left")
        .join(delist_ev, on="ticker", how="left")
        .join(lc, on="ticker", how="left")
        .with_columns(
            [
                pl.col("has_any_event").fill_null(False),
                pl.col("has_listed_event").fill_null(False),
                pl.col("has_delist_signal_event").fill_null(False),
                pl.col("has_lifecycle_list_date").fill_null(False),
                pl.col("has_lifecycle_delist_date").fill_null(False),
            ]
        )
        .with_columns(
            [
                (~pl.col("has_any_event")).alias("gap_missing_any_event"),
                (~pl.col("has_listed_event")).alias("gap_missing_listed_event"),
                (~pl.col("has_delist_signal_event")).alias("gap_missing_delist_signal_event"),
                (~pl.col("has_lifecycle_list_date")).alias("gap_missing_lifecycle_list_date"),
                (~pl.col("has_lifecycle_delist_date")).alias("gap_missing_lifecycle_delist_date"),
            ]
        )
        .sort("ticker")
    )

    universe_n = len(universe)
    summary = {
        "universe_tickers": universe_n,
        "tickers_with_any_event": int(gaps.filter(pl.col("has_any_event")).height),
        "tickers_with_listed_event": int(gaps.filter(pl.col("has_listed_event")).height),
        "tickers_with_delist_signal_event": int(gaps.filter(pl.col("has_delist_signal_event")).height),
        "tickers_with_lifecycle_list_date": int(gaps.filter(pl.col("has_lifecycle_list_date")).height),
        "tickers_with_lifecycle_delist_date": int(gaps.filter(pl.col("has_lifecycle_delist_date")).height),
        "coverage_any_event_pct": round(100.0 * int(gaps.filter(pl.col("has_any_event")).height) / universe_n, 2),
        "coverage_listed_event_pct": round(100.0 * int(gaps.filter(pl.col("has_listed_event")).height) / universe_n, 2),
        "coverage_delist_signal_event_pct": round(
            100.0 * int(gaps.filter(pl.col("has_delist_signal_event")).height) / universe_n, 2
        ),
        "coverage_lifecycle_delist_date_pct": round(
            100.0 * int(gaps.filter(pl.col("has_lifecycle_delist_date")).height) / universe_n, 2
        ),
    }
    return gaps, summary


def main() -> None:
    p = argparse.ArgumentParser(
        description="Reconcile official events from SEC + optional NASDAQ/NYSE local feeds and produce coverage/gap reports."
    )
    p.add_argument("--tickers-file", required=True, help="Universe tickers list (one per line).")
    p.add_argument("--sec-events-csv", required=True, help="Base SEC events CSV (official_ticker_events.csv).")
    p.add_argument("--nasdaq-glob", default="", help="Optional glob for NASDAQ files, e.g. data/external/nasdaq/**/*.*")
    p.add_argument("--nyse-glob", default="", help="Optional glob for NYSE/CTA files, e.g. data/external/nyse/**/*.*")
    p.add_argument("--out-events-csv", required=True)
    p.add_argument("--out-events-parquet", required=True)
    p.add_argument("--out-lifecycle-csv", required=True)
    p.add_argument("--out-lifecycle-parquet", required=True)
    p.add_argument("--out-validation-json", required=True)
    p.add_argument("--out-coverage-csv", required=True)
    p.add_argument("--out-gaps-csv", required=True)
    p.add_argument("--out-summary-json", required=True)
    p.add_argument("--enforce-official-domains", action="store_true")
    p.add_argument(
        "--allowed-domains",
        default="",
        help="Comma-separated domain whitelist used only with --enforce-official-domains.",
    )
    args = p.parse_args()

    universe = _load_universe(Path(args.tickers_file))
    sec = _load_sec_events(Path(args.sec_events_csv))

    nasdaq_files = _expand_glob(args.nasdaq_glob)
    nyse_files = _expand_glob(args.nyse_glob)

    extras: list[pl.DataFrame] = []
    for f in nasdaq_files:
        extras.append(_source_rows_from_feed(f, "NASDAQ"))
    for f in nyse_files:
        extras.append(_source_rows_from_feed(f, "NYSE_CTA"))

    all_frames = [sec] + extras
    merged = pl.concat(all_frames, how="vertical_relaxed")

    allowed_domains = {d.strip().lower() for d in args.allowed_domains.split(",") if d.strip()}
    events_v, validation_stats = validate_events(
        merged,
        enforce_official_domains=args.enforce_official_domains,
        allowed_domains=allowed_domains if allowed_domains else None,
    )
    lifecycle = build_lifecycle(events_v)
    gaps, coverage_summary = _coverage_tables(universe, events_v, lifecycle)

    source_counts = (
        events_v.group_by("source_name")
        .agg(pl.len().alias("rows"))
        .sort("rows", descending=True)
        .to_dict(as_series=False)
    )
    event_type_counts = (
        events_v.group_by("event_type")
        .agg(pl.len().alias("rows"))
        .sort("rows", descending=True)
        .to_dict(as_series=False)
    )

    out_events_csv = Path(args.out_events_csv)
    out_events_parquet = Path(args.out_events_parquet)
    out_lifecycle_csv = Path(args.out_lifecycle_csv)
    out_lifecycle_parquet = Path(args.out_lifecycle_parquet)
    out_validation_json = Path(args.out_validation_json)
    out_coverage_csv = Path(args.out_coverage_csv)
    out_gaps_csv = Path(args.out_gaps_csv)
    out_summary_json = Path(args.out_summary_json)

    for out in [
        out_events_csv,
        out_events_parquet,
        out_lifecycle_csv,
        out_lifecycle_parquet,
        out_validation_json,
        out_coverage_csv,
        out_gaps_csv,
        out_summary_json,
    ]:
        out.parent.mkdir(parents=True, exist_ok=True)

    events_v.write_csv(out_events_csv)
    events_v.write_parquet(out_events_parquet)
    lifecycle.select(
        [
            "ticker",
            "cik",
            pl.col("list_date").cast(pl.Utf8),
            pl.col("delist_date").cast(pl.Utf8),
            "evidence_count",
            "source_count",
            pl.col("source_doc_types").list.join("|").alias("source_doc_types"),
        ]
    ).write_csv(out_lifecycle_csv)
    lifecycle.write_parquet(out_lifecycle_parquet)

    pl.DataFrame([coverage_summary]).write_csv(out_coverage_csv)
    gaps.write_csv(out_gaps_csv)

    out_validation_json.write_text(
        json.dumps(
            {
                "validation": validation_stats,
                "sources_input": {
                    "sec_events_csv": args.sec_events_csv,
                    "nasdaq_files": [str(x) for x in nasdaq_files],
                    "nyse_files": [str(x) for x in nyse_files],
                },
                "source_counts": source_counts,
                "event_type_counts": event_type_counts,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    out_summary_json.write_text(
        json.dumps(
            {
                "coverage_summary": coverage_summary,
                "source_counts": source_counts,
                "event_type_counts": event_type_counts,
                "outputs": {
                    "events_csv": str(out_events_csv),
                    "events_parquet": str(out_events_parquet),
                    "lifecycle_csv": str(out_lifecycle_csv),
                    "lifecycle_parquet": str(out_lifecycle_parquet),
                    "coverage_csv": str(out_coverage_csv),
                    "gaps_csv": str(out_gaps_csv),
                    "validation_json": str(out_validation_json),
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print(f"Universe tickers: {len(universe)}")
    print(f"Merged input rows (pre-validation): {merged.height}")
    print(f"Validated events rows: {events_v.height}")
    print(f"Lifecycle rows: {lifecycle.height}")
    print(f"NASDAQ files parsed: {len(nasdaq_files)}")
    print(f"NYSE files parsed: {len(nyse_files)}")
    print(f"Coverage summary: {coverage_summary}")
    print(f"Saved: {out_events_csv}")
    print(f"Saved: {out_lifecycle_csv}")
    print(f"Saved: {out_coverage_csv}")
    print(f"Saved: {out_gaps_csv}")
    print(f"Saved: {out_summary_json}")


if __name__ == "__main__":
    main()

