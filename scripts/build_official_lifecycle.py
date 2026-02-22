from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

REQUIRED_COLS = {
    "ticker",
    "cik",
    "event_type",
    "event_date",
    "source_name",
    "source_doc_type",
    "source_url",
    "source_title",
    "notes",
}


def validate_events(df: pl.DataFrame) -> pl.DataFrame:
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = (
        df.with_columns([
            pl.col("ticker").cast(pl.Utf8).str.strip_chars().str.to_uppercase(),
            pl.col("cik").cast(pl.Utf8).str.strip_chars(),
            pl.col("event_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            pl.col("event_date").cast(pl.Utf8).str.strptime(pl.Date, strict=False),
            pl.col("source_name").cast(pl.Utf8).str.strip_chars(),
            pl.col("source_doc_type").cast(pl.Utf8).str.strip_chars(),
            pl.col("source_url").cast(pl.Utf8).str.strip_chars(),
            pl.col("source_title").cast(pl.Utf8).str.strip_chars(),
            pl.col("notes").cast(pl.Utf8),
        ])
        .filter(pl.col("event_date").is_not_null())
        .filter(pl.col("event_type").is_in(["listed", "delisted", "renamed", "halted", "suspended"]))
        .filter(pl.col("source_url").str.starts_with("http"))
    )

    if out.height == 0:
        raise ValueError("No valid official events after validation")

    return out


def build_lifecycle(events: pl.DataFrame) -> pl.DataFrame:
    listed = (
        events
        .filter(pl.col("event_type") == "listed")
        .group_by(["ticker", "cik"]) 
        .agg(pl.col("event_date").min().alias("list_date"))
    )

    delisted = (
        events
        .filter(pl.col("event_type").is_in(["delisted", "suspended", "halted"]))
        .group_by(["ticker", "cik"]) 
        .agg(pl.col("event_date").max().alias("delist_date"))
    )

    evidence = (
        events
        .group_by(["ticker", "cik"]) 
        .agg([
            pl.len().alias("evidence_count"),
            pl.col("source_name").n_unique().alias("source_count"),
            pl.col("source_doc_type").unique().alias("source_doc_types"),
        ])
    )

    lc = (
        listed.join(delisted, on=["ticker", "cik"], how="left")
        .join(evidence, on=["ticker", "cik"], how="left")
        .with_columns([
            pl.col("list_date").cast(pl.Date),
            pl.col("delist_date").cast(pl.Date),
            pl.when(pl.col("delist_date").is_not_null() & (pl.col("delist_date") < pl.col("list_date")))
            .then(pl.lit(True)).otherwise(pl.lit(False)).alias("date_order_invalid"),
        ])
        .filter(~pl.col("date_order_invalid"))
        .drop("date_order_invalid")
        .sort(["ticker", "cik"])
    )

    return lc


def main() -> None:
    parser = argparse.ArgumentParser(description="Build official ticker lifecycle from official event evidence")
    parser.add_argument("--events-csv", required=True)
    parser.add_argument("--out-lifecycle-csv", required=True)
    parser.add_argument("--out-events-parquet", required=True)
    parser.add_argument("--out-lifecycle-parquet", required=True)
    args = parser.parse_args()

    events_csv = Path(args.events_csv)
    out_lifecycle_csv = Path(args.out_lifecycle_csv)
    out_events_parquet = Path(args.out_events_parquet)
    out_lifecycle_parquet = Path(args.out_lifecycle_parquet)

    events = pl.read_csv(events_csv)
    events_v = validate_events(events)
    lifecycle = build_lifecycle(events_v)

    events_v.write_parquet(out_events_parquet)
    lifecycle.write_parquet(out_lifecycle_parquet)

    # CSV for notebook consumption
    lifecycle.select([
        "ticker",
        "cik",
        pl.col("list_date").cast(pl.Utf8),
        pl.col("delist_date").cast(pl.Utf8),
        "evidence_count",
        "source_count",
        pl.col("source_doc_types").list.join("|").alias("source_doc_types"),
    ]).write_csv(out_lifecycle_csv)

    print(f"Validated events: {events_v.height}")
    print(f"Lifecycle rows: {lifecycle.height}")
    print(f"Saved: {out_events_parquet}")
    print(f"Saved: {out_lifecycle_parquet}")
    print(f"Saved: {out_lifecycle_csv}")


if __name__ == "__main__":
    main()
