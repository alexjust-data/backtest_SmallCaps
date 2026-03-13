from __future__ import annotations

import argparse
import json
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


def validate_events(
    df: pl.DataFrame,
    enforce_official_domains: bool = False,
    allowed_domains: set[str] | None = None,
) -> tuple[pl.DataFrame, dict]:
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    norm = (
        df.with_columns(
            [
            pl.col("ticker").cast(pl.Utf8).str.strip_chars().str.to_uppercase(),
            pl.col("cik").cast(pl.Utf8).str.strip_chars(),
            pl.col("event_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            pl.col("event_date").cast(pl.Utf8).str.strptime(pl.Date, strict=False),
            pl.col("source_name").cast(pl.Utf8).str.strip_chars(),
            pl.col("source_doc_type").cast(pl.Utf8).str.strip_chars(),
            pl.col("source_url").cast(pl.Utf8).str.strip_chars(),
            pl.col("source_title").cast(pl.Utf8).str.strip_chars(),
            pl.col("notes").cast(pl.Utf8),
            ]
        )
        .with_columns(
            [
                pl.col("source_url").str.to_lowercase().alias("source_url_lc"),
                pl.col("source_url").str.extract(r"^https?://([^/]+)", 1).alias("source_domain"),
            ]
        )
    )

    total_rows = int(norm.height)
    bad_event_date = int(norm.filter(pl.col("event_date").is_null()).height)
    bad_event_type = int(
        norm.filter(~pl.col("event_type").is_in(["listed", "delisted", "renamed", "halted", "suspended"])).height
    )
    bad_url = int(norm.filter(~pl.col("source_url_lc").str.starts_with("http")).height)

    out = (
        norm.filter(pl.col("event_date").is_not_null())
        .filter(pl.col("event_type").is_in(["listed", "delisted", "renamed", "halted", "suspended"]))
        .filter(pl.col("source_url_lc").str.starts_with("http"))
    )

    domain_rejected = 0
    if enforce_official_domains:
        domains = allowed_domains or {
            "sec.gov",
            "www.sec.gov",
            "nasdaq.com",
            "www.nasdaq.com",
            "nyse.com",
            "www.nyse.com",
            "otcmarkets.com",
            "www.otcmarkets.com",
        }
        before = int(out.height)
        out = out.filter(pl.col("source_domain").is_in(sorted(domains)))
        domain_rejected = before - int(out.height)

    before_dedup = int(out.height)
    out = out.unique(
        subset=[
            "ticker",
            "cik",
            "event_type",
            "event_date",
            "source_name",
            "source_doc_type",
            "source_url",
        ],
        keep="first",
    )
    dedup_removed = before_dedup - int(out.height)

    if out.height == 0:
        raise ValueError("No valid official events after validation")

    stats = {
        "rows_input": total_rows,
        "rows_bad_event_date": bad_event_date,
        "rows_bad_event_type": bad_event_type,
        "rows_bad_url": bad_url,
        "rows_rejected_by_domain": domain_rejected,
        "rows_removed_as_duplicate": dedup_removed,
        "rows_validated": int(out.height),
    }
    return out.drop(["source_url_lc"]), stats


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
            pl.col("source_doc_type").implode().list.unique().list.sort().alias("source_doc_types"),
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
    parser.add_argument("--out-validation-json", required=False, default=None)
    parser.add_argument("--enforce-official-domains", action="store_true")
    parser.add_argument(
        "--allowed-domains",
        required=False,
        default="",
        help="Comma-separated whitelist for source_url domains (used only with --enforce-official-domains).",
    )
    args = parser.parse_args()

    events_csv = Path(args.events_csv)
    out_lifecycle_csv = Path(args.out_lifecycle_csv)
    out_events_parquet = Path(args.out_events_parquet)
    out_lifecycle_parquet = Path(args.out_lifecycle_parquet)
    out_validation_json = Path(args.out_validation_json) if args.out_validation_json else None

    allowed_domains = {d.strip().lower() for d in args.allowed_domains.split(",") if d.strip()}

    events = pl.read_csv(events_csv)
    events_v, validation_stats = validate_events(
        events,
        enforce_official_domains=args.enforce_official_domains,
        allowed_domains=allowed_domains if allowed_domains else None,
    )
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

    if out_validation_json is not None:
        out_validation_json.parent.mkdir(parents=True, exist_ok=True)
        out_validation_json.write_text(
            json.dumps(
                {
                    "validation": validation_stats,
                    "enforce_official_domains": bool(args.enforce_official_domains),
                    "allowed_domains": sorted(allowed_domains) if allowed_domains else None,
                    "events_csv": str(events_csv),
                    "out_events_parquet": str(out_events_parquet),
                    "out_lifecycle_parquet": str(out_lifecycle_parquet),
                    "out_lifecycle_csv": str(out_lifecycle_csv),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    print(f"Validated events: {events_v.height}")
    print(f"Lifecycle rows: {lifecycle.height}")
    print(f"Validation stats: {validation_stats}")
    print(f"Saved: {out_events_parquet}")
    print(f"Saved: {out_lifecycle_parquet}")
    print(f"Saved: {out_lifecycle_csv}")
    if out_validation_json is not None:
        print(f"Saved: {out_validation_json}")


if __name__ == "__main__":
    main()
