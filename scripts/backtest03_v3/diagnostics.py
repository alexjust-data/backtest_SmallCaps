from __future__ import annotations

from pathlib import Path

import polars as pl


def compare_v2_v3(v2_prefilter_fp: Path, v3_prefilter_fp: Path) -> pl.DataFrame:
    v2 = pl.read_parquet(v2_prefilter_fp).select(["ticker", "eligible", "reason"]).rename(
        {"eligible": "eligible_v2", "reason": "reason_v2"}
    )
    v3 = pl.read_parquet(v3_prefilter_fp).select(["ticker", "prefilter_decision", "prefilter_reason"]).with_columns(
        (pl.col("prefilter_decision") == "eligible").alias("eligible_v3")
    )

    j = v2.join(v3, on="ticker", how="inner")
    return (
        j.with_columns(
            pl.when(pl.col("eligible_v2") == pl.col("eligible_v3"))
            .then(pl.lit("same"))
            .otherwise(pl.lit("changed"))
            .alias("delta_status")
        )
        .group_by(["delta_status", "eligible_v2", "eligible_v3", "reason_v2", "prefilter_reason"])
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )
