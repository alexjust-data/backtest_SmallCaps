from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import polars as pl

from .contracts import PipelineConfig
from .evidence import compute_ticker_evidence
from .policy import classify_evidence


def run_prefilter_v3(cfg: PipelineConfig) -> Path:
    if not cfg.universe_source.exists():
        raise FileNotFoundError(f"Universe source no existe: {cfg.universe_source}")

    audit = pl.read_parquet(cfg.universe_source)
    # Keep parity with legacy 03 input universe.
    universe = (
        audit.filter(pl.col("has_overlap") == True)
        .select("ticker")
        .unique()
        .sort("ticker")
        .to_series()
        .to_list()
    )

    run_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_prefilter_v3_multi_era"
    out_dir = cfg.output_root / run_tag
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for t in universe:
        ev = compute_ticker_evidence(str(t), cfg.eras)
        decision, reason = classify_evidence(ev, cfg.thresholds)
        ev["prefilter_decision"] = decision
        ev["prefilter_reason"] = reason
        rows.append(ev)

    df = pl.DataFrame(rows)

    per_era_flat = []
    for r in rows:
        for era in r["per_era"]:
            per_era_flat.append({"ticker": r["ticker"], **era})
    era_df = pl.DataFrame(per_era_flat) if per_era_flat else pl.DataFrame()

    eligible_df = df.filter(pl.col("prefilter_decision") == "eligible").select("ticker").sort("ticker")

    prefilter_fp = out_dir / "03_universe_prefilter_v3.parquet"
    eligible_fp = out_dir / "03_universe_eligible_v3.parquet"
    evidence_fp = out_dir / "03_ticker_evidence_v3.parquet"
    per_era_fp = out_dir / "03_ticker_evidence_v3_per_era.parquet"
    summary_fp = out_dir / "03_universe_prefilter_v3_summary.json"

    df.write_parquet(prefilter_fp)
    eligible_df.write_parquet(eligible_fp)
    df.write_parquet(evidence_fp)
    if era_df.height > 0:
        era_df.write_parquet(per_era_fp)

    summary = {
        "run_tag": run_tag,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "universe_source": str(cfg.universe_source),
        "n_universe_input": int(len(universe)),
        "n_eligible": int(eligible_df.height),
        "n_manual_review": int(df.filter(pl.col("prefilter_decision") == "manual_review").height),
        "n_excluded": int(df.filter(pl.col("prefilter_decision") == "excluded").height),
        "decision_counts": df.group_by("prefilter_decision").agg(pl.len().alias("n")).to_dicts(),
        "reason_counts": df.group_by("prefilter_reason").agg(pl.len().alias("n")).to_dicts(),
        "thresholds": {
            "min_overlap_months": cfg.thresholds.min_overlap_months,
            "min_overlap_quote_days": cfg.thresholds.min_overlap_quote_days,
            "continuity_gap_years": cfg.thresholds.continuity_gap_years,
        },
        "outputs": {
            "prefilter": str(prefilter_fp),
            "eligible": str(eligible_fp),
            "evidence": str(evidence_fp),
            "per_era": str(per_era_fp),
            "summary": str(summary_fp),
        },
    }
    summary_fp.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_dir
