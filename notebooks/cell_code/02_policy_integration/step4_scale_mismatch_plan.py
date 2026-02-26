from pathlib import Path
from datetime import datetime, timezone
import json
import math
import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
RUN06 = PROJECT_ROOT / "runs" / "data_quality" / "06_ohlcv_vs_quotes" / "20260224_114030_mvp_03_06_alignment_massive_v2"
POLICY = RUN06 / "mvp_policy_mode_table.parquet"
OUT_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "scale_mismatch_diagnostics"
OUT_ROOT.mkdir(parents=True, exist_ok=True)

if not POLICY.exists():
    raise FileNotFoundError(f"No existe policy source: {POLICY}")

df = pl.read_parquet(POLICY)
sm = df.filter((pl.col("decision_policy") == "NOT_COMPARABLE") & (pl.col("root_cause_policy") == "scale_mismatch"))

if sm.height == 0:
    print("No scale_mismatch rows found.")
else:
    ladder = [0.01,0.02,0.04,0.05,0.1,0.2,0.25,0.5,1,2,4,5,8,10,20,25,40,50,100]

    def infer_factor(x):
        if x is None:
            return (None, None, None)
        x = float(x)
        if math.isnan(x) or math.isinf(x) or x <= 0:
            return (None, None, None)
        best = min(ladder, key=lambda f: abs(x - f))
        abs_err = abs(x - best)
        rel_err = abs_err / max(abs(best), 1e-12)
        return (best, abs_err, rel_err)

    rows = []
    keep_cols = [c for c in [
        "ticker","n_days","coverage_global","p_overlap","p_in_range",
        "out_dist_abs_p95","price_scale_ratio_close_over_mid","traceability"
    ] if c in sm.columns]

    for r in sm.select(keep_cols).to_dicts():
        f, ae, re = infer_factor(r.get("price_scale_ratio_close_over_mid"))
        r["candidate_scale_factor"] = f
        r["abs_error"] = ae
        r["rel_error"] = re
        if f is None:
            conf = "LOW"
        elif ae <= 0.05 or re <= 0.01:
            conf = "HIGH"
        elif ae <= 0.25 or re <= 0.03:
            conf = "MEDIUM"
        else:
            conf = "LOW"
        r["remediation_confidence"] = conf
        r["proposed_action"] = (
            "AUTO_SCALE_RECHECK" if conf in ["HIGH","MEDIUM"] else "MANUAL_REVIEW"
        )
        rows.append(r)

    plan = pl.DataFrame(rows).sort(["remediation_confidence","abs_error"], descending=[False, False])

    conf_summary = plan.group_by(["remediation_confidence","proposed_action"]).agg(pl.len().alias("n_tickers")).sort(["remediation_confidence","proposed_action"])
    factor_summary = plan.group_by("candidate_scale_factor").agg(pl.len().alias("n_tickers")).sort("n_tickers", descending=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = OUT_ROOT / f"step4_scale_mismatch_plan_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    plan_fp = out_dir / "scale_mismatch_plan.parquet"
    conf_fp = out_dir / "scale_mismatch_conf_summary.parquet"
    factor_fp = out_dir / "scale_mismatch_factor_summary.parquet"

    plan.write_parquet(plan_fp)
    conf_summary.write_parquet(conf_fp)
    factor_summary.write_parquet(factor_fp)

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": str(POLICY),
        "n_scale_mismatch": int(sm.height),
        "outputs": {
            "plan": str(plan_fp),
            "confidence_summary": str(conf_fp),
            "factor_summary": str(factor_fp),
        }
    }
    (out_dir / "step4_scale_mismatch_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=== STEP 4 - SCALE_MISMATCH PLAN ===")
    print("source_policy:", POLICY)
    print("output_dir:", out_dir)
    print("n_scale_mismatch:", sm.height)
    print("\n[confidence summary]")
    print(conf_summary)
    print("\n[factor summary]")
    print(factor_summary)
    print("\n[sample plan rows]")
    print(plan.head(25))
