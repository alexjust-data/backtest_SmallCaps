from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
FINAL_ROOT = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "final_policy"

final_candidates = sorted(
    FINAL_ROOT.glob("step9_manual_integration_*/mvp_policy_mode_table_final_manual_v1.parquet"),
    key=lambda p: p.stat().st_mtime,
)
if not final_candidates:
    raise FileNotFoundError("No existe mvp_policy_mode_table_final_manual_v1.parquet (Step 9)")

final_fp = final_candidates[-1]
df = pl.read_parquet(final_fp)

if "decision_policy_final" not in df.columns:
    raise ValueError("No existe decision_policy_final en la policy final")

base_cols = [
    "ticker",
    "decision_policy_final",
    "root_cause_policy_final",
    "effective_scale_factor",
    "reviewer_decision",
    "reviewer_notes",
]
keep_cols = [c for c in base_cols if c in df.columns]

strict_df = (
    df.filter(pl.col("decision_policy_final") == "GO")
    .select(keep_cols)
    .sort("ticker")
)

guardrailed_df = (
    df.filter(pl.col("decision_policy_final").is_in(["GO", "WARN"]))
    .select(keep_cols)
    .sort("ticker")
)

exploratory_df = (
    df.filter(pl.col("decision_policy_final").is_in(["GO", "WARN", "FAIL"]))
    .select(keep_cols)
    .sort("ticker")
)

tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "final_universes" / f"step11_materialized_{tag}"
out_dir.mkdir(parents=True, exist_ok=True)

paths = {
    "strict_parquet": out_dir / "universe_strict.parquet",
    "strict_csv": out_dir / "universe_strict.csv",
    "guardrailed_parquet": out_dir / "universe_guardrailed.parquet",
    "guardrailed_csv": out_dir / "universe_guardrailed.csv",
    "exploratory_parquet": out_dir / "universe_exploratory.parquet",
    "exploratory_csv": out_dir / "universe_exploratory.csv",
    "summary_parquet": out_dir / "universe_sizes_summary.parquet",
    "summary_json": out_dir / "step11_materialization_manifest.json",
}

strict_df.write_parquet(paths["strict_parquet"])
strict_df.write_csv(paths["strict_csv"])
guardrailed_df.write_parquet(paths["guardrailed_parquet"])
guardrailed_df.write_csv(paths["guardrailed_csv"])
exploratory_df.write_parquet(paths["exploratory_parquet"])
exploratory_df.write_csv(paths["exploratory_csv"])

n_total = int(df.height)
summary_df = pl.DataFrame([
    {
        "universe": "strict",
        "n_tickers": int(strict_df.height),
        "share_vs_total": float(strict_df.height / n_total if n_total else 0.0),
        "rule": "decision_policy_final == GO",
    },
    {
        "universe": "guardrailed",
        "n_tickers": int(guardrailed_df.height),
        "share_vs_total": float(guardrailed_df.height / n_total if n_total else 0.0),
        "rule": "decision_policy_final in {GO, WARN}",
    },
    {
        "universe": "exploratory",
        "n_tickers": int(exploratory_df.height),
        "share_vs_total": float(exploratory_df.height / n_total if n_total else 0.0),
        "rule": "decision_policy_final in {GO, WARN, FAIL}",
    },
]).sort("n_tickers")
summary_df.write_parquet(paths["summary_parquet"])

manifest = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "source_final_policy": str(final_fp),
    "n_total_policy": n_total,
    "decision_counts_final": (
        df.group_by("decision_policy_final")
        .agg(pl.len().alias("n"))
        .sort("decision_policy_final")
        .to_dicts()
    ),
    "universes": {
        "strict": {
            "rule": "decision_policy_final == GO",
            "n_tickers": int(strict_df.height),
            "parquet": str(paths["strict_parquet"]),
            "csv": str(paths["strict_csv"]),
        },
        "guardrailed": {
            "rule": "decision_policy_final in {GO, WARN}",
            "n_tickers": int(guardrailed_df.height),
            "parquet": str(paths["guardrailed_parquet"]),
            "csv": str(paths["guardrailed_csv"]),
        },
        "exploratory": {
            "rule": "decision_policy_final in {GO, WARN, FAIL}",
            "n_tickers": int(exploratory_df.height),
            "parquet": str(paths["exploratory_parquet"]),
            "csv": str(paths["exploratory_csv"]),
        },
    },
    "outputs": {
        "summary_parquet": str(paths["summary_parquet"]),
        "summary_json": str(paths["summary_json"]),
    },
}
paths["summary_json"].write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 11 - FINAL UNIVERSE MATERIALIZATION ===")
print("source_final_policy:", final_fp)
print("output_dir:", out_dir)
print("[universe sizes]")
print(summary_df.sort("n_tickers"))
print("Saved:")
for k, v in paths.items():
    print(f"- {k}: {v}")
