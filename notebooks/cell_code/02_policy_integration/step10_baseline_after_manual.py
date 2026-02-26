from pathlib import Path
from datetime import datetime, timezone
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
BASELINE_ROOT = PROJECT_ROOT / "runs" / "baseline"

final_root = PROJECT_ROOT / "runs" / "backtest" / "02_policy_integration" / "final_policy"
final_candidates = sorted(
    final_root.glob("step9_manual_integration_*/mvp_policy_mode_table_final_manual_v1.parquet"),
    key=lambda p: p.stat().st_mtime,
)
if not final_candidates:
    raise FileNotFoundError("No existe final policy de Step 9")
final_fp = final_candidates[-1]

df = pl.read_parquet(final_fp)
counts = df.group_by("decision_policy_final").agg(pl.len().alias("n")).sort("decision_policy_final")
roots = df.group_by("root_cause_policy_final").agg(pl.len().alias("n")).sort("root_cause_policy_final")

tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = BASELINE_ROOT / f"after_manual_{tag}"
out_dir.mkdir(parents=True, exist_ok=True)

counts_fp = out_dir / "baseline_after_manual_counts.parquet"
roots_fp = out_dir / "baseline_after_manual_root_causes.parquet"
manifest_fp = out_dir / "baseline_after_manual_manifest.json"

counts.write_parquet(counts_fp)
roots.write_parquet(roots_fp)

manifest = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "source_final_policy": str(final_fp),
    "n_total": int(df.height),
    "decision_counts": counts.to_dicts(),
    "root_cause_counts": roots.to_dicts(),
    "outputs": {
        "counts_parquet": str(counts_fp),
        "root_causes_parquet": str(roots_fp),
        "manifest_json": str(manifest_fp),
    },
}
manifest_fp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

print("=== STEP 10 - BASELINE AFTER MANUAL ===")
print("source_final_policy:", final_fp)
print("out_dir:", out_dir)
print("\n[decision counts]")
print(counts)
print("\n[root cause counts]")
print(roots)
print("\nSaved:")
print(counts_fp)
print(roots_fp)
print(manifest_fp)