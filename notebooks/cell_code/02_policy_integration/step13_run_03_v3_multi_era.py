from pathlib import Path
import sys
import json

import polars as pl

PROJECT_ROOT = Path("C:/TSIS_Data/v1/backtest_SmallCaps")
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from backtest03_v3.contracts import default_config
from backtest03_v3.orchestrator import run_prefilter_v3
from backtest03_v3.diagnostics import compare_v2_v3

cfg = default_config(PROJECT_ROOT)
out_dir = run_prefilter_v3(cfg)

v3_prefilter = out_dir / "03_universe_prefilter_v3.parquet"
v3_summary_fp = out_dir / "03_universe_prefilter_v3_summary.json"
v3_summary = json.loads(v3_summary_fp.read_text(encoding="utf-8"))

v2_run = PROJECT_ROOT / "runs" / "data_quality" / "03_time_coverage" / "20260224_103217_massive_v2"
v2_prefilter = v2_run / "03_universe_prefilter.parquet"

delta = compare_v2_v3(v2_prefilter, v3_prefilter)
delta_fp = out_dir / "03_v2_vs_v3_prefilter_delta.parquet"
delta.write_parquet(delta_fp)

v2 = pl.read_parquet(v2_prefilter)
v3 = pl.read_parquet(v3_prefilter)

v2_n_eligible = int(v2.filter(pl.col("eligible") == True).height)
v3_n_eligible = int(v3.filter(pl.col("prefilter_decision") == "eligible").height)

print("=== STEP 13 - 03 v3 MULTI-ERA (EXECUTED) ===")
print("out_dir:", out_dir)
print("[v2 vs v3 eligible]")
print(
    {
        "v2_eligible": v2_n_eligible,
        "v3_eligible": v3_n_eligible,
        "delta": v3_n_eligible - v2_n_eligible,
    }
)
print("[v3 summary]")
print(
    {
        "n_universe_input": v3_summary.get("n_universe_input"),
        "n_eligible": v3_summary.get("n_eligible"),
        "n_manual_review": v3_summary.get("n_manual_review"),
        "n_excluded": v3_summary.get("n_excluded"),
    }
)
print("[top delta buckets]")
print(delta.sort("n", descending=True).head(12))
print("Saved:")
print(v3_prefilter)
print(v3_summary_fp)
print(delta_fp)