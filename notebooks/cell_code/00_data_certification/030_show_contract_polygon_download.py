from pathlib import Path
import json
import pandas as pd

try:
    import yaml
except Exception as e:
    raise RuntimeError("PyYAML no disponible en este entorno. Instala con: pip install pyyaml") from e


CONTRACT_FILE = Path(
    globals().get(
        "CONTRACT_FILE",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\00_data_certification\contracts_polygon_download.yaml",
    )
)

DATASET_TO_VIEW = globals().get("DATASET_TO_VIEW", "quotes")

if not CONTRACT_FILE.exists():
    raise FileNotFoundError(f"Contract file not found: {CONTRACT_FILE}")

contract = yaml.safe_load(CONTRACT_FILE.read_text(encoding="utf-8"))
print(f"Contract file: {CONTRACT_FILE}")
print(f"version: {contract.get('version')} | profile: {contract.get('profile')}")

rows = []
for name, cfg in contract.get("datasets", {}).items():
    rows.append(
        {
            "dataset": name,
            "enabled": cfg.get("enabled"),
            "priority": cfg.get("priority"),
            "target_path": cfg.get("target_path"),
            "expected_file_name": cfg.get("expected_file_name"),
            "partition_pattern": cfg.get("partition_pattern"),
            "min_cols": ", ".join(cfg.get("minimum_required_columns", [])),
            "min_rows_per_file": cfg.get("min_rows_per_file"),
            "coverage_unit": cfg.get("coverage", {}).get("coverage_unit"),
            "success_criteria": cfg.get("coverage", {}).get("success_criteria"),
        }
    )

summary_df = pd.DataFrame(rows).sort_values(["priority", "dataset"]).reset_index(drop=True)
try:
    display(summary_df)
except Exception:
    print(summary_df.to_string(index=False))

print(f"\nDetalle dataset: {DATASET_TO_VIEW}")
if DATASET_TO_VIEW in contract.get("datasets", {}):
    print(json.dumps(contract["datasets"][DATASET_TO_VIEW], indent=2, ensure_ascii=False))
else:
    print(f"Dataset not found in contract: {DATASET_TO_VIEW}")
