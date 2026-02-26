from __future__ import annotations

from pathlib import Path

from backtest03_v3.contracts import default_config
from backtest03_v3.orchestrator import run_prefilter_v3


def main() -> None:
    project_root = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps")
    cfg = default_config(project_root)
    out_dir = run_prefilter_v3(cfg)
    print("RUN 03 v3 multi-era completed")
    print("out_dir:", out_dir)


if __name__ == "__main__":
    main()
