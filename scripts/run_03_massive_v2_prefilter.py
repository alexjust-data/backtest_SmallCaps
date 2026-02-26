import json
import os
from pathlib import Path

PROJECT_ROOT = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps")
NB_PATH = PROJECT_ROOT / "notebooks" / "01_data_integrity" / "03_time_coverage.ipynb"
TARGET_CELL = 78  # massive v2 orchestration cell (with prefilter fix)

# Keep cwd aligned with notebook assumptions
os.chdir(PROJECT_ROOT / "notebooks" / "01_data_integrity")

nb = json.loads(NB_PATH.read_text(encoding="utf-8"))
code = nb["cells"][TARGET_CELL]["source"]
if isinstance(code, list):
    code = "".join(code)

# MAX_TICKERS can be overridden from env for dry-runs
max_tickers_env = os.getenv("MAX_TICKERS")
max_tickers = None
if max_tickers_env not in (None, "", "None"):
    try:
        max_tickers = int(max_tickers_env)
    except ValueError:
        raise ValueError(f"Invalid MAX_TICKERS env value: {max_tickers_env}")

ns = {"MAX_TICKERS": max_tickers}
exec(code, ns, ns)
print("DONE_03_MASSIVE_V2_PREFILTER")
