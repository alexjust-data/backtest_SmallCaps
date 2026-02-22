
import json
from pathlib import Path

nb_path = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\01_data_integrity\03_time_coverage.ipynb")
nb = json.loads(nb_path.read_text(encoding="utf-8"))
code = nb["cells"][78]["source"]
if isinstance(code, list):
    code = "".join(code)

ns = {"MAX_TICKERS": None}
exec(code, ns, ns)
print("DONE_03_MASSIVE_V2")
