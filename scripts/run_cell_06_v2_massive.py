
import json
from pathlib import Path
nb = json.loads(Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\01_data_integrity\06_ohlcv_vs_quotes.ipynb").read_text(encoding="utf-8"))
code = nb["cells"][62]["source"]
if isinstance(code, list):
    code = "".join(code)
ns = {"MAX_TICKERS": None}
exec(code, ns, ns)
print("DONE_06_V2_MASSIVE")
