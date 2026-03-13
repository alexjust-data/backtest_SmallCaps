from pathlib import Path
import pandas as pd

DATA_ROOT = Path(r'C:\TSIS_Data\data')
OUT_ROOT = Path(r'C:\TSIS_Data\v1\backtest_SmallCaps\runs\backtest\02_policy_integration\coverage_radiography')
OUT_ROOT.mkdir(parents=True, exist_ok=True)

TICKERS = ['AUY','SWN','DWDP','DNR','UAA','ATVI','MRO','ECA','FSR','JBLU','PE','CLOV','WPX','FDC','AKS','CDEV','WORK','NBL']

print('Setup OK')
print('DATA_ROOT:', DATA_ROOT)
print('Tickers:', len(TICKERS))
