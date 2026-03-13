$ErrorActionPreference = "Stop"

$Python = "C:\TSIS_Data\v1\backtest_SmallCaps\backtest\Scripts\python.exe"
$Script = "C:\TSIS_Data\v1\backtest_SmallCaps\notebooks\cell_code\build_smoke_ohlcv_1m_input.py"

Write-Host "Building smoke input..."
& $Python $Script

if ($LASTEXITCODE -ne 0) {
    throw "Smoke input build failed with exit code $LASTEXITCODE"
}
