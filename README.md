# Trading Backtester (Quant-grade, reproducible)

## Setup
1) Create venv & install deps (uv)
- Install uv: https://github.com/astral-sh/uv
- Then:

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
cp .env.example .env
# backtest_SmallCaps
