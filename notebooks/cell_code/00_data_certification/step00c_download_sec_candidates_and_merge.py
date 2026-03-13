from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps")
PYTHON_EXE = PROJECT_ROOT / "backtest" / "Scripts" / "python.exe"
SEC_SCRIPT = PROJECT_ROOT / "scripts" / "download_official_events_sec.py"

TICKERS_FILE = PROJECT_ROOT / "data" / "reference" / "tickers_universe.txt"
OUT_CANDIDATES_CSV = PROJECT_ROOT / "data" / "reference" / "official_ticker_events.sec_candidates.csv"
MERGE_INTO_OFFICIAL_CSV = PROJECT_ROOT / "data" / "reference" / "official_ticker_events.csv"

# Cambia esto por tu email real para cumplir política SEC.
USER_AGENT = "AlexJ tu_email_real@dominio.com"
TIMEOUT_SEC = 30
SLEEP_MS = 120
MAX_TICKERS = 0  # 0 = todos
REQUIRE_PRIMARY_DOC = False


def _python_cmd() -> Path:
    if PYTHON_EXE.exists():
        return PYTHON_EXE
    return Path(sys.executable)


def main() -> None:
    if not SEC_SCRIPT.exists():
        raise FileNotFoundError(f"No existe script SEC: {SEC_SCRIPT}")
    if not TICKERS_FILE.exists():
        raise FileNotFoundError(f"No existe tickers file: {TICKERS_FILE}")

    cmd = [
        str(_python_cmd()),
        str(SEC_SCRIPT),
        "--tickers-file",
        str(TICKERS_FILE),
        "--out-candidates-csv",
        str(OUT_CANDIDATES_CSV),
        "--merge-into-official-csv",
        str(MERGE_INTO_OFFICIAL_CSV),
        "--user-agent",
        USER_AGENT,
        "--timeout-sec",
        str(TIMEOUT_SEC),
        "--sleep-ms",
        str(SLEEP_MS),
        "--max-tickers",
        str(MAX_TICKERS),
    ]
    if REQUIRE_PRIMARY_DOC:
        cmd.append("--require-primary-doc")

    print("[00C] Ejecutando descarga SEC + merge...")
    print("[00C] Comando:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))
    print(f"[00C] Guardado candidates: {OUT_CANDIDATES_CSV}")
    print(f"[00C] Guardado official merged: {MERGE_INTO_OFFICIAL_CSV}")


if __name__ == "__main__":
    main()

