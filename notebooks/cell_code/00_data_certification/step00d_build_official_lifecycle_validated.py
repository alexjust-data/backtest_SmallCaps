from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps")
PYTHON_EXE = PROJECT_ROOT / "backtest" / "Scripts" / "python.exe"
LIFECYCLE_SCRIPT = PROJECT_ROOT / "scripts" / "build_official_lifecycle.py"

EVENTS_CSV = PROJECT_ROOT / "data" / "reference" / "official_ticker_events.csv"
OUT_LIFECYCLE_CSV = PROJECT_ROOT / "data" / "reference" / "official_lifecycle_compiled.csv"
OUT_EVENTS_PARQUET = PROJECT_ROOT / "data" / "reference" / "official_ticker_events.validated.parquet"
OUT_LIFECYCLE_PARQUET = PROJECT_ROOT / "data" / "reference" / "official_lifecycle_compiled.parquet"
OUT_VALIDATION_JSON = PROJECT_ROOT / "data" / "reference" / "official_lifecycle_validation_report.json"

ENFORCE_OFFICIAL_DOMAINS = False
ALLOWED_DOMAINS = ""  # ej: "sec.gov,www.sec.gov,nasdaq.com,www.nasdaq.com,nyse.com,www.nyse.com,ftp.nyse.com"


def _python_cmd() -> Path:
    if PYTHON_EXE.exists():
        return PYTHON_EXE
    return Path(sys.executable)


def main() -> None:
    if not LIFECYCLE_SCRIPT.exists():
        raise FileNotFoundError(f"No existe script lifecycle: {LIFECYCLE_SCRIPT}")
    if not EVENTS_CSV.exists():
        raise FileNotFoundError(f"No existe events csv: {EVENTS_CSV}")

    cmd = [
        str(_python_cmd()),
        str(LIFECYCLE_SCRIPT),
        "--events-csv",
        str(EVENTS_CSV),
        "--out-lifecycle-csv",
        str(OUT_LIFECYCLE_CSV),
        "--out-events-parquet",
        str(OUT_EVENTS_PARQUET),
        "--out-lifecycle-parquet",
        str(OUT_LIFECYCLE_PARQUET),
        "--out-validation-json",
        str(OUT_VALIDATION_JSON),
    ]
    if ENFORCE_OFFICIAL_DOMAINS:
        cmd.append("--enforce-official-domains")
        if ALLOWED_DOMAINS.strip():
            cmd.extend(["--allowed-domains", ALLOWED_DOMAINS.strip()])

    print("[00D] Ejecutando validacion + build lifecycle...")
    print("[00D] Comando:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))
    print(f"[00D] Guardado lifecycle csv: {OUT_LIFECYCLE_CSV}")
    print(f"[00D] Guardado validation json: {OUT_VALIDATION_JSON}")


if __name__ == "__main__":
    main()

