from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps")
PYTHON_EXE = PROJECT_ROOT / "backtest" / "Scripts" / "python.exe"
RECON_SCRIPT = PROJECT_ROOT / "scripts" / "reconcile_official_events_multisource.py"

TICKERS_FILE = PROJECT_ROOT / "data" / "reference" / "tickers_universe.txt"
SEC_EVENTS_CSV = PROJECT_ROOT / "data" / "reference" / "official_ticker_events.csv"

# Deposita aquí los ficheros oficiales cuando los tengas:
# - NASDAQ Daily List (licenciado/sftp normalmente)
# - NYSE/CTA symbol files
NASDAQ_GLOB = "data/external/nasdaq/**/*.*"
NYSE_GLOB = "data/external/nyse_cta/**/*.*"

ENFORCE_OFFICIAL_DOMAINS = False
ALLOWED_DOMAINS = ""

RUN_ROOT = PROJECT_ROOT / "runs" / "data_quality" / "00_data_certification" / "official_multisource"
RUN_DIR = RUN_ROOT / f"step00e_official_multisource_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
RUN_DIR.mkdir(parents=True, exist_ok=True)

OUT_EVENTS_CSV = RUN_DIR / "official_ticker_events.multisource.validated.csv"
OUT_EVENTS_PARQUET = RUN_DIR / "official_ticker_events.multisource.validated.parquet"
OUT_LIFECYCLE_CSV = RUN_DIR / "official_lifecycle_compiled.multisource.csv"
OUT_LIFECYCLE_PARQUET = RUN_DIR / "official_lifecycle_compiled.multisource.parquet"
OUT_VALIDATION_JSON = RUN_DIR / "official_multisource_validation_report.json"
OUT_COVERAGE_CSV = RUN_DIR / "official_multisource_coverage_summary.csv"
OUT_GAPS_CSV = RUN_DIR / "official_multisource_gaps_by_ticker.csv"
OUT_SUMMARY_JSON = RUN_DIR / "official_multisource_summary.json"


def _python_cmd() -> Path:
    if PYTHON_EXE.exists():
        return PYTHON_EXE
    return Path(sys.executable)


def main() -> None:
    if not RECON_SCRIPT.exists():
        raise FileNotFoundError(f"No existe script reconciliador: {RECON_SCRIPT}")
    if not TICKERS_FILE.exists():
        raise FileNotFoundError(f"No existe tickers file: {TICKERS_FILE}")
    if not SEC_EVENTS_CSV.exists():
        raise FileNotFoundError(f"No existe SEC events CSV: {SEC_EVENTS_CSV}")

    cmd = [
        str(_python_cmd()),
        str(RECON_SCRIPT),
        "--tickers-file",
        str(TICKERS_FILE),
        "--sec-events-csv",
        str(SEC_EVENTS_CSV),
        "--nasdaq-glob",
        NASDAQ_GLOB,
        "--nyse-glob",
        NYSE_GLOB,
        "--out-events-csv",
        str(OUT_EVENTS_CSV),
        "--out-events-parquet",
        str(OUT_EVENTS_PARQUET),
        "--out-lifecycle-csv",
        str(OUT_LIFECYCLE_CSV),
        "--out-lifecycle-parquet",
        str(OUT_LIFECYCLE_PARQUET),
        "--out-validation-json",
        str(OUT_VALIDATION_JSON),
        "--out-coverage-csv",
        str(OUT_COVERAGE_CSV),
        "--out-gaps-csv",
        str(OUT_GAPS_CSV),
        "--out-summary-json",
        str(OUT_SUMMARY_JSON),
    ]
    if ENFORCE_OFFICIAL_DOMAINS:
        cmd.append("--enforce-official-domains")
        if ALLOWED_DOMAINS.strip():
            cmd.extend(["--allowed-domains", ALLOWED_DOMAINS.strip()])

    print("[00E] Ejecutando reconciliacion multisource...")
    print("[00E] Comando:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))

    summary = json.loads(OUT_SUMMARY_JSON.read_text(encoding="utf-8"))
    print("[00E] RUN_DIR:", RUN_DIR)
    print("[00E] coverage_summary:", summary.get("coverage_summary", {}))
    print("[00E] outputs:", summary.get("outputs", {}))


if __name__ == "__main__":
    main()

