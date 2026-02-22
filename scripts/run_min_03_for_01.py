import json
import os
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps")
NB_PATH = PROJECT_ROOT / "notebooks" / "01_data_integrity" / "03_time_coverage.ipynb"

# Importante: cwd en carpeta del notebook para que PROJECT_ROOT=Path.cwd().parents[1] funcione
os.chdir(PROJECT_ROOT / "notebooks" / "01_data_integrity")

nb = json.loads(NB_PATH.read_text(encoding="utf-8"))

# Solo lo necesario para generar cmp5_robust.parquet
needed_cells = [2, 3, 4, 6, 8, 12, 16, 18, 20, 22, 61, 62, 63, 69, 70, 72]

# Entorno compartido entre celdas
ns = {}
ns["display"] = lambda x: print(x)

for idx in needed_cells:
    code = "".join(nb["cells"][idx].get("source", []))
    if not code.strip():
        print(f"[SKIP EMPTY] cell {idx}")
        continue

    print(f"\n=== RUN cell {idx} ===")
    try:
        # Celda 72 requiere OUT; si no existe lo bootstrapeamos
        if idx == 72 and "OUT" not in ns:
            note = ns.get("NOTEBOOK_ID", "03_time_coverage")
            runs_dir = ns.get("RUNS_DIR", PROJECT_ROOT / "runs")
            ns["OUT"] = Path(runs_dir) / "data_quality" / note / datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            ns["OUT"].mkdir(parents=True, exist_ok=True)

        exec(code, ns, ns)
        print(f"[OK] cell {idx}")
    except Exception as e:
        print(f"[FAIL] cell {idx}: {type(e).__name__}: {e}")
        print(traceback.format_exc())
        raise

print("\nDONE. Verificando artifact robusto...")

artifact = None
if "OUT" in ns:
    p = Path(ns["OUT"]) / "multi_ticker_5_robust" / "cmp5_robust.parquet"
    if p.exists():
        artifact = p

if artifact is None:
    root03 = PROJECT_ROOT / "runs" / "data_quality" / "03_time_coverage"
    cands = sorted(
        [p for p in root03.glob("*") if (p / "multi_ticker_5_robust" / "cmp5_robust.parquet").exists()],
        key=lambda p: p.name,
    )
    if cands:
        artifact = cands[-1] / "multi_ticker_5_robust" / "cmp5_robust.parquet"

if artifact and artifact.exists():
    print("ARTIFACT OK:", artifact)
else:
    print("ARTIFACT NOT FOUND: cmp5_robust.parquet")
    raise SystemExit(1)
