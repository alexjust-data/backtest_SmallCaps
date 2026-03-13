from __future__ import annotations

import runpy
import sys
from pathlib import Path


BASE_SCRIPT = Path(__file__).with_name("download_ohlcv_minute_v1.py")


def main() -> None:
    if not BASE_SCRIPT.exists():
        raise SystemExit(f"No existe script base: {BASE_SCRIPT}")

    # Forzamos REST-only eliminando cualquier --source previo.
    raw = sys.argv[1:]
    args: list[str] = []
    i = 0
    while i < len(raw):
        a = raw[i]
        if a == "--source":
            # Si viene sin valor, no consumimos el siguiente argumento legítimo.
            if i + 1 < len(raw) and not raw[i + 1].startswith("-"):
                i += 2
            else:
                i += 1
            continue
        if a.startswith("--source="):
            i += 1
            continue
        args.append(a)
        i += 1
    args += ["--source", "rest"]

    old_argv = sys.argv
    try:
        sys.argv = [str(BASE_SCRIPT)] + args
        runpy.run_path(str(BASE_SCRIPT), run_name="__main__")
    finally:
        sys.argv = old_argv


if __name__ == "__main__":
    main()
