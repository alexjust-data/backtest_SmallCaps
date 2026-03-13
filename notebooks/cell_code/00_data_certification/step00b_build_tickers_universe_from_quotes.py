from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(r"C:/TSIS_Data/v1/backtest_SmallCaps")
QUOTES_ROOT = Path(r"C:/TSIS_Data/data/quotes_p95")
OUT_TICKERS_FILE = PROJECT_ROOT / "data" / "reference" / "tickers_universe.txt"


def main() -> None:
    if not QUOTES_ROOT.exists():
        raise FileNotFoundError(f"No existe quotes root: {QUOTES_ROOT}")

    tickers = sorted(
        {
            p.name.strip().upper()
            for p in QUOTES_ROOT.iterdir()
            if p.is_dir() and p.name.strip()
        }
    )
    if not tickers:
        raise ValueError(f"No se encontraron carpetas ticker en: {QUOTES_ROOT}")

    OUT_TICKERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_TICKERS_FILE.write_text("\n".join(tickers) + "\n", encoding="utf-8")

    print(f"tickers_universe generado: {OUT_TICKERS_FILE}")
    print(f"tickers total: {len(tickers)}")
    print(f"primeros 10: {tickers[:10]}")


if __name__ == "__main__":
    main()

