from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

EXAMPLES = {
    "balance_sheets": Path(r"C:\TSIS_Data\data\fundamentals\balance_sheets\AAC.parquet"),
    "cash_flow_statements": Path(r"C:\TSIS_Data\data\fundamentals\cash_flow_statements\AAC.parquet"),
    "income_statements": Path(r"C:\TSIS_Data\data\fundamentals\income_statements\AAC.parquet"),
    "ratios": Path(r"D:\financial\ratios\ticker=AABA\ratios_AABA.parquet"),
}


def print_one(dataset: str, rows: int) -> None:
    path = EXAMPLES[dataset]
    print("=" * 110)
    print(f"DATASET: {dataset}")
    print(f"FILE: {path}")

    if not path.exists():
        print("ERROR: file not found")
        return

    df = pd.read_parquet(path)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 220)
    pd.set_option("display.max_colwidth", 120)

    print(f"SHAPE: rows={len(df)} cols={len(df.columns)}")
    print(f"COLUMNS ({len(df.columns)}): {list(df.columns)}")
    print(f"\nHEAD({rows}):")
    print(df.head(rows).to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Imprime ejemplos de parquets fundamentals por endpoint"
    )
    ap.add_argument(
        "--dataset",
        default="all",
        choices=["all", "balance_sheets", "cash_flow_statements", "income_statements", "ratios"],
        help="Endpoint a imprimir. Usa 'all' para todos.",
    )
    ap.add_argument("--rows", type=int, default=5, help="Numero de filas a mostrar")
    args = ap.parse_args()

    if args.dataset == "all":
        for ds in ("balance_sheets", "cash_flow_statements", "income_statements", "ratios"):
            print_one(ds, args.rows)
    else:
        print_one(args.dataset, args.rows)


if __name__ == "__main__":
    main()
