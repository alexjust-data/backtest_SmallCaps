from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

DATASETS = {
    "balance_sheets",
    "cash_flow_statements",
    "income_statements",
    "ratios",
}
META_COLS = {"_dataset", "_ingested_utc", "_empty"}


def load_expected_schema() -> Dict[str, Set[str]]:
    try:
        from massive.rest.models.financials import (
            FinancialBalanceSheet,
            FinancialCashFlowStatement,
            FinancialIncomeStatement,
            FinancialRatio,
        )

        return {
            "balance_sheets": set(FinancialBalanceSheet.__annotations__.keys()),
            "cash_flow_statements": set(
                FinancialCashFlowStatement.__annotations__.keys()
            ),
            "income_statements": set(
                FinancialIncomeStatement.__annotations__.keys()
            ),
            "ratios": set(FinancialRatio.__annotations__.keys()),
        }
    except Exception:
        return {k: set() for k in DATASETS}


def parse_checks(checks_csv: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for chunk in checks_csv.split(","):
        c = chunk.strip()
        if not c:
            continue
        if ":" not in c:
            raise ValueError(
                f"Formato invalido '{c}'. Usa dataset:ticker, por ejemplo ratios:AAPL"
            )
        dataset, ticker = [x.strip() for x in c.split(":", 1)]
        if dataset not in DATASETS:
            raise ValueError(f"Dataset no soportado: {dataset}")
        if not ticker:
            raise ValueError(f"Ticker vacio en '{c}'")
        out.append((dataset, ticker.upper()))
    return out


def resolve_file(base_dirs: List[Path], dataset: str, ticker: str) -> Optional[Path]:
    file_old = f"{ticker}.parquet"
    file_new = f"{dataset}_{ticker}.parquet"

    for base in base_dirs:
        ds_base = base / dataset
        p_old = ds_base / file_old
        if p_old.exists():
            return p_old

        p_new_flat = ds_base / file_new
        if p_new_flat.exists():
            return p_new_flat

        p_new_partition = ds_base / f"ticker={ticker}" / file_new
        if p_new_partition.exists():
            return p_new_partition
    return None


def inspect_file(
    file_path: Path,
    dataset: str,
    expected: Set[str],
    top_null: int,
) -> Dict:
    df = pd.read_parquet(file_path)
    cols = [str(c) for c in df.columns]
    data_cols = [c for c in cols if c not in META_COLS]

    is_empty_placeholder = (
        "_empty" in df.columns and len(df) > 0 and df["_empty"].fillna(False).all()
    )

    present_expected = sorted(set(data_cols) & expected) if expected else []
    missing_expected = sorted(expected - set(data_cols)) if expected else []
    extra_cols = sorted(set(data_cols) - expected) if expected else []

    null_top = {}
    if len(df) > 0 and data_cols:
        series = df[data_cols].isna().mean().sort_values(ascending=False).head(top_null)
        null_top = {k: round(float(v) * 100.0, 2) for k, v in series.items()}

    return {
        "dataset": dataset,
        "file": str(file_path),
        "rows": int(len(df)),
        "cols_total": int(len(cols)),
        "cols_all": cols,
        "cols_data": data_cols,
        "empty_placeholder": bool(is_empty_placeholder),
        "expected_fields": int(len(expected)),
        "present_expected": int(len(present_expected)),
        "missing_expected": int(len(missing_expected)),
        "missing_list": missing_expected,
        "extra_list": extra_cols,
        "top_null_pct": null_top,
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Audita atributos descargados por endpoint fundamentals"
    )
    ap.add_argument(
        "--checks",
        required=True,
        help=(
            "Lista CSV dataset:ticker. "
            "Ej: balance_sheets:AAC,cash_flow_statements:AAC,income_statements:AAC,ratios:AABA"
        ),
    )
    ap.add_argument(
        "--roots",
        default=r"C:\TSIS_Data\data\fundamentals,D:\financial",
        help="Raices de busqueda separadas por coma",
    )
    ap.add_argument(
        "--top-null",
        type=int,
        default=15,
        help="Top N columnas por porcentaje de nulos",
    )
    ap.add_argument(
        "--out-json",
        default="",
        help="Ruta opcional para guardar reporte JSON",
    )
    args = ap.parse_args()

    checks = parse_checks(args.checks)
    roots = [Path(x.strip()) for x in args.roots.split(",") if x.strip()]
    expected_map = load_expected_schema()

    report: List[Dict] = []
    for dataset, ticker in checks:
        fp = resolve_file(roots, dataset, ticker)
        if fp is None:
            report.append(
                {
                    "dataset": dataset,
                    "ticker": ticker,
                    "file": None,
                    "error": "file_not_found",
                }
            )
            continue

        item = inspect_file(
            file_path=fp,
            dataset=dataset,
            expected=expected_map.get(dataset, set()),
            top_null=args.top_null,
        )
        item["ticker"] = ticker
        report.append(item)

    print(json.dumps(report, indent=2, ensure_ascii=False))

    if args.out_json:
        out = Path(args.out_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nSaved report: {out}")


if __name__ == "__main__":
    main()
