from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds


DEFAULT_OUTDIR = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti")


def pct(x: int, n: int) -> float:
    return round((100.0 * x / n), 2) if n else 0.0


def run_study(outdir: Path) -> dict:
    panel_dir = outdir / "tickers_panel_pti"
    all_p = outdir / "tickers_all.parquet"
    qa_p = outdir / "qa_coverage_by_cut.csv"

    panel = ds.dataset(str(panel_dir), format="parquet").to_table(
        columns=[
            "snapshot_date",
            "entity_id",
            "ticker",
            "primary_exchange",
            "composite_figi",
            "share_class_figi",
            "active",
        ]
    ).to_pandas()
    panel["snapshot_date"] = pd.to_datetime(panel["snapshot_date"]).dt.date

    d_all = pd.read_parquet(all_p)
    qa = pd.read_csv(qa_p)

    n_rows = len(panel)
    n_snaps = panel["snapshot_date"].nunique()
    min_d = panel["snapshot_date"].min()
    max_d = panel["snapshot_date"].max()

    dup_snap_entity = int(panel.duplicated(["snapshot_date", "entity_id"]).sum())
    null_entity = int(panel["entity_id"].isna().sum())
    null_composite = int(panel["composite_figi"].isna().sum())
    null_share = int(panel["share_class_figi"].isna().sum())

    has_any_figi = panel["composite_figi"].notna() | panel["share_class_figi"].notna()
    n_has_figi = int(has_any_figi.sum())

    by_day = panel.groupby("snapshot_date", as_index=False).agg(
        rows=("entity_id", "size"),
        entities=("entity_id", "nunique"),
        null_comp=("composite_figi", lambda s: int(s.isna().sum())),
        null_share=("share_class_figi", lambda s: int(s.isna().sum())),
    )
    by_day["pct_no_composite"] = by_day["null_comp"] / by_day["rows"] * 100.0

    last = panel[panel["snapshot_date"] == max_d]
    ex = (
        last["primary_exchange"]
        .fillna("UNK")
        .value_counts(dropna=False)
        .rename_axis("primary_exchange")
        .reset_index(name="rows_last_snapshot")
    )

    qa_rows = len(qa)
    qa_max = pd.to_datetime(qa["snapshot_date"]).dt.date.max()

    print("=== RESUMEN GENERAL ===")
    print(f"panel_rows={n_rows:,}")
    print(f"snapshots={n_snaps:,}  min={min_d}  max={max_d}")
    print(f"tickers_all_rows={len(d_all):,}")
    print()
    print("=== INTEGRIDAD ENTIDAD ===")
    print(f"dup(snapshot_date,entity_id)={dup_snap_entity:,}")
    print(f"null_entity_id={null_entity:,}")
    print(f"null_composite_figi={null_composite:,} ({pct(null_composite, n_rows)}%)")
    print(f"null_share_class_figi={null_share:,} ({pct(null_share, n_rows)}%)")
    print(f"rows_with_any_figi={n_has_figi:,} ({pct(n_has_figi, n_rows)}%)")
    print()
    print("=== QA CONSISTENCY ===")
    print(f"qa_rows={qa_rows:,}  qa_max={qa_max}")
    print(f"panel_snapshots={n_snaps:,}  panel_max={max_d}")
    print()
    print("=== EXCHANGE MIX (ULTIMO SNAPSHOT) ===")
    print(ex.head(10).to_string(index=False))
    print()
    print("=== MUESTRA COBERTURA DIARIA ===")
    print(by_day.head(5).to_string(index=False))
    print("...")
    print(by_day.tail(5).to_string(index=False))

    study_dir = outdir / "study_outputs"
    study_dir.mkdir(exist_ok=True)
    by_day.to_csv(study_dir / "study_by_day.csv", index=False)
    ex.to_csv(study_dir / "study_exchange_mix_last_snapshot.csv", index=False)

    summary = pd.DataFrame(
        [
            {
                "panel_rows": n_rows,
                "snapshots": n_snaps,
                "min_snapshot": min_d,
                "max_snapshot": max_d,
                "dup_snapshot_entity": dup_snap_entity,
                "null_entity_id": null_entity,
                "pct_null_composite_figi": pct(null_composite, n_rows),
                "pct_null_share_class_figi": pct(null_share, n_rows),
                "pct_rows_with_any_figi": pct(n_has_figi, n_rows),
                "qa_rows": qa_rows,
                "qa_max_snapshot": qa_max,
            }
        ]
    )
    summary_path = study_dir / "study_summary.csv"
    summary.to_csv(summary_path, index=False)
    print()
    print(f"saved: {summary_path}")

    return {
        "study_dir": str(study_dir),
        "summary_path": str(summary_path),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    args, _ = ap.parse_known_args()
    run_study(Path(args.outdir))


if __name__ == "__main__":
    main()

