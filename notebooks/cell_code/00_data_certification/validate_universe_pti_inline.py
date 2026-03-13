from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.dataset as ds
from IPython.display import Markdown, display


DEFAULT_OUTDIR = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti")


def _md(title: str) -> None:
    display(Markdown(f"## {title}"))


def _md_table(df: pd.DataFrame, max_rows: int = 20) -> None:
    show = df.head(max_rows).copy()
    try:
        display(Markdown(show.to_markdown(index=False)))
    except Exception:
        # Fallback when optional dependency 'tabulate' is not installed.
        display(show)


def _pct(num: int, den: int) -> float:
    return round((100.0 * num / den), 2) if den else 0.0


def run_validation(outdir: Path) -> None:
    panel_dir = outdir / "tickers_panel_pti"
    all_p = outdir / "tickers_all.parquet"
    qa_p = outdir / "qa_coverage_by_cut.csv"
    progress_p = outdir / "build_universe_pti.progress.json"
    meta_p = outdir / "build_universe_pti.meta.json"

    panel = ds.dataset(str(panel_dir), format="parquet").to_table(
        columns=[
            "snapshot_date",
            "ticker",
            "primary_exchange",
            "entity_id",
            "composite_figi",
            "share_class_figi",
            "list_date",
            "active",
        ]
    ).to_pandas()
    panel["snapshot_date"] = pd.to_datetime(panel["snapshot_date"]).dt.date
    panel["has_figi"] = panel["composite_figi"].notna() | panel["share_class_figi"].notna()
    panel["fallback_entity"] = panel["entity_id"].astype(str).str.contains(r"\|", regex=True)

    qa = pd.read_csv(qa_p)
    qa["snapshot_date"] = pd.to_datetime(qa["snapshot_date"]).dt.date
    all_df = pd.read_parquet(all_p)

    # Resumen general
    _md("Resumen General")
    n_rows = len(panel)
    n_snaps = panel["snapshot_date"].nunique()
    summary = pd.DataFrame(
        [
            {
                "panel_rows": n_rows,
                "panel_snapshots": n_snaps,
                "min_snapshot": panel["snapshot_date"].min(),
                "max_snapshot": panel["snapshot_date"].max(),
                "tickers_all_rows": len(all_df),
                "qa_rows": len(qa),
                "qa_max_snapshot": qa["snapshot_date"].max(),
            }
        ]
    )
    _md_table(summary)

    # Integridad
    _md("Integridad de Entidad")
    dup_snap_entity = int(panel.duplicated(["snapshot_date", "entity_id"]).sum())
    dup_snap_composite = int(
        panel[panel["composite_figi"].notna()].duplicated(["snapshot_date", "composite_figi"]).sum()
    )
    null_entity = int(panel["entity_id"].isna().sum())
    null_comp = int(panel["composite_figi"].isna().sum())
    null_share = int(panel["share_class_figi"].isna().sum())
    fallback_rows = int(panel["fallback_entity"].sum())
    integ = pd.DataFrame(
        [
            {"metric": "dup(snapshot_date, entity_id)", "value": dup_snap_entity},
            {"metric": "dup(snapshot_date, composite_figi) no-nulls", "value": dup_snap_composite},
            {"metric": "null_entity_id", "value": null_entity},
            {"metric": "null_composite_figi", "value": f"{null_comp} ({_pct(null_comp, n_rows)}%)"},
            {"metric": "null_share_class_figi", "value": f"{null_share} ({_pct(null_share, n_rows)}%)"},
            {"metric": "rows con fallback entity_id ticker|exchange", "value": f"{fallback_rows} ({_pct(fallback_rows, n_rows)}%)"},
        ]
    )
    _md_table(integ, max_rows=50)

    # Exchange stats
    _md("Estadisticas por Exchange (Ultimo Snapshot)")
    last_d = panel["snapshot_date"].max()
    last = panel[panel["snapshot_date"] == last_d].copy()
    by_ex_last = (
        last.groupby("primary_exchange", dropna=False)
        .agg(
            rows=("entity_id", "size"),
            entities=("entity_id", "nunique"),
            with_figi=("has_figi", "sum"),
            fallback_rows=("fallback_entity", "sum"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    by_ex_last["pct_with_figi"] = (100.0 * by_ex_last["with_figi"] / by_ex_last["rows"]).round(2)
    by_ex_last["pct_fallback"] = (100.0 * by_ex_last["fallback_rows"] / by_ex_last["rows"]).round(2)
    _md_table(by_ex_last, max_rows=20)

    # Atributos nulos en ultimo snapshot
    _md("Calidad de Atributos (Ultimo Snapshot)")
    attrs = ["cik", "list_date", "composite_figi", "share_class_figi"]
    attr_stats = []
    for c in attrs:
        nulls = int(last[c].isna().sum()) if c in last.columns else len(last)
        attr_stats.append({"attribute": c, "nulls": nulls, "pct_null": round(100.0 * nulls / len(last), 2)})
    attr_df = pd.DataFrame(attr_stats).sort_values("pct_null", ascending=False)
    _md_table(attr_df, max_rows=20)

    # Coverage by day
    by_day = panel.groupby("snapshot_date", as_index=False).agg(
        rows=("entity_id", "size"),
        entities=("entity_id", "nunique"),
        with_figi=("has_figi", "sum"),
    )
    by_day["pct_with_figi"] = (100.0 * by_day["with_figi"] / by_day["rows"]).round(2)

    _md("Cobertura Temporal (muestra)")
    _md_table(pd.concat([by_day.head(5), by_day.tail(5)]), max_rows=10)

    # Graficos inline
    _md("Graficos Inline")
    plt.figure(figsize=(12, 4))
    plt.plot(by_day["snapshot_date"], by_day["entities"], linewidth=1.2)
    plt.title("Entidades por Snapshot (diario)")
    plt.xlabel("Fecha")
    plt.ylabel("N entidades")
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 4))
    plt.plot(by_day["snapshot_date"], by_day["pct_with_figi"], linewidth=1.2)
    plt.title("% Filas con FIGI (composite o share_class)")
    plt.xlabel("Fecha")
    plt.ylabel("% con FIGI")
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(8, 4))
    plt.bar(by_ex_last["primary_exchange"].astype(str), by_ex_last["rows"])
    plt.title(f"Rows por Exchange (ultimo snapshot: {last_d})")
    plt.xlabel("Exchange")
    plt.ylabel("Rows")
    plt.tight_layout()
    plt.show()

    # Estado final para decision de avance
    _md("Gate de Verificacion")
    checks = pd.DataFrame(
        [
            {"check": "panel_snapshots == qa_rows", "ok": n_snaps == len(qa)},
            {"check": "max(panel) == max(qa)", "ok": panel["snapshot_date"].max() == qa["snapshot_date"].max()},
            {"check": "dup(snapshot_date,entity_id) == 0", "ok": dup_snap_entity == 0},
            {"check": "null_entity_id == 0", "ok": null_entity == 0},
        ]
    )
    _md_table(checks, max_rows=20)

    if checks["ok"].all():
        display(Markdown("**Estado:** VALIDACION ESTRUCTURAL OK (puedes avanzar a siguiente fase)."))
    else:
        display(Markdown("**Estado:** VALIDACION ESTRUCTURAL NO OK (corregir antes de avanzar)."))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    args, _ = ap.parse_known_args()
    run_validation(Path(args.outdir))


if __name__ == "__main__":
    main()
