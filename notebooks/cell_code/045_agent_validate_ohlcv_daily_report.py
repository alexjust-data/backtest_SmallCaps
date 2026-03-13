from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd


def _parse_list_like(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    s = str(v).strip()
    if s in {"", "[]", "nan", "None"}:
        return []
    try:
        out = ast.literal_eval(s)
        if isinstance(out, list):
            return [str(x) for x in out]
    except Exception:
        pass
    return [s]


def _latest_valid_run(base_dir: Path) -> Path:
    runs = [p for p in base_dir.glob("*") if (p / "ohlcv_daily_events_current.csv").exists()]
    if not runs:
        raise FileNotFoundError(
            f"No hay runs con ohlcv_daily_events_current.csv en {base_dir}. Ejecuta primero el auditor 044."
        )
    return sorted(runs, key=lambda p: (p / "ohlcv_daily_events_current.csv").stat().st_mtime, reverse=True)[0]


def _severity_rank(s: str) -> int:
    s = str(s).upper()
    if s == "HARD_FAIL":
        return 3
    if s == "SOFT_FAIL":
        return 2
    if s == "WARN":
        return 1
    return 0


def build_report(
    run_dir: Path,
    soft_fail_warn_threshold_pct: float = 1.0,
    enforce_zero_missing_expected: bool = True,
    show_plots: bool = True,
) -> None:
    events_p = run_dir / "ohlcv_daily_events_current.csv"
    retry_p = run_dir / "retry_queue_ohlcv_daily_current.csv"
    live_p = run_dir / "live_status_ohlcv_daily.json"

    if not events_p.exists():
        raise FileNotFoundError(f"No existe events csv: {events_p}")

    ev = pd.read_csv(events_p)
    rq = pd.read_csv(retry_p) if retry_p.exists() else pd.DataFrame()
    live = json.loads(live_p.read_text(encoding="utf-8")) if live_p.exists() else {}

    if ev.empty:
        print("RUN_DIR:", run_dir)
        print("events_rows: 0")
        print("No hay eventos para analizar.")
        return

    for c in ["severity", "issues", "warns", "action", "rows", "ticker_path", "year_path"]:
        if c not in ev.columns:
            ev[c] = None

    ev["rows"] = pd.to_numeric(ev["rows"], errors="coerce").fillna(0).astype(int)
    ev["severity"] = ev["severity"].astype("string").fillna("UNKNOWN")
    ev["action"] = ev["action"].astype("string").fillna("UNKNOWN")
    ev["ticker_path"] = ev["ticker_path"].astype("string")
    ev["year_path"] = pd.to_numeric(ev["year_path"], errors="coerce").astype("Int64")

    ev["issues_list"] = ev["issues"].map(_parse_list_like)
    ev["warns_list"] = ev["warns"].map(_parse_list_like)
    ev["issues_n"] = ev["issues_list"].map(len)
    ev["warns_n"] = ev["warns_list"].map(len)

    fail_mask = ev["severity"].isin(["SOFT_FAIL", "HARD_FAIL"])
    hard_mask = ev["severity"].eq("HARD_FAIL")
    review_mask = ev["action"].eq("REVIEW")

    total_files = len(ev)
    total_rows = int(ev["rows"].sum())
    total_tickers = int(ev["ticker_path"].nunique(dropna=True))
    total_years = int(ev["year_path"].nunique(dropna=True))

    fail_files = int(fail_mask.sum())
    hard_files = int(hard_mask.sum())
    soft_files = int(ev["severity"].eq("SOFT_FAIL").sum())
    review_files = int(review_mask.sum())

    fail_rows = int(ev.loc[fail_mask, "rows"].sum())
    fail_tickers = int(ev.loc[fail_mask, "ticker_path"].nunique(dropna=True))
    fail_years = int(ev.loc[fail_mask, "year_path"].nunique(dropna=True))

    soft_fail_pct = 100.0 * soft_files / total_files if total_files else 0.0
    fail_pct = 100.0 * fail_files / total_files if total_files else 0.0
    pass_pct = 100.0 * float(ev["severity"].eq("PASS").sum()) / total_files if total_files else 0.0
    fail_rows_pct = 100.0 * fail_rows / total_rows if total_rows else 0.0

    # Root-cause granular (issues + warns explotados)
    issue_exp = (
        ev[["file", "ticker_path", "year_path", "rows", "issues_list"]]
        .explode("issues_list")
        .rename(columns={"issues_list": "cause"})
    )
    issue_exp["cause_source"] = "issue"
    issue_exp = issue_exp[issue_exp["cause"].notna() & (issue_exp["cause"].astype(str) != "")]

    warn_exp = (
        ev[["file", "ticker_path", "year_path", "rows", "warns_list"]]
        .explode("warns_list")
        .rename(columns={"warns_list": "cause"})
    )
    warn_exp["cause_source"] = "warn"
    warn_exp = warn_exp[warn_exp["cause"].notna() & (warn_exp["cause"].astype(str) != "")]

    causes = pd.concat([issue_exp, warn_exp], ignore_index=True)

    if not causes.empty:
        cause_stats = (
            causes.groupby(["cause_source", "cause"], dropna=False)
            .agg(
                files=("file", "nunique"),
                tickers=("ticker_path", "nunique"),
                years=("year_path", "nunique"),
                rows=("rows", "sum"),
            )
            .reset_index()
            .sort_values(["files", "rows"], ascending=False)
        )
    else:
        cause_stats = pd.DataFrame(columns=["cause_source", "cause", "files", "tickers", "years", "rows"])

    # Concentracion de fallos por ticker (HHI)
    fail_by_ticker = (
        ev.loc[fail_mask]
        .groupby("ticker_path", dropna=False)
        .agg(
            fail_files=("file", "count"),
            fail_rows=("rows", "sum"),
            years_affected=("year_path", "nunique"),
            max_severity=("severity", lambda s: max((_severity_rank(x) for x in s), default=0)),
        )
        .reset_index()
        .sort_values(["fail_files", "fail_rows"], ascending=False)
    )

    if fail_files > 0 and not fail_by_ticker.empty:
        p = (fail_by_ticker["fail_files"] / fail_files).astype(float)
        hhi = float((p * p).sum())
    else:
        hhi = 0.0

    # Perfil temporal de calidad
    by_year = (
        ev.groupby("year_path", dropna=False)
        .agg(
            files=("file", "count"),
            fail_files=("severity", lambda s: int(s.isin(["SOFT_FAIL", "HARD_FAIL"]).sum())),
            hard_files=("severity", lambda s: int((s == "HARD_FAIL").sum())),
            rows=("rows", "sum"),
            fail_rows=("rows", lambda r: int(ev.loc[r.index, :].loc[fail_mask.loc[r.index], "rows"].sum())),
        )
        .reset_index()
        .sort_values("year_path")
    )
    by_year["fail_pct"] = (100.0 * by_year["fail_files"] / by_year["files"]).round(4)

    # Resumen de severidad ponderado por filas
    sev = (
        ev.groupby("severity", dropna=False)
        .agg(files=("file", "count"), rows=("rows", "sum"))
        .reset_index()
        .sort_values("files", ascending=False)
    )
    sev["files_pct"] = (100.0 * sev["files"] / total_files).round(4)
    sev["rows_pct"] = (100.0 * sev["rows"] / total_rows).round(4)

    action_tbl = (
        ev.groupby("action", dropna=False)
        .agg(files=("file", "count"), rows=("rows", "sum"))
        .reset_index()
        .sort_values("files", ascending=False)
    )
    action_tbl["files_pct"] = (100.0 * action_tbl["files"] / total_files).round(4)

    # Cobertura esperada
    expected_total = int(live.get("expected_total", 0)) if isinstance(live, dict) else 0
    observed_total = int(live.get("observed_in_data_root", 0)) if isinstance(live, dict) else 0
    missing_expected = int(live.get("missing_in_data_root", 0)) if isinstance(live, dict) else 0

    # Criterios de dictamen
    criteria = {
        "hard_fail_zero": hard_files == 0,
        "soft_fail_pct_le_threshold": soft_fail_pct <= soft_fail_warn_threshold_pct,
        "retry_queue_empty": len(rq) == 0,
        "expected_coverage_ok": (missing_expected == 0) if (enforce_zero_missing_expected and expected_total > 0) else True,
    }

    if not criteria["hard_fail_zero"]:
        overall = "FAIL"
    elif not criteria["retry_queue_empty"]:
        overall = "WARN"
    elif not criteria["soft_fail_pct_le_threshold"]:
        overall = "WARN"
    elif not criteria["expected_coverage_ok"]:
        overall = "WARN"
    else:
        overall = "OK"

    # =========================
    # OUTPUT
    # =========================
    print("RUN_DIR:", run_dir)
    print("\n=== EXECUTIVE SUMMARY ===")
    summary = pd.DataFrame([
        {
            "overall": overall,
            "files_total": total_files,
            "rows_total": total_rows,
            "tickers_total": total_tickers,
            "years_total": total_years,
            "pass_pct_files": round(pass_pct, 4),
            "fail_pct_files": round(fail_pct, 4),
            "soft_fail_pct_files": round(soft_fail_pct, 4),
            "fail_pct_rows": round(fail_rows_pct, 4),
            "fail_tickers": fail_tickers,
            "fail_years": fail_years,
            "hhi_fail_concentration": round(hhi, 6),
            "expected_total": expected_total,
            "observed_in_data_root": observed_total,
            "missing_in_data_root": missing_expected,
            "retry_rows": len(rq),
        }
    ])
    print(summary.to_string(index=False))

    print("\n=== CRITERIA (PASS/FAIL) ===")
    crit_df = pd.DataFrame([{"criterion": k, "pass": bool(v)} for k, v in criteria.items()])
    print(crit_df.to_string(index=False))

    print("\n=== SEVERITY DISTRIBUTION ===")
    print(sev.to_string(index=False))

    print("\n=== ACTION DISTRIBUTION ===")
    print(action_tbl.to_string(index=False))

    print("\n=== TOP ROOT CAUSES ===")
    if cause_stats.empty:
        print("Sin causas registradas (issues/warns vacios).")
    else:
        cause_stats = cause_stats.copy()
        cause_stats["files_pct"] = (100.0 * cause_stats["files"] / total_files).round(4)
        print(cause_stats.head(20).to_string(index=False))

    print("\n=== FAIL CONCENTRATION BY TICKER ===")
    if fail_by_ticker.empty:
        print("Sin tickers con FAIL.")
    else:
        print(fail_by_ticker.head(20).to_string(index=False))

    print("\n=== TEMPORAL QUALITY BY YEAR ===")
    print(by_year.to_string(index=False))

    if not rq.empty:
        print("\n=== RETRY QUEUE (sample) ===")
        print(rq.head(20).to_string(index=False))

    if show_plots:
        # 1) Severidad por archivos
        plt.figure(figsize=(8, 4))
        plt.bar(sev["severity"].astype(str), sev["files"])
        plt.title("OHLCV Daily Audit - Severidad por Archivos")
        plt.xlabel("Severity")
        plt.ylabel("Files")
        plt.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.show()

        # 2) Fail % por año
        if not by_year.empty:
            plt.figure(figsize=(10, 4))
            x = by_year["year_path"].astype("Int64").astype("string")
            plt.plot(x, by_year["fail_pct"], marker="o")
            plt.title("OHLCV Daily Audit - Fail % por Año")
            plt.xlabel("Year")
            plt.ylabel("Fail %")
            plt.grid(alpha=0.3)
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            plt.show()

        # 3) Causas raíz (top)
        if not cause_stats.empty:
            top = cause_stats.head(10)
            labels = (top["cause_source"] + ":" + top["cause"]).astype(str)
            plt.figure(figsize=(11, 4))
            plt.bar(labels, top["files"])
            plt.title("OHLCV Daily Audit - Top Causas Raíz (por files)")
            plt.xlabel("Cause")
            plt.ylabel("Files")
            plt.xticks(rotation=45, ha="right")
            plt.grid(axis="y", alpha=0.3)
            plt.tight_layout()
            plt.show()

        # 4) Cobertura esperada
        if expected_total > 0:
            plt.figure(figsize=(8, 4))
            labs = ["expected", "observed", "missing"]
            vals = [expected_total, observed_total, missing_expected]
            plt.bar(labs, vals)
            plt.title("OHLCV Daily Audit - Cobertura de Tickers")
            plt.ylabel("Count")
            plt.grid(axis="y", alpha=0.3)
            plt.tight_layout()
            plt.show()


def main() -> None:
    ap = argparse.ArgumentParser(description="Reporte analitico riguroso de auditoria OHLCV daily")
    ap.add_argument("--run-dir", default="", help="Directorio del run de auditoria (si vacio, toma el ultimo valido)")
    ap.add_argument(
        "--base-dir",
        default=r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\ohlcv_daily_audit",
        help="Base de runs para auto-deteccion",
    )
    ap.add_argument("--soft-fail-threshold-pct", type=float, default=1.0, help="Umbral de soft_fail %% para WARN")
    ap.add_argument("--no-enforce-expected", action="store_true", help="No exigir missing_in_data_root=0")
    ap.add_argument("--no-plots", action="store_true", help="No mostrar graficos")
    args = ap.parse_args()

    run_dir = Path(args.run_dir) if str(args.run_dir).strip() else _latest_valid_run(Path(args.base_dir))
    build_report(
        run_dir=run_dir,
        soft_fail_warn_threshold_pct=float(args.soft_fail_threshold_pct),
        enforce_zero_missing_expected=not bool(args.no_enforce_expected),
        show_plots=not bool(args.no_plots),
    )


if __name__ == "__main__":
    main()
