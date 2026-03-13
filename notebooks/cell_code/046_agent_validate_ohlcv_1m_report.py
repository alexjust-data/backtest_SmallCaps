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
    runs = [p for p in base_dir.glob("*") if (p / "ohlcv_1m_events_current.csv").exists()]
    if not runs:
        raise FileNotFoundError(
            f"No hay runs con ohlcv_1m_events_current.csv en {base_dir}. Ejecuta primero 045_agent_validate_ohlcv_1m_strict.py"
        )
    return sorted(runs, key=lambda p: (p / "ohlcv_1m_events_current.csv").stat().st_mtime, reverse=True)[0]


def _sev_rank(s: str) -> int:
    s = str(s).upper()
    if s == "HARD_FAIL":
        return 3
    if s == "SOFT_FAIL":
        return 2
    if s == "WARN":
        return 1
    return 0


def build_report(run_dir: Path, soft_fail_warn_threshold_pct: float = 1.0, show_plots: bool = True) -> None:
    events_p = run_dir / "ohlcv_1m_events_current.csv"
    retry_p = run_dir / "retry_queue_ohlcv_1m_current.csv"
    live_p = run_dir / "live_status_ohlcv_1m.json"

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

    for c in ["severity", "issues", "warns", "action", "rows", "ticker_path", "year_path", "month_path"]:
        if c not in ev.columns:
            ev[c] = None

    ev["rows"] = pd.to_numeric(ev["rows"], errors="coerce").fillna(0).astype(int)
    ev["severity"] = ev["severity"].astype("string").fillna("UNKNOWN")
    ev["action"] = ev["action"].astype("string").fillna("UNKNOWN")
    ev["ticker_path"] = ev["ticker_path"].astype("string")
    ev["year_path"] = pd.to_numeric(ev["year_path"], errors="coerce").astype("Int64")
    ev["month_path"] = pd.to_numeric(ev["month_path"], errors="coerce").astype("Int64")

    ev["issues_list"] = ev["issues"].map(_parse_list_like)
    ev["warns_list"] = ev["warns"].map(_parse_list_like)

    fail_mask = ev["severity"].isin(["SOFT_FAIL", "HARD_FAIL"])
    hard_mask = ev["severity"].eq("HARD_FAIL")

    total_files = len(ev)
    total_rows = int(ev["rows"].sum())
    total_tickers = int(ev["ticker_path"].nunique(dropna=True))

    soft_files = int(ev["severity"].eq("SOFT_FAIL").sum())
    hard_files = int(hard_mask.sum())
    fail_files = int(fail_mask.sum())
    fail_rows = int(ev.loc[fail_mask, "rows"].sum())

    pass_pct = 100.0 * float(ev["severity"].eq("PASS").sum()) / total_files if total_files else 0.0
    fail_pct = 100.0 * fail_files / total_files if total_files else 0.0
    soft_fail_pct = 100.0 * soft_files / total_files if total_files else 0.0
    fail_rows_pct = 100.0 * fail_rows / total_rows if total_rows else 0.0

    issue_exp = ev[["file", "ticker_path", "year_path", "month_path", "rows", "issues_list"]].explode("issues_list")
    issue_exp = issue_exp.rename(columns={"issues_list": "cause"})
    issue_exp["source"] = "issue"
    issue_exp = issue_exp[issue_exp["cause"].notna() & (issue_exp["cause"].astype(str) != "")]

    warn_exp = ev[["file", "ticker_path", "year_path", "month_path", "rows", "warns_list"]].explode("warns_list")
    warn_exp = warn_exp.rename(columns={"warns_list": "cause"})
    warn_exp["source"] = "warn"
    warn_exp = warn_exp[warn_exp["cause"].notna() & (warn_exp["cause"].astype(str) != "")]

    causes = pd.concat([issue_exp, warn_exp], ignore_index=True)
    if not causes.empty:
        cause_stats = (
            causes.groupby(["source", "cause"], dropna=False)
            .agg(files=("file", "nunique"), tickers=("ticker_path", "nunique"), year_months=("file", "count"), rows=("rows", "sum"))
            .reset_index()
            .sort_values(["files", "rows"], ascending=False)
        )
    else:
        cause_stats = pd.DataFrame(columns=["source", "cause", "files", "tickers", "year_months", "rows"])

    fail_by_ticker = (
        ev.loc[fail_mask]
        .groupby("ticker_path", dropna=False)
        .agg(fail_files=("file", "count"), fail_rows=("rows", "sum"), years=("year_path", "nunique"), months=("month_path", "nunique"), max_severity=("severity", lambda s: max((_sev_rank(x) for x in s), default=0)))
        .reset_index()
        .sort_values(["fail_files", "fail_rows"], ascending=False)
    )

    if fail_files > 0 and not fail_by_ticker.empty:
        p = (fail_by_ticker["fail_files"] / fail_files).astype(float)
        hhi = float((p * p).sum())
    else:
        hhi = 0.0

    ev["ym"] = ev["year_path"].astype("string") + "-" + ev["month_path"].astype("string").str.zfill(2)
    by_ym = (
        ev.groupby("ym", dropna=False)
        .agg(files=("file", "count"), fail_files=("severity", lambda s: int(s.isin(["SOFT_FAIL", "HARD_FAIL"]).sum())), rows=("rows", "sum"), fail_rows=("rows", lambda r: int(ev.loc[r.index, :].loc[fail_mask.loc[r.index], "rows"].sum())))
        .reset_index()
        .sort_values("ym")
    )
    by_ym["fail_pct"] = (100.0 * by_ym["fail_files"] / by_ym["files"]).round(4)

    sev = ev.groupby("severity", dropna=False).agg(files=("file", "count"), rows=("rows", "sum")).reset_index().sort_values("files", ascending=False)
    sev["files_pct"] = (100.0 * sev["files"] / total_files).round(4)
    sev["rows_pct"] = (100.0 * sev["rows"] / total_rows).round(4)

    expected_total = int(live.get("expected_total", 0)) if isinstance(live, dict) else 0
    observed_total = int(live.get("observed_in_data_root", 0)) if isinstance(live, dict) else 0
    missing_total = int(live.get("missing_in_data_root", 0)) if isinstance(live, dict) else 0

    criteria = {
        "hard_fail_zero": hard_files == 0,
        "soft_fail_pct_le_threshold": soft_fail_pct <= soft_fail_warn_threshold_pct,
        "retry_queue_empty": len(rq) == 0,
    }
    if expected_total > 0:
        criteria["expected_coverage_ok"] = (missing_total == 0)

    overall = "OK"
    if not criteria.get("hard_fail_zero", True):
        overall = "FAIL"
    elif not all(criteria.values()):
        overall = "WARN"

    print("RUN_DIR:", run_dir)
    print("\n=== EXECUTIVE SUMMARY (1m) ===")
    summary = pd.DataFrame([
        {
            "overall": overall,
            "files_total": total_files,
            "rows_total": total_rows,
            "tickers_total": total_tickers,
            "pass_pct_files": round(pass_pct, 4),
            "fail_pct_files": round(fail_pct, 4),
            "soft_fail_pct_files": round(soft_fail_pct, 4),
            "fail_pct_rows": round(fail_rows_pct, 4),
            "hhi_fail_concentration": round(hhi, 6),
            "retry_rows": len(rq),
            "expected_total": expected_total,
            "observed_in_data_root": observed_total,
            "missing_in_data_root": missing_total,
        }
    ])
    print(summary.to_string(index=False))

    print("\n=== CRITERIA (PASS/FAIL) ===")
    print(pd.DataFrame([{"criterion":k, "pass":bool(v)} for k,v in criteria.items()]).to_string(index=False))

    print("\n=== SEVERITY DISTRIBUTION ===")
    print(sev.to_string(index=False))

    print("\n=== TOP ROOT CAUSES ===")
    if cause_stats.empty:
        print("Sin causas registradas.")
    else:
        cause_stats["files_pct"] = (100.0 * cause_stats["files"] / total_files).round(4)
        print(cause_stats.head(25).to_string(index=False))

    print("\n=== FAIL CONCENTRATION BY TICKER ===")
    if fail_by_ticker.empty:
        print("Sin tickers con FAIL.")
    else:
        print(fail_by_ticker.head(25).to_string(index=False))

    print("\n=== TEMPORAL QUALITY BY YEAR-MONTH ===")
    print(by_ym.to_string(index=False))

    if not rq.empty:
        print("\n=== RETRY QUEUE (sample) ===")
        print(rq.head(20).to_string(index=False))

    if show_plots:
        plt.figure(figsize=(8, 4))
        plt.bar(sev["severity"].astype(str), sev["files"])
        plt.title("OHLCV 1m - Severidad por archivos")
        plt.xlabel("Severity")
        plt.ylabel("Files")
        plt.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.show()

        if not by_ym.empty:
            plt.figure(figsize=(12, 4))
            plt.plot(by_ym["ym"].astype(str), by_ym["fail_pct"], marker="o", linewidth=1)
            plt.title("OHLCV 1m - Fail % por Year-Month")
            plt.xlabel("Year-Month")
            plt.ylabel("Fail %")
            plt.grid(alpha=0.3)
            plt.xticks(rotation=60, ha="right")
            plt.tight_layout()
            plt.show()


def main() -> None:
    ap = argparse.ArgumentParser(description="Reporte analitico de auditoria OHLCV 1m")
    ap.add_argument("--run-dir", default="", help="Run directory; vacio => ultimo valido")
    ap.add_argument("--base-dir", default=r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\ohlcv_1m_audit")
    ap.add_argument("--soft-fail-threshold-pct", type=float, default=1.0)
    ap.add_argument("--no-plots", action="store_true")
    args = ap.parse_args()

    run_dir = Path(args.run_dir) if str(args.run_dir).strip() else _latest_valid_run(Path(args.base_dir))
    build_report(run_dir=run_dir, soft_fail_warn_threshold_pct=float(args.soft_fail_threshold_pct), show_plots=not bool(args.no_plots))


if __name__ == "__main__":
    main()
