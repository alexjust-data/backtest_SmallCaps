from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

DATASETS = ["income_statements", "balance_sheets", "cash_flow_statements", "ratios"]
DATE_CANDIDATES_BY_DATASET = {
    "income_statements": ["period_end", "filing_date", "_ingested_utc"],
    "balance_sheets": ["period_end", "filing_date", "_ingested_utc"],
    "cash_flow_statements": ["period_end", "filing_date", "_ingested_utc"],
    "ratios": ["date", "_ingested_utc"],
}
DATE_CANDIDATES_FALLBACK = ["period_end", "date", "filing_date", "report_period", "calendar_date", "_ingested_utc"]
MIN_REQUIRED_COLS = {
    "income_statements": ["ticker", "cik"],
    "balance_sheets": ["ticker", "cik"],
    "cash_flow_statements": ["ticker", "cik"],
    "ratios": ["ticker"],
}


def _expected_tickers(universe_path: Path) -> List[str]:
    d = pd.read_parquet(universe_path, columns=["ticker"])
    return (
        d["ticker"].astype("string").str.strip().dropna().str.upper().drop_duplicates().sort_values().tolist()
    )


def _universe_windows(universe_path: Path) -> pd.DataFrame:
    cols = [
        "ticker",
        "cik",
        "first_seen_date",
        "last_seen_date",
        "list_date",
        "delisted_utc",
    ]
    schema = set(pq.ParquetFile(universe_path).schema.names)
    use_cols = [c for c in cols if c in schema]
    d = pd.read_parquet(universe_path, columns=use_cols)
    d["ticker"] = d["ticker"].astype("string").str.strip().str.upper()

    for c in ["first_seen_date", "last_seen_date", "list_date", "delisted_utc"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce", utc=True)

    # one row per ticker
    agg = {
        "cik": "first",
        "first_seen_date": "min",
        "last_seen_date": "max",
        "list_date": "min",
        "delisted_utc": "max",
    }
    agg = {k: v for k, v in agg.items() if k in d.columns}
    out = d.groupby("ticker", as_index=False).agg(agg)
    return out


def _load_lifecycle(lifecycle_path: Optional[Path]) -> pd.DataFrame:
    if lifecycle_path is None or not lifecycle_path.exists():
        return pd.DataFrame(columns=["ticker", "cik", "official_list_date", "official_delist_date"])

    cols = ["ticker", "cik", "list_date", "delist_date"]
    schema = set(pq.ParquetFile(lifecycle_path).schema.names)
    use_cols = [c for c in cols if c in schema]
    d = pd.read_parquet(lifecycle_path, columns=use_cols)
    if "ticker" not in d.columns:
        return pd.DataFrame(columns=["ticker", "cik", "official_list_date", "official_delist_date"])

    d["ticker"] = d["ticker"].astype("string").str.strip().str.upper()
    if "list_date" in d.columns:
        d["list_date"] = pd.to_datetime(d["list_date"], errors="coerce", utc=True)
    if "delist_date" in d.columns:
        d["delist_date"] = pd.to_datetime(d["delist_date"], errors="coerce", utc=True)

    d = d.rename(columns={"list_date": "official_list_date", "delist_date": "official_delist_date"})
    return d


def _dataset_files(dataset_root: Path) -> List[Path]:
    return sorted(dataset_root.glob("ticker=*/*.parquet"))


def _ticker_from_parent(p: Path) -> str:
    return p.parent.name.replace("ticker=", "").strip().upper()


def _date_candidates(dataset: str) -> List[str]:
    return DATE_CANDIDATES_BY_DATASET.get(dataset, DATE_CANDIDATES_FALLBACK)


def _read_table_columns(p: Path, cols: List[str]) -> pd.DataFrame:
    # Preserve order but avoid duplicate column requests.
    use_cols = list(dict.fromkeys([c for c in cols if c]))
    try:
        table = pq.read_table(p, columns=use_cols)
        df = table.to_pandas()
    except Exception:
        # Some parquet files have mixed encoding across row-groups for the same field.
        # pandas with pyarrow engine is more tolerant here.
        df = pd.read_parquet(p, columns=use_cols)
    # Defensive: if provider emits duplicate names, keep first occurrence.
    if hasattr(df, "columns") and df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _extract_business_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "_empty" in df.columns:
        empty_mask = df["_empty"].astype("boolean").fillna(False)
        return df[~empty_mask].copy()
    return df


def _coerce_tickers(cell) -> List[str]:
    out: List[str] = []
    if isinstance(cell, str):
        s = cell.strip().upper()
        if s:
            out.append(s)
    elif isinstance(cell, list):
        for x in cell:
            if isinstance(x, str):
                s = x.strip().upper()
                if s:
                    out.append(s)
    return out


def _analyze_file(p: Path, dataset: str, expected_ticker: str, page_cap_rows: int) -> Dict:
    base = {
        "dataset": dataset,
        "file": str(p),
        "expected_ticker": expected_ticker,
        "read_ok": False,
        "rows_total": None,
        "rows_business": None,
        "ticker_col_ok": False,
        "ticker_values_nunique": None,
        "tickers_col_mismatch_rows": None,
        "cik_nunique": None,
        "dataset_col_ok": None,
        "ingested_utc_parseable": None,
        "missing_required_cols": "",
        "date_col_used": None,
        "date_start": None,
        "date_end": None,
        "suspicious_page_cap": False,
        "issues": "",
    }

    issues: List[str] = []
    try:
        pf = pq.ParquetFile(p)
        meta = pf.metadata
        rows_total = int(meta.num_rows) if meta else 0
        base["rows_total"] = rows_total
        base["suspicious_page_cap"] = rows_total >= page_cap_rows
        if base["suspicious_page_cap"]:
            issues.append("rows_hit_page_cap")

        schema_cols = pf.schema_arrow.names
        req = MIN_REQUIRED_COLS.get(dataset, ["ticker"])
        missing_req = sorted([c for c in req if c not in schema_cols])
        base["missing_required_cols"] = "|".join(missing_req)
        if missing_req:
            issues.append("missing_required_cols")

        to_read = ["ticker", "tickers", "cik", "_empty", "_dataset", "_ingested_utc"]
        to_read.extend(_date_candidates(dataset))
        to_read = [c for c in to_read if c in schema_cols]
        df = _read_table_columns(p, to_read) if to_read else pd.DataFrame(index=range(rows_total))
        biz = _extract_business_rows(df)
        base["rows_business"] = int(len(biz))

        # ticker canonical
        if "ticker" in biz.columns and len(biz) > 0:
            vals = biz["ticker"].astype("string").str.strip().str.upper().dropna().unique().tolist()
            base["ticker_values_nunique"] = len(vals)
            if len(vals) == 1 and vals[0] == expected_ticker:
                base["ticker_col_ok"] = True
            else:
                issues.append("ticker_col_mismatch")
        elif len(biz) == 0:
            base["ticker_col_ok"] = True
            base["ticker_values_nunique"] = 0
        else:
            issues.append("missing_ticker_col")

        # tickers field consistency
        if "tickers" in biz.columns and len(biz) > 0:
            bad = 0
            for v in biz["tickers"].tolist():
                vals = _coerce_tickers(v)
                if vals and expected_ticker not in vals:
                    bad += 1
            base["tickers_col_mismatch_rows"] = int(bad)
            if bad > 0:
                issues.append("tickers_field_mismatch")
        else:
            base["tickers_col_mismatch_rows"] = 0

        # cik consistency (warning level)
        if "cik" in biz.columns and len(biz) > 0:
            base["cik_nunique"] = int(biz["cik"].astype("string").str.strip().dropna().nunique())
            if base["cik_nunique"] > 1:
                issues.append("multi_cik")
        else:
            base["cik_nunique"] = 0

        # dataset marker
        if "_dataset" in df.columns and len(df) > 0:
            vals = df["_dataset"].astype("string").str.strip().dropna().unique().tolist()
            base["dataset_col_ok"] = bool(len(vals) == 1 and vals[0] == dataset)
            if not base["dataset_col_ok"]:
                issues.append("dataset_marker_mismatch")
        else:
            base["dataset_col_ok"] = None

        # ingested utc parse
        if "_ingested_utc" in df.columns and len(df) > 0:
            ser = pd.to_datetime(df["_ingested_utc"], errors="coerce", utc=True)
            base["ingested_utc_parseable"] = bool(ser.notna().all())
            if not base["ingested_utc_parseable"]:
                issues.append("bad_ingested_utc")
        else:
            base["ingested_utc_parseable"] = None

        # dates
        date_used: Optional[str] = None
        dmin = None
        dmax = None
        for c in _date_candidates(dataset):
            if c in biz.columns:
                ser = pd.to_datetime(biz[c], errors="coerce", utc=True)
                if ser.notna().any():
                    date_used = c
                    dmin = ser.min()
                    dmax = ser.max()
                    break
        base["date_col_used"] = date_used
        base["date_start"] = dmin
        base["date_end"] = dmax
        if len(biz) > 0 and date_used is None:
            issues.append("no_parseable_date")

        base["read_ok"] = True
    except Exception as e:
        issues.append(f"read_error:{e}")

    base["issues"] = "|".join(issues)
    return base


def _safe_mkdir(preferred: Path) -> Path:
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        return preferred
    except Exception:
        fallback = Path.cwd() / f"audit_fundamentals_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _build_temporal_validation(
    files_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    lifecycle_df: pd.DataFrame,
    tol_pre_days: int,
    tol_post_days: int,
) -> pd.DataFrame:
    if len(files_df) == 0:
        return pd.DataFrame()

    r = files_df[["dataset", "expected_ticker", "rows_business", "date_start", "date_end"]].copy()
    r = r.rename(columns={"expected_ticker": "ticker"})
    r["ticker"] = r["ticker"].astype("string").str.strip().str.upper()

    # per ticker fundamentals window
    g = (
        r.groupby("ticker", as_index=False)
        .agg(
            fin_min_date=("date_start", "min"),
            fin_max_date=("date_end", "max"),
            datasets_with_rows=("rows_business", lambda s: int((pd.Series(s).fillna(0) > 0).sum())),
            total_rows=("rows_business", "sum"),
        )
    )

    u = universe_df.copy()
    u["ticker"] = u["ticker"].astype("string").str.strip().str.upper()
    l = lifecycle_df.copy()
    if len(l):
        l["ticker"] = l["ticker"].astype("string").str.strip().str.upper()

    m = u.merge(g, on="ticker", how="left")
    if len(l):
        m = m.merge(l[[c for c in ["ticker", "official_list_date", "official_delist_date", "cik"] if c in l.columns]], on="ticker", how="left", suffixes=("", "_lifecycle"))

    # reference window: official > PTI
    m["start_ref"] = m["official_list_date"] if "official_list_date" in m.columns else pd.NaT
    m["end_ref"] = m["official_delist_date"] if "official_delist_date" in m.columns else pd.NaT

    m["start_ref"] = m["start_ref"].fillna(m.get("first_seen_date"))
    m["end_ref"] = m["end_ref"].fillna(m.get("last_seen_date"))

    pre_tol = pd.Timedelta(days=int(tol_pre_days))
    post_tol = pd.Timedelta(days=int(tol_post_days))

    m["anomaly_pre_start"] = False
    m["anomaly_post_end"] = False

    has_fin = m["fin_min_date"].notna() & m["fin_max_date"].notna()
    has_start = m["start_ref"].notna()
    has_end = m["end_ref"].notna()

    m.loc[has_fin & has_start, "anomaly_pre_start"] = m.loc[has_fin & has_start, "fin_min_date"] < (
        m.loc[has_fin & has_start, "start_ref"] - pre_tol
    )
    m.loc[has_fin & has_end, "anomaly_post_end"] = m.loc[has_fin & has_end, "fin_max_date"] > (
        m.loc[has_fin & has_end, "end_ref"] + post_tol
    )

    def _status(row: pd.Series) -> str:
        if pd.isna(row.get("fin_min_date")):
            return "NO_DATA"
        if bool(row.get("anomaly_pre_start")):
            return "ANOMALY_PRE_START"
        if bool(row.get("anomaly_post_end")):
            return "ANOMALY_POST_END"
        if pd.isna(row.get("start_ref")) and pd.isna(row.get("end_ref")):
            return "UNKNOWN_WINDOW"
        return "OK"

    m["temporal_status"] = m.apply(_status, axis=1)
    return m


def run_audit(
    universe_path: Path,
    outdir: Path,
    datasets: List[str],
    limit: int,
    max_pages: int,
    lifecycle_path: Optional[Path],
    tol_pre_days: int,
    tol_post_days: int,
    audit_dir_arg: Optional[Path],
) -> None:
    preferred = audit_dir_arg if audit_dir_arg is not None else (outdir / "_audit")
    audit_dir = _safe_mkdir(preferred)

    expected = _expected_tickers(universe_path)
    expected_set = set(expected)

    coverage_rows: List[Dict] = []
    file_rows: List[Dict] = []
    missing_rows: List[Dict] = []

    page_cap_rows = int(limit * max_pages)

    for ds in datasets:
        ds_root = outdir / ds
        files = _dataset_files(ds_root) if ds_root.exists() else []

        downloaded_tickers = sorted({_ticker_from_parent(p) for p in files})
        downloaded_set = set(downloaded_tickers)

        missing = sorted(expected_set - downloaded_set)
        extra = sorted(downloaded_set - expected_set)

        by_ticker_count: Dict[str, int] = {}
        for p in files:
            t = _ticker_from_parent(p)
            by_ticker_count[t] = by_ticker_count.get(t, 0) + 1
        duplicate_file_tickers = sorted([t for t, c in by_ticker_count.items() if c > 1])

        coverage_rows.append(
            {
                "dataset": ds,
                "expected_tickers": len(expected_set),
                "downloaded_tickers": len(downloaded_set),
                "missing_tickers": len(missing),
                "extra_tickers": len(extra),
                "tickers_with_multiple_files": len(duplicate_file_tickers),
                "coverage_pct": round((len(downloaded_set) / len(expected_set) * 100.0), 4) if expected_set else 0.0,
            }
        )

        for t in missing:
            missing_rows.append({"dataset": ds, "ticker": t})

        for p in files:
            exp = _ticker_from_parent(p)
            file_rows.append(_analyze_file(p, ds, exp, page_cap_rows))

    cov = pd.DataFrame(coverage_rows)
    files_df = pd.DataFrame(file_rows)
    miss_df = pd.DataFrame(missing_rows)

    ranges_cols = [
        "dataset",
        "expected_ticker",
        "rows_business",
        "date_col_used",
        "date_start",
        "date_end",
        "ticker_col_ok",
        "cik_nunique",
        "dataset_col_ok",
        "ingested_utc_parseable",
        "missing_required_cols",
        "suspicious_page_cap",
        "issues",
    ]
    ranges = (
        files_df[ranges_cols].rename(columns={"expected_ticker": "ticker"}).copy()
        if len(files_df)
        else pd.DataFrame(columns=ranges_cols)
    )

    severe_mask = (
        (files_df["read_ok"] != True)
        | (files_df["ticker_col_ok"] != True)
        | (files_df["issues"].astype(str).str.contains(
            "tickers_field_mismatch|rows_hit_page_cap|read_error|missing_required_cols|dataset_marker_mismatch|bad_ingested_utc",
            na=False,
        ))
    ) if len(files_df) else pd.Series(dtype=bool)
    severe = files_df[severe_mask].copy() if len(files_df) else pd.DataFrame()

    universe_df = _universe_windows(universe_path)
    lifecycle_df = _load_lifecycle(lifecycle_path)
    temporal = _build_temporal_validation(
        files_df=files_df,
        universe_df=universe_df,
        lifecycle_df=lifecycle_df,
        tol_pre_days=tol_pre_days,
        tol_post_days=tol_post_days,
    )

    temporal_issues = temporal[temporal["temporal_status"].isin(["ANOMALY_PRE_START", "ANOMALY_POST_END"])].copy() if len(temporal) else pd.DataFrame()

    overall_ok = (
        (cov["missing_tickers"].sum() == 0 if len(cov) else False)
        and (cov["extra_tickers"].sum() == 0 if len(cov) else False)
        and (cov["tickers_with_multiple_files"].sum() == 0 if len(cov) else False)
        and (len(severe) == 0)
        and (len(temporal_issues) == 0)
    )

    cov.to_csv(audit_dir / "coverage_by_endpoint.csv", index=False)
    miss_df.to_csv(audit_dir / "missing_tickers_by_endpoint.csv", index=False)
    files_df.to_csv(audit_dir / "file_level_audit.csv", index=False)
    ranges.to_csv(audit_dir / "date_ranges_by_ticker_endpoint.csv", index=False)
    severe.to_csv(audit_dir / "severe_issues.csv", index=False)
    temporal.to_csv(audit_dir / "temporal_validation_by_ticker.csv", index=False)
    temporal_issues.to_csv(audit_dir / "temporal_issues.csv", index=False)

    summary = {
        "status": "OK" if overall_ok else "FAIL",
        "expected_tickers": len(expected_set),
        "datasets": datasets,
        "missing_total": int(cov["missing_tickers"].sum()) if len(cov) else None,
        "extra_total": int(cov["extra_tickers"].sum()) if len(cov) else None,
        "multi_file_tickers_total": int(cov["tickers_with_multiple_files"].sum()) if len(cov) else None,
        "severe_issues": int(len(severe)),
        "temporal_issues": int(len(temporal_issues)),
        "temporal_status_counts": temporal["temporal_status"].value_counts(dropna=False).to_dict() if len(temporal) else {},
        "outdir": str(outdir),
        "audit_dir": str(audit_dir),
        "lifecycle_path": str(lifecycle_path) if lifecycle_path else None,
        "tolerance_pre_days": int(tol_pre_days),
        "tolerance_post_days": int(tol_post_days),
    }
    (audit_dir / "audit_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("=== AUDIT SUMMARY ===")
    print(json.dumps(summary, indent=2))
    print("\n=== COVERAGE BY ENDPOINT ===")
    print(cov.to_string(index=False))
    print("\n=== TEMPORAL STATUS ===")
    if len(temporal):
        print(temporal["temporal_status"].value_counts(dropna=False).to_string())
    else:
        print("(empty)")
    print("\nSaved:")
    print(audit_dir)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit fundamentals download: coverage + integrity + temporal lifecycle checks")
    ap.add_argument("--input-universe", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--datasets", default=",".join(DATASETS))
    ap.add_argument("--limit", type=int, default=1000, help="Downloader limit usado")
    ap.add_argument("--max-pages", type=int, default=50, help="Downloader max-pages usado")
    ap.add_argument("--lifecycle-parquet", default="", help="Opcional: official_lifecycle_compiled.multisource.parquet")
    ap.add_argument("--tolerance-pre-days", type=int, default=365)
    ap.add_argument("--tolerance-post-days", type=int, default=180)
    ap.add_argument("--audit-dir", default="", help="Directorio de salida de auditoria (opcional)")
    args = ap.parse_args()

    ds = [x.strip() for x in args.datasets.split(",") if x.strip()]
    lifecycle = Path(args.lifecycle_parquet) if args.lifecycle_parquet else None
    audit_dir = Path(args.audit_dir) if args.audit_dir else None
    run_audit(
        universe_path=Path(args.input_universe),
        outdir=Path(args.outdir),
        datasets=ds,
        limit=args.limit,
        max_pages=args.max_pages,
        lifecycle_path=lifecycle,
        tol_pre_days=args.tolerance_pre_days,
        tol_post_days=args.tolerance_post_days,
        audit_dir_arg=audit_dir,
    )


if __name__ == "__main__":
    main()
