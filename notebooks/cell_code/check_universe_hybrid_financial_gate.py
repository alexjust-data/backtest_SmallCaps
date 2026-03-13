from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    p_tickers = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet")
    p_tickers_upper = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet")
    p_hybrid = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\hybrid_enriched\universe_hybrid_enriched.parquet")

    audit_candidates = [
        Path(r"D:\financial\_audit"),
        Path(r"C:\Users\AlexJ\financial_audit"),
    ]

    outdir = Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\runs\data_quality\00_data_certification")
    outdir.mkdir(parents=True, exist_ok=True)

    t = pd.read_parquet(p_tickers)
    tu = pd.read_parquet(p_tickers_upper)
    h = pd.read_parquet(p_hybrid)

    for df in [t, tu, h]:
        for c in ["ticker", "primary_exchange", "entity_id"]:
            if c in df.columns:
                df[c] = df[c].astype("string").str.strip()

    t_key_dups = int(t.duplicated(["entity_id"]).sum()) if "entity_id" in t.columns else None
    t_null_entity = int(t["entity_id"].isna().sum()) if "entity_id" in t.columns else None
    t_status = t["status"].value_counts(dropna=False).to_dict() if "status" in t.columns else {}

    t_entity = set(t["entity_id"].dropna().unique()) if "entity_id" in t.columns else set()
    h_entity = set(h["entity_id"].dropna().unique()) if "entity_id" in h.columns else set()

    missing_in_hybrid = sorted(t_entity - h_entity)
    extra_in_hybrid = sorted(h_entity - t_entity)

    consistency_summary = {
        "tickers_rows": int(len(t)),
        "tickers_upper_rows": int(len(tu)),
        "hybrid_rows": int(len(h)),
        "tickers_entity_unique": int(len(t_entity)),
        "hybrid_entity_unique": int(len(h_entity)),
        "missing_entities_in_hybrid": int(len(missing_in_hybrid)),
        "extra_entities_in_hybrid": int(len(extra_in_hybrid)),
        "tickers_entity_dup": t_key_dups,
        "tickers_null_entity": t_null_entity,
        "tickers_status_dist": t_status,
    }

    if {"entity_id", "final_cik"}.issubset(h.columns):
        cik_check = (
            h.assign(final_cik=h["final_cik"].astype("string").str.strip())
            .groupby("entity_id", dropna=False)["final_cik"]
            .apply(lambda s: s.dropna().nunique())
            .reset_index(name="cik_nunique_nonnull")
        )
        cik_issues = cik_check[cik_check["cik_nunique_nonnull"] > 1].copy()
    else:
        cik_check = pd.DataFrame()
        cik_issues = pd.DataFrame()

    audit_dir = None
    for c in audit_candidates:
        if (c / "date_ranges_by_ticker_endpoint.csv").exists():
            audit_dir = c
            break

    if audit_dir is None:
        raise FileNotFoundError(
            "No encontré date_ranges_by_ticker_endpoint.csv en D:\\financial\\_audit ni en C:\\Users\\AlexJ\\financial_audit"
        )

    r = pd.read_csv(audit_dir / "date_ranges_by_ticker_endpoint.csv")
    r["ticker"] = r["ticker"].astype("string").str.strip().str.upper()
    r["date_start"] = pd.to_datetime(r["date_start"], errors="coerce", utc=True)
    r["date_end"] = pd.to_datetime(r["date_end"], errors="coerce", utc=True)

    if "rows_business" in r.columns:
        r_data = r[r["rows_business"].fillna(0) > 0].copy()
    else:
        r_data = r.copy()

    piv_start = r_data.pivot_table(index="ticker", columns="dataset", values="date_start", aggfunc="min")
    piv_end = r_data.pivot_table(index="ticker", columns="dataset", values="date_end", aggfunc="max")

    piv_start.columns = [f"fin_{c}_min_date" for c in piv_start.columns]
    piv_end.columns = [f"fin_{c}_max_date" for c in piv_end.columns]

    fin_ranges = piv_start.join(piv_end, how="outer").reset_index()

    global_ranges = (
        r_data.groupby("ticker", as_index=False)
        .agg(fin_min_date=("date_start", "min"), fin_max_date=("date_end", "max"))
    )

    fin_ranges = fin_ranges.merge(global_ranges, on="ticker", how="outer")

    h2 = h.copy()
    h2["ticker"] = h2["ticker"].astype("string").str.strip().str.upper()
    h2 = h2.merge(fin_ranges, on="ticker", how="left")

    p_hybrid_fin = p_hybrid.parent / "universe_hybrid_enriched_with_financial_ranges.parquet"
    h2.to_parquet(p_hybrid_fin, index=False)

    close_candidates = [
        Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\additional\day_aggs_v1"),
        Path(r"C:\TSIS_Data\v1\backtest_SmallCaps\data\additional\ohlcv_daily"),
        Path(r"D:\financial\day_aggs_v1"),
        Path(r"C:\TSIS_Data\data\day_aggs_v1"),
    ]
    close_found = [str(p) for p in close_candidates if p.exists()]

    gate = {
        "close_t_source_found": len(close_found) > 0,
        "close_t_source_paths": close_found,
        "shares_temporal_ranges_available": len(r_data) > 0,
        "can_compute_smallcap_pti_now": bool(len(close_found) > 0 and len(r_data) > 0),
    }

    pd.DataFrame([consistency_summary]).to_json(
        outdir / "check_universe_consistency_summary.json", orient="records", indent=2
    )
    pd.Series(gate).to_json(outdir / "check_smallcap_gate.json", indent=2)

    pd.DataFrame({"entity_id": missing_in_hybrid}).to_csv(outdir / "missing_entities_in_hybrid.csv", index=False)
    pd.DataFrame({"entity_id": extra_in_hybrid}).to_csv(outdir / "extra_entities_in_hybrid.csv", index=False)
    cik_check.to_csv(outdir / "cik_consistency_by_entity.csv", index=False)
    cik_issues.to_csv(outdir / "cik_issues_multi_values.csv", index=False)

    endpoint_summary = (
        r_data.groupby("dataset", as_index=False)
        .agg(
            tickers_totales=("ticker", "nunique"),
            min_date=("date_start", "min"),
            max_date=("date_end", "max"),
        )
    )
    endpoint_summary.to_csv(outdir / "financial_endpoint_minmax_summary.csv", index=False)

    print("=== CONSISTENCY SUMMARY ===")
    print(pd.DataFrame([consistency_summary]).to_string(index=False))
    print("\n=== SMALLCAP GATE ===")
    print(pd.Series(gate).to_string())
    print("\n=== ENDPOINT MIN/MAX ===")
    print(endpoint_summary.to_string(index=False))
    print("\nSaved:")
    print(outdir)
    print("Hybrid+financial ranges:", p_hybrid_fin)


if __name__ == "__main__":
    main()
