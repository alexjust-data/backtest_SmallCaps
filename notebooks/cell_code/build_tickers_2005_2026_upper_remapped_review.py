from pathlib import Path
import json
import pandas as pd


UNIVERSE_FULL = Path(
    globals().get(
        "UNIVERSE_FULL",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet",
    )
)
UNIVERSE_UPPER = Path(
    globals().get(
        "UNIVERSE_UPPER",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper.parquet",
    )
)
EXCLUDED_PARQUET = Path(
    globals().get(
        "EXCLUDED_PARQUET",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026_upper_excluded_overview404.parquet",
    )
)
OUTDIR = Path(
    globals().get(
        "OUTDIR",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti",
    )
)

REVIEW_PARQUET = OUTDIR / "tickers_2005_2026_upper_remapped_review.parquet"
REVIEW_CSV = OUTDIR / "tickers_2005_2026_upper_remapped_review.csv"
SUMMARY_JSON = OUTDIR / "tickers_2005_2026_upper_remapped_review.summary.json"


if not UNIVERSE_FULL.exists():
    raise FileNotFoundError(UNIVERSE_FULL)
if not UNIVERSE_UPPER.exists():
    raise FileNotFoundError(UNIVERSE_UPPER)
if not EXCLUDED_PARQUET.exists():
    raise FileNotFoundError(EXCLUDED_PARQUET)

OUTDIR.mkdir(parents=True, exist_ok=True)

full = pd.read_parquet(UNIVERSE_FULL).copy()
upper = pd.read_parquet(UNIVERSE_UPPER).copy()
excluded = pd.read_parquet(EXCLUDED_PARQUET).copy()

full["ticker"] = full["ticker"].astype("string").str.strip().str.upper()
upper["ticker"] = upper["ticker"].astype("string").str.strip().str.upper()
excluded["ticker"] = excluded["ticker"].astype("string").str.strip().str.upper()
excluded["suggested_base_ticker"] = excluded["suggested_base_ticker"].astype("string").str.strip().str.upper()

upper_lookup = (
    upper.loc[:, [c for c in upper.columns if c in ["ticker", "name", "status", "primary_exchange", "entity_id"]]]
    .drop_duplicates(subset=["ticker"], keep="first")
    .rename(columns={
        "ticker": "ticker_suggested",
        "name": "suggested_name_upper",
        "status": "suggested_status_upper",
        "primary_exchange": "suggested_primary_exchange_upper",
        "entity_id": "suggested_entity_id_upper",
    })
)

full_lookup = (
    full.loc[:, [c for c in full.columns if c in ["ticker", "name", "status", "primary_exchange", "entity_id", "first_seen_date", "last_seen_date"]]]
    .drop_duplicates(subset=["ticker"], keep="first")
    .rename(columns={
        "ticker": "ticker_suggested",
        "name": "suggested_name_full",
        "status": "suggested_status_full",
        "primary_exchange": "suggested_primary_exchange_full",
        "entity_id": "suggested_entity_id_full",
        "first_seen_date": "suggested_first_seen_date",
        "last_seen_date": "suggested_last_seen_date",
    })
)

review = excluded.copy()
review["ticker_original"] = review["ticker"]
review["ticker_suggested"] = review["suggested_base_ticker"]
review["candidate_exists_in_upper"] = review["ticker_suggested"].isin(set(upper["ticker"].dropna().tolist()))
review = review.merge(upper_lookup, on="ticker_suggested", how="left")
review = review.merge(full_lookup, on="ticker_suggested", how="left")

review["manual_review_status"] = "pending"
review["review_note"] = pd.NA

ordered_cols = [c for c in [
    "ticker_original",
    "ticker_suggested",
    "candidate_exists_in_upper",
    "exclude_reason",
    "full_name",
    "request_date",
    "msg",
    "suggested_name_upper",
    "suggested_status_upper",
    "suggested_primary_exchange_upper",
    "suggested_entity_id_upper",
    "suggested_name_full",
    "suggested_status_full",
    "suggested_primary_exchange_full",
    "suggested_entity_id_full",
    "suggested_first_seen_date",
    "suggested_last_seen_date",
    "manual_review_status",
    "review_note",
    "out_file",
    "endswith_w",
    "endswith_ws",
    "endswith_u",
    "endswith_r",
    "contains_dot",
    "contains_slash",
    "suggested_base_exists_in_upper",
    "ticker",
    "suggested_base_ticker",
] if c in review.columns]

review = review[ordered_cols].sort_values(["ticker_original"]).reset_index(drop=True)

review.to_parquet(REVIEW_PARQUET, index=False)
review.to_csv(REVIEW_CSV, index=False, encoding="utf-8")

summary = {
    "rows_total": int(len(review)),
    "candidate_exists_in_upper": int(review["candidate_exists_in_upper"].fillna(False).sum()) if "candidate_exists_in_upper" in review.columns else 0,
    "paths": {
        "review_parquet": str(REVIEW_PARQUET),
        "review_csv": str(REVIEW_CSV),
        "excluded_parquet": str(EXCLUDED_PARQUET),
        "universe_upper": str(UNIVERSE_UPPER),
        "universe_full": str(UNIVERSE_FULL),
    },
}
SUMMARY_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("tickers_2005_2026_upper_remapped_review build completed")
print("REVIEW_PARQUET:", REVIEW_PARQUET)
print("REVIEW_CSV:", REVIEW_CSV)
print("SUMMARY_JSON:", SUMMARY_JSON)
print("rows_total:", len(review))
