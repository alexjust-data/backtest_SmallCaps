from pathlib import Path
import re
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
REFERENCE_ROOT = Path(globals().get("REFERENCE_ROOT", r"D:\reference"))
OUTDIR = Path(
    globals().get(
        "OUTDIR",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti",
    )
)


AUDIT_CSV = REFERENCE_ROOT / "_run" / "download_reference_universe_polygon.audit.csv"
CLEAN_PARQUET = OUTDIR / "tickers_2005_2026_upper_clean.parquet"
EXCLUDED_PARQUET = OUTDIR / "tickers_2005_2026_upper_excluded_overview404.parquet"
REMAP_CANDIDATES_PARQUET = OUTDIR / "tickers_2005_2026_upper_remap_candidates.parquet"
SUMMARY_JSON = OUTDIR / "tickers_2005_2026_upper_clean.summary.json"


def suggest_base_ticker(ticker: str) -> str | None:
    t = str(ticker).strip().upper()
    candidates: list[str] = []

    if "." in t:
        left = t.split(".", 1)[0]
        if left:
            candidates.append(left)
        compact = re.sub(r"\.[A-Z]+$", "", t)
        if compact and compact != t:
            candidates.append(compact)

    if len(t) >= 2 and t.endswith("W"):
        candidates.append(t[:-1])
    if len(t) >= 2 and t.endswith("P"):
        candidates.append(t[:-1])
    if len(t) >= 3 and t.endswith("WI"):
        candidates.append(t[:-2])

    for c in candidates:
        c = c.strip().upper()
        if c:
            return c
    return None


if not UNIVERSE_FULL.exists():
    raise FileNotFoundError(UNIVERSE_FULL)
if not UNIVERSE_UPPER.exists():
    raise FileNotFoundError(UNIVERSE_UPPER)
if not AUDIT_CSV.exists():
    raise FileNotFoundError(AUDIT_CSV)

OUTDIR.mkdir(parents=True, exist_ok=True)

full = pd.read_parquet(UNIVERSE_FULL).copy()
upper = pd.read_parquet(UNIVERSE_UPPER).copy()
audit = pd.read_csv(AUDIT_CSV).copy()

full["ticker"] = full["ticker"].astype("string").str.strip().str.upper()
upper["ticker"] = upper["ticker"].astype("string").str.strip().str.upper()
audit["ticker"] = audit["ticker"].astype("string").str.strip().str.upper()
audit["dataset"] = audit["dataset"].astype("string")
audit["http_status"] = pd.to_numeric(audit["http_status"], errors="coerce")

overview_404 = (
    audit[(audit["dataset"] == "overview") & (audit["http_status"] == 404)]
    .loc[:, ["ticker", "request_date", "msg", "out_file"]]
    .drop_duplicates(subset=["ticker"])
    .reset_index(drop=True)
)

upper_set = set(upper["ticker"].dropna().tolist())

excluded = upper.merge(overview_404, on="ticker", how="inner").copy()
excluded["suggested_base_ticker"] = excluded["ticker"].map(suggest_base_ticker)
excluded["suggested_base_exists_in_upper"] = excluded["suggested_base_ticker"].isin(upper_set)
excluded["exclude_reason"] = "overview_404"

if "name" in full.columns:
    name_lookup = (
        full.loc[:, ["ticker", "name"]]
        .dropna(subset=["ticker"])
        .drop_duplicates(subset=["ticker"], keep="first")
        .rename(columns={"name": "full_name"})
    )
    excluded = excluded.merge(name_lookup, on="ticker", how="left")

clean = upper[~upper["ticker"].isin(set(excluded["ticker"].dropna().tolist()))].copy()

remap_candidates = excluded[excluded["suggested_base_exists_in_upper"]].copy()
remap_candidates = remap_candidates.sort_values(["ticker"]).reset_index(drop=True)

clean.to_parquet(CLEAN_PARQUET, index=False)
excluded.to_parquet(EXCLUDED_PARQUET, index=False)
remap_candidates.to_parquet(REMAP_CANDIDATES_PARQUET, index=False)

summary = {
    "universe_full_rows": int(len(full)),
    "universe_upper_rows": int(len(upper)),
    "overview_404_unique_tickers": int(overview_404["ticker"].nunique()),
    "excluded_rows": int(len(excluded)),
    "clean_rows": int(len(clean)),
    "remap_candidate_rows": int(len(remap_candidates)),
    "paths": {
        "universe_full": str(UNIVERSE_FULL),
        "universe_upper": str(UNIVERSE_UPPER),
        "audit_csv": str(AUDIT_CSV),
        "clean_parquet": str(CLEAN_PARQUET),
        "excluded_parquet": str(EXCLUDED_PARQUET),
        "remap_candidates_parquet": str(REMAP_CANDIDATES_PARQUET),
    },
}
SUMMARY_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

print("tickers_2005_2026_upper_clean build completed")
print("CLEAN_PARQUET:", CLEAN_PARQUET)
print("EXCLUDED_PARQUET:", EXCLUDED_PARQUET)
print("REMAP_CANDIDATES_PARQUET:", REMAP_CANDIDATES_PARQUET)
print("SUMMARY_JSON:", SUMMARY_JSON)
print("excluded_rows:", len(excluded))
print("clean_rows:", len(clean))
print("remap_candidate_rows:", len(remap_candidates))
