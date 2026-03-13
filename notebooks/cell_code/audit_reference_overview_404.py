from pathlib import Path
import pandas as pd


REFERENCE_ROOT = Path(globals().get("REFERENCE_ROOT", r"D:\reference"))
UNIVERSE_PATH = Path(
    globals().get(
        "UNIVERSE_PATH",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_2005_2026.parquet",
    )
)
TOP_N = int(globals().get("TOP_N", 50))

AUDIT_CSV = REFERENCE_ROOT / "_run" / "download_reference_universe_polygon.audit.csv"

if not AUDIT_CSV.exists():
    raise FileNotFoundError(AUDIT_CSV)
if not UNIVERSE_PATH.exists():
    raise FileNotFoundError(UNIVERSE_PATH)

audit = pd.read_csv(AUDIT_CSV)
audit["dataset"] = audit["dataset"].astype("string")
audit["msg"] = audit["msg"].astype("string")
audit["ticker"] = audit["ticker"].astype("string").str.strip().str.upper()
audit["request_date"] = pd.to_datetime(audit["request_date"], errors="coerce")
audit["http_status"] = pd.to_numeric(audit["http_status"], errors="coerce")

ov404 = audit[(audit["dataset"] == "overview") & (audit["http_status"] == 404)].copy()
if ov404.empty:
    print("No hay overview 404")
    raise SystemExit(0)

univ = pd.read_parquet(UNIVERSE_PATH).copy()
univ["ticker"] = univ["ticker"].astype("string").str.strip().str.upper()
for c in ["first_seen_date", "last_seen_date", "snapshot_date"]:
    if c in univ.columns:
        univ[c] = pd.to_datetime(univ[c], errors="coerce")

univ_cols = [c for c in [
    "ticker",
    "status",
    "first_seen_date",
    "last_seen_date",
    "primary_exchange",
    "entity_id",
    "type",
    "active",
    "name",
    "snapshot_date",
    "composite_figi",
    "share_class_figi",
] if c in univ.columns]

univ_subset = univ[univ_cols].copy()
rename_map = {c: f"universe_{c}" for c in univ_subset.columns if c != "ticker"}
univ_subset = univ_subset.rename(columns=rename_map)

merged = ov404.merge(univ_subset, on="ticker", how="left")

merged["ticker_len"] = merged["ticker"].str.len()
merged["endswith_w"] = merged["ticker"].str.endswith("W", na=False)
merged["endswith_ws"] = merged["ticker"].str.endswith("WS", na=False)
merged["endswith_u"] = merged["ticker"].str.endswith("U", na=False)
merged["endswith_r"] = merged["ticker"].str.endswith("R", na=False)
merged["contains_dot"] = merged["ticker"].str.contains(r"\.", na=False)
merged["contains_slash"] = merged["ticker"].str.contains(r"/", na=False)
merged["in_universe"] = merged["universe_entity_id"].notna() if "universe_entity_id" in merged.columns else False

summary_flags = pd.DataFrame(
    [
        {"flag": "endswith_w", "count": int(merged["endswith_w"].sum())},
        {"flag": "endswith_ws", "count": int(merged["endswith_ws"].sum())},
        {"flag": "endswith_u", "count": int(merged["endswith_u"].sum())},
        {"flag": "endswith_r", "count": int(merged["endswith_r"].sum())},
        {"flag": "contains_dot", "count": int(merged["contains_dot"].sum())},
        {"flag": "contains_slash", "count": int(merged["contains_slash"].sum())},
        {"flag": "in_universe", "count": int(merged["in_universe"].sum())},
    ]
)

by_exchange = (
    merged.groupby("universe_primary_exchange", dropna=False)
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .reset_index(drop=True)
)

by_status = (
    merged.groupby("universe_status", dropna=False)
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .reset_index(drop=True)
)

sample_cols = [c for c in [
    "ticker",
    "request_date",
    "universe_status",
    "universe_primary_exchange",
    "universe_first_seen_date",
    "universe_last_seen_date",
    "universe_entity_id",
    "universe_name",
    "endswith_w",
    "endswith_ws",
    "endswith_u",
    "endswith_r",
    "contains_dot",
    "contains_slash",
] if c in merged.columns]

print("=== OVERVIEW 404 COUNT ===")
print(len(merged))

print("\n=== FLAGS ===")
display(summary_flags)

print("\n=== BY STATUS ===")
display(by_status)

print("\n=== BY EXCHANGE ===")
display(by_exchange)

print("\n=== TOP SAMPLE ===")
display(merged[sample_cols].sort_values(["ticker", "request_date"]).head(TOP_N))
