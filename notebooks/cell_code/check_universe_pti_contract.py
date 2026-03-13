from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
from IPython.display import display


DEFAULT_PANEL_PATH = Path(
    r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_panel_pti"
)
DEFAULT_YEAR_MIN = 2005
DEFAULT_YEAR_MAX = 2026


def unexpected_values(series: pd.Series, allowed: set) -> list:
    s = series.dropna()
    return sorted(set(s) - set(allowed))


def parse_last_updated_robust(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    # pandas version differences: try ISO8601 first, then mixed.
    try:
        parsed = pd.to_datetime(s, errors="coerce", utc=True, format="ISO8601")
    except Exception:
        parsed = pd.to_datetime(s, errors="coerce", utc=True, format="mixed")
    return parsed


def run_check(panel_path: Path, year_min: int, year_max: int) -> pd.DataFrame:
    dataset = ds.dataset(str(panel_path), format="parquet")
    available = set(dataset.schema.names)

    required_cols = [
        "ticker",
        "market",
        "locale",
        "primary_exchange",
        "type",
        "active",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "list_date",
        "delisted_utc",
        "last_updated_utc",
        "snapshot_date",
        "entity_id",
        "exchange_priority",
        "has_composite_figi",
        "has_share_class_figi",
        "has_list_date",
    ]
    cols = [c for c in required_cols if c in available]
    missing_cols = sorted(set(required_cols) - set(cols))

    d = dataset.to_table(columns=cols).to_pandas()

    # Derivadas
    d["snapshot_date"] = pd.to_datetime(d["snapshot_date"], errors="coerce")
    d["snapshot_year"] = d["snapshot_date"].dt.year
    d["snapshot_month"] = d["snapshot_date"].dt.month

    for c in ["market", "locale", "primary_exchange", "type", "currency_name", "entity_id"]:
        if c in d.columns:
            d[c] = d[c].astype("string").str.strip()

    results = []

    # 0) Esquema
    results.append(("missing_required_columns", len(missing_cols)))
    results.append(("missing_required_columns_list", missing_cols))

    # 1) Formato / nulos críticos
    bad_snapshot = int(d["snapshot_date"].isna().sum())
    bad_lastupd = int(parse_last_updated_robust(d["last_updated_utc"]).isna().sum())
    null_entity = int(d["entity_id"].isna().sum())
    results += [
        ("bad_snapshot_date_parse", bad_snapshot),
        ("bad_last_updated_utc_parse", bad_lastupd),
        ("null_entity_id", null_entity),
    ]

    # 2) Dominio esperado
    allowed_ex = {"XNAS", "XNYS", "XASE", "ARCX"}
    allowed_prio = {0, 1, 2, 3, 999}
    results += [
        ("bad_market", int((d["market"] != "stocks").sum())),
        ("bad_locale", int((d["locale"] != "us").sum())),
        ("bad_type", int((d["type"] != "CS").sum())),
        ("bad_exchange", int((~d["primary_exchange"].isin(list(allowed_ex))).sum())),
        ("bad_currency_name", int((d["currency_name"] != "usd").sum())),
        ("null_active", int(d["active"].isna().sum())),
        ("bad_snapshot_month", int((~d["snapshot_month"].between(1, 12)).sum())),
        ("bad_snapshot_year", int((~d["snapshot_year"].between(year_min, year_max)).sum())),
        ("bad_exchange_priority", int((~d["exchange_priority"].isin(list(allowed_prio))).sum())),
    ]

    # 3) Valores inesperados explícitos
    u_market = unexpected_values(d["market"], {"stocks"})
    u_locale = unexpected_values(d["locale"], {"us"})
    u_type = unexpected_values(d["type"], {"CS"})
    u_exchange = unexpected_values(d["primary_exchange"], allowed_ex)
    u_currency = unexpected_values(d["currency_name"], {"usd"})
    u_prio = unexpected_values(d["exchange_priority"], allowed_prio)
    results += [
        ("unexpected_market_count", len(u_market)),
        ("unexpected_market_values", u_market[:20]),
        ("unexpected_locale_count", len(u_locale)),
        ("unexpected_locale_values", u_locale[:20]),
        ("unexpected_type_count", len(u_type)),
        ("unexpected_type_values", u_type[:20]),
        ("unexpected_exchange_count", len(u_exchange)),
        ("unexpected_exchange_values", u_exchange[:20]),
        ("unexpected_currency_count", len(u_currency)),
        ("unexpected_currency_values", u_currency[:20]),
        ("unexpected_exchange_priority_count", len(u_prio)),
        ("unexpected_exchange_priority_values", u_prio[:20]),
    ]

    # 4) Consistencia flags vs valores
    bad_flag_comp = int(
        (d["has_composite_figi"].astype("Int64") != d["composite_figi"].notna().astype("Int64")).sum()
    )
    bad_flag_share = int(
        (d["has_share_class_figi"].astype("Int64") != d["share_class_figi"].notna().astype("Int64")).sum()
    )
    list_date_parsed = pd.to_datetime(d["list_date"], errors="coerce")
    bad_flag_list = int((d["has_list_date"].astype("Int64") != list_date_parsed.notna().astype("Int64")).sum())
    results += [
        ("flag_vs_composite_mismatch", bad_flag_comp),
        ("flag_vs_share_class_mismatch", bad_flag_share),
        ("flag_vs_list_date_mismatch", bad_flag_list),
    ]

    # 5) Duplicados clave
    dup_snap_entity = int(d.duplicated(["snapshot_date", "entity_id"]).sum())
    tmp = d[d["composite_figi"].notna()].copy()
    dup_snap_comp = int(tmp.duplicated(["snapshot_date", "composite_figi"]).sum())
    results += [
        ("dup_snapshot_entity", dup_snap_entity),
        ("dup_snapshot_composite_figi_nonnull", dup_snap_comp),
    ]

    # 6) Patrones de formato (opcional fuerte)
    # FIGI exacto o fallback ticker|EXCHANGE (ticker permisivo; exchange 4 chars)
    entity_pat_figi = d["entity_id"].str.match(r"^BBG[0-9A-Z]{9,}$", na=False)
    entity_pat_fallback = d["entity_id"].str.match(r"^[^|]+\|[A-Z]{4}$", na=False)
    entity_bad_pattern = int((~(entity_pat_figi | entity_pat_fallback) & d["entity_id"].notna()).sum())

    cik_nonnull = d["cik"].dropna().astype("string")
    cik_bad_pattern = int((~cik_nonnull.str.match(r"^\d{10}$", na=False)).sum())

    delisted_nonnull = d["delisted_utc"].dropna()
    delisted_bad_parse = int(pd.to_datetime(delisted_nonnull, errors="coerce", utc=True).isna().sum())

    results += [
        ("entity_id_bad_pattern", entity_bad_pattern),
        ("cik_bad_pattern_nonnull", cik_bad_pattern),
        ("delisted_utc_bad_parse_nonnull", delisted_bad_parse),
    ]

    return pd.DataFrame(results, columns=["check", "value"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel-path", default=str(DEFAULT_PANEL_PATH))
    ap.add_argument("--year-min", type=int, default=DEFAULT_YEAR_MIN)
    ap.add_argument("--year-max", type=int, default=DEFAULT_YEAR_MAX)
    args, _ = ap.parse_known_args()

    out = run_check(Path(args.panel_path), args.year_min, args.year_max)
    display(out)

    def is_problem(v):
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, list):
            return len(v) > 0
        return False

    issues = out[out["value"].map(is_problem)].copy()
    print("\n=== ISSUES ===")
    if len(issues):
        display(issues)
    else:
        display(pd.DataFrame({"status": ["OK - sin issues"]}))


if __name__ == "__main__":
    main()

