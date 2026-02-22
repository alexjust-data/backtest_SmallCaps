from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import polars as pl

# Quotes: quotes_p95/<SYMBOL>/year=YYYY/month=MM/day=DD/quotes.parquet
QUOTES_RE = re.compile(
    r"^(?P<quoteset>quotes_p95|quotes_p95_2004_2018|quotes_p95_2019_2025)/(?P<symbol>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/quotes\.parquet$"
)

# Trades: trades_ticks_YYYY_YYYY/<SYMBOL>/year=YYYY/month=MM/day=YYYY-MM-DD/<session>.parquet
TRADES_RE = re.compile(
    r"^(?P<dataset>trades_ticks_(?:2004_2018|2019_2025))/(?P<symbol>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{4}-\d{2}-\d{2})/(?P<session>[^/]+)\.parquet$"
)


@dataclass
class CalendarBuildResult:
    dates: pd.DatetimeIndex
    source: str
    notes: List[str]


def build_expected_trading_days(
    start_date: str,
    end_date: str,
    calendar_name: str = "XNYS",
    strict_official: bool = False,
) -> CalendarBuildResult:
    """
    Build expected market trading days.

    If exchange_calendars is installed, uses official exchange schedule.
    Otherwise falls back to pandas business days and records a warning.
    strict_official=True will raise if official calendar package is missing.
    """
    notes: List[str] = []
    try:
        import exchange_calendars as xc  # type: ignore

        cal = xc.get_calendar(calendar_name)
        sched = cal.schedule.loc[start_date:end_date]
        dates = pd.DatetimeIndex(sched.index.tz_localize(None).date)
        return CalendarBuildResult(
            dates=pd.DatetimeIndex(dates).normalize(),
            source=f"exchange_calendars:{calendar_name}",
            notes=notes,
        )
    except Exception as exc:
        if strict_official:
            raise RuntimeError(
                "Official calendar required but exchange_calendars is unavailable"
            ) from exc

        notes.append(
            "FALLBACK_IN_USE: using pandas business-day calendar (not official exchange schedule)."
        )
        dates = pd.date_range(start_date, end_date, freq="B").normalize()
        return CalendarBuildResult(
            dates=dates,
            source="pandas_business_day_fallback",
            notes=notes,
        )


def _iter_relative_paths(root: Path, include_prefixes: Tuple[str, ...]) -> Iterable[str]:
    for fp in root.rglob("*.parquet"):
        rel = fp.relative_to(root).as_posix()
        if rel.startswith(include_prefixes):
            yield rel


def scan_observed_daily_coverage(
    data_root: Path,
    include_datasets: Tuple[str, ...] = ("quotes_p95/", "quotes_p95_2004_2018/", "quotes_p95_2019_2025/", "trades_ticks_2004_2018/", "trades_ticks_2019_2025/"),
    max_files_per_dataset: int | None = None,
    symbols: List[str] | None = None,
) -> pl.DataFrame:
    """
    Scan partitioned files and produce daily file counts by dataset/date.
    Scales well for large universes because it uses file paths only.
    """
    counters: Dict[Tuple[str, str], int] = defaultdict(int)

    symbol_set = None
    if symbols is not None:
        symbol_set = {s.upper() for s in symbols}

    seen_by_dataset: Dict[str, int] = defaultdict(int)

    for rel in _iter_relative_paths(data_root, include_datasets):
        m_q = QUOTES_RE.match(rel)
        if m_q:
            sym = m_q.group("symbol").upper()
            if symbol_set is not None and sym not in symbol_set:
                continue
            ds = "quotes_p95"
            if max_files_per_dataset is not None and seen_by_dataset[ds] >= max_files_per_dataset:
                continue
            d = f"{m_q.group('year')}-{m_q.group('month')}-{m_q.group('day')}"
            counters[(ds, d)] += 1
            seen_by_dataset[ds] += 1
            continue

        m_t = TRADES_RE.match(rel)
        if m_t:
            sym = m_t.group("symbol").upper()
            if symbol_set is not None and sym not in symbol_set:
                continue
            ds = m_t.group("dataset")
            if max_files_per_dataset is not None and seen_by_dataset[ds] >= max_files_per_dataset:
                continue
            d = m_t.group("day")
            counters[(ds, d)] += 1
            seen_by_dataset[ds] += 1
            continue

    rows = [
        {"dataset": ds, "date": dt, "file_count": cnt}
        for (ds, dt), cnt in counters.items()
    ]
    if not rows:
        return pl.DataFrame(schema={"dataset": pl.Utf8, "date": pl.Date, "file_count": pl.Int64})

    out = pl.DataFrame(rows).with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
    return out.sort(["dataset", "date"])


def reconcile_expected_vs_observed(
    expected_dates: pd.DatetimeIndex,
    observed_daily: pl.DataFrame,
    dataset: str,
) -> dict:
    ds = observed_daily.filter(pl.col("dataset") == dataset)
    obs_dates = set(ds["date"].to_list())

    exp_dates = set(pd.to_datetime(expected_dates).date)
    overlap = exp_dates & obs_dates

    missing = sorted(exp_dates - obs_dates)
    extra = sorted(obs_dates - exp_dates)

    coverage_ratio = float(len(overlap) / len(exp_dates)) if exp_dates else 0.0

    return {
        "dataset": dataset,
        "expected_n_days": int(len(exp_dates)),
        "observed_n_days": int(len(obs_dates)),
        "overlap_n_days": int(len(overlap)),
        "coverage_ratio": coverage_ratio,
        "missing_n_days": int(len(missing)),
        "extra_n_days": int(len(extra)),
        "missing_sample": [str(x) for x in missing[:20]],
        "extra_sample": [str(x) for x in extra[:20]],
    }


def status_from_coverage(coverage_ratio: float, missing_n: int, extra_n: int) -> str:
    # Conservative gate for production readiness.
    if coverage_ratio < 0.98 or missing_n > 20:
        return "FAIL"
    if coverage_ratio < 0.995 or missing_n > 5 or extra_n > 5:
        return "WARN"
    return "PASS"
