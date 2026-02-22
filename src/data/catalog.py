from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# ---- Patterns for your R2 layout ----
# OHLCV: ohlcv_intraday_1m/<era>/<SYMBOL>/year=YYYY/month=MM/minute.parquet
OHLCV_1M_RE = re.compile(
    r"^ohlcv_intraday_1m/(?P<era>[^/]+)/(?P<symbol>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/minute\.parquet$"
)

# Quotes: quotes_p95/<SYMBOL>/year=YYYY/month=MM/day=DD/quotes.parquet
QUOTES_RE = re.compile(
    r"^(?P<quoteset>quotes_p95|quotes_test)/(?P<symbol>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/quotes\.parquet$"
)

# Trades: trades_ticks_2019_2025/<SYMBOL>/year=YYYY/month=MM/day=YYYY-MM-DD/<session>.parquet
TRADES_RE = re.compile(
    r"^(?P<era>trades_ticks_(?:2004_2018|2019_2025))/(?P<symbol>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{4}-\d{2}-\d{2})/(?P<session>[^/]+)\.parquet$"
)


@dataclass(frozen=True)
class ParsedKey:
    dataset: str            # e.g., ohlcv_intraday_1m, quotes_p95
    symbol: str
    year: int
    month: int
    day: Optional[int]      # quotes have day; ohlcv month files don't
    era: Optional[str]      # ohlcv has era like 2019_2025
    key: str                # original R2 key


def parse_r2_key(key: str) -> Optional[ParsedKey]:
    """
    Parse an R2 key into structured partition fields.
    Returns None if it doesn't match known datasets.
    """
    m = OHLCV_1M_RE.match(key)
    if m:
        return ParsedKey(
            dataset="ohlcv_intraday_1m",
            symbol=m.group("symbol"),
            year=int(m.group("year")),
            month=int(m.group("month")),
            day=None,
            era=m.group("era"),
            key=key,
        )

    m = QUOTES_RE.match(key)
    if m:
        return ParsedKey(
            dataset=m.group("quoteset"),
            symbol=m.group("symbol"),
            year=int(m.group("year")),
            month=int(m.group("month")),
            day=int(m.group("day")),
            era=None,
            key=key,
        )

    m = TRADES_RE.match(key)
    if m:
        day_str = m.group("day")
        day_int = int(day_str.split("-")[2])
        return ParsedKey(
            dataset=m.group("era"),
            symbol=m.group("symbol"),
            year=int(m.group("year")),
            month=int(m.group("month")),
            day=day_int,
            era=m.group("era").replace("trades_ticks_", ""),
            key=key,
        )

    return None
