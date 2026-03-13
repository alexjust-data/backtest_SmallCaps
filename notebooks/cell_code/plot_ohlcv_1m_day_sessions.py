from __future__ import annotations

from pathlib import Path

import ipywidgets as widgets
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd
from IPython.display import clear_output, display


DATA_ROOT = Path(globals().get("DATA_ROOT", r"D:\ohlcv_1m"))
PARQUET_PATH = globals().get("PARQUET_PATH", None)
COMPANY_LOOKUP_PATH = Path(
    globals().get(
        "COMPANY_LOOKUP_PATH",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_all.parquet",
    )
)

DEFAULT_DAY = globals().get("DAY", None)
DEFAULT_TICKER = globals().get("TICKER", None)
DEFAULT_MONTH = globals().get("MONTH_KEY", None)
TIMEZONE = str(globals().get("TIMEZONE", "America/New_York"))

PREMARKET_START = str(globals().get("PREMARKET_START", "04:00:00"))
MARKET_START = str(globals().get("MARKET_START", "09:30:00"))
MARKET_END = str(globals().get("MARKET_END", "16:00:00"))
POSTMARKET_END = str(globals().get("POSTMARKET_END", "20:00:00"))

CHART_STYLE = str(globals().get("CHART_STYLE", "candlestick")).lower()  # candlestick | ohlc | line
SHOW_VOLUME = bool(globals().get("SHOW_VOLUME", True))
SHOW_VWAP = bool(globals().get("SHOW_VWAP", True))
FIGSIZE = tuple(globals().get("FIGSIZE", (16, 8)))


def _load_month_df(parquet_path: Path) -> pd.DataFrame:
    if not parquet_path.exists():
        raise FileNotFoundError(f"No existe parquet: {parquet_path}")

    df = pd.read_parquet(parquet_path).copy()
    if df.empty:
        raise ValueError(f"El parquet está vacío: {parquet_path}")

    required = ["ticker", "ts_utc", "date", "o", "h", "l", "c"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    df["dt_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    if df["dt_utc"].isna().all():
        raise ValueError("No se pudo parsear ts_utc")

    df["dt_local"] = df["dt_utc"].dt.tz_convert(TIMEZONE)
    df["date_local"] = df["dt_local"].dt.strftime("%Y-%m-%d")
    return df.sort_values("dt_local").reset_index(drop=True)


def _build_company_lookup(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path, columns=["ticker", "name", "snapshot_date"])
    except Exception:
        try:
            df = pd.read_parquet(path, columns=["ticker", "name"])
        except Exception:
            return {}
    if "ticker" not in df.columns or "name" not in df.columns:
        return {}
    df["ticker"] = df["ticker"].astype("string").str.strip().str.upper()
    df["name"] = df["name"].astype("string").str.strip()
    if "snapshot_date" in df.columns:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
        df = df.sort_values("snapshot_date").drop_duplicates(subset=["ticker"], keep="last")
    else:
        df = df.drop_duplicates(subset=["ticker"], keep="last")
    return {
        str(r["ticker"]): str(r["name"])
        for _, r in df.iterrows()
        if pd.notna(r["ticker"]) and pd.notna(r["name"])
    }


def _iter_tickers(data_root: Path) -> list[str]:
    if not data_root.exists():
        raise FileNotFoundError(f"No existe DATA_ROOT: {data_root}")
    out = []
    for p in sorted(data_root.glob("ticker=*")):
        if p.is_dir():
            out.append(p.name.split("=", 1)[1])
    if not out:
        raise ValueError(f"No se encontraron tickers en {data_root}")
    return out


def _month_files_for_ticker(data_root: Path, ticker: str) -> list[tuple[str, Path]]:
    ticker_dir = data_root / f"ticker={ticker}"
    out: list[tuple[str, Path]] = []
    if not ticker_dir.exists():
        return out
    for year_dir in sorted(ticker_dir.glob("year=*")):
        if not year_dir.is_dir():
            continue
        year_s = year_dir.name.split("=", 1)[1]
        for month_dir in sorted(year_dir.glob("month=*")):
            if not month_dir.is_dir():
                continue
            month_s = month_dir.name.split("=", 1)[1]
            fp = month_dir / f"minute_aggs_{ticker}_{year_s}_{month_s}.parquet"
            if fp.exists():
                out.append((f"{year_s}-{month_s}", fp))
    return out


def _session_ts(day: str, hhmmss: str) -> pd.Timestamp:
    return pd.Timestamp(f"{day} {hhmmss}", tz=TIMEZONE)


def _shade_sessions(ax: plt.Axes, day: str, x_min: pd.Timestamp, x_max: pd.Timestamp) -> None:
    sessions = [
        (_session_ts(day, PREMARKET_START), _session_ts(day, MARKET_START), "#cfe8ff"),
        (_session_ts(day, MARKET_START), _session_ts(day, MARKET_END), "#d9f2d9"),
        (_session_ts(day, MARKET_END), _session_ts(day, POSTMARKET_END), "#fff4cc"),
    ]
    for x0, x1, color in sessions:
        if x1 < x_min or x0 > x_max:
            continue
        ax.axvspan(max(x0, x_min), min(x1, x_max), color=color, alpha=0.35, zorder=0)

    for ts, label in [
        (_session_ts(day, PREMARKET_START), PREMARKET_START[:5]),
        (_session_ts(day, MARKET_START), MARKET_START[:5]),
        (_session_ts(day, MARKET_END), MARKET_END[:5]),
        (_session_ts(day, POSTMARKET_END), POSTMARKET_END[:5]),
    ]:
        if x_min <= ts <= x_max:
            ax.axvline(ts, color="gray", linestyle="--", linewidth=1)
            ax.text(ts, 1.01, label, transform=ax.get_xaxis_transform(), ha="center", va="bottom", fontsize=9, color="gray")


def _plot_ohlc(ax: plt.Axes, day_df: pd.DataFrame) -> None:
    xs = mdates.date2num(day_df["dt_local"].dt.to_pydatetime())
    width = 0.0009
    for x, o, h, l, c in zip(xs, day_df["o"], day_df["h"], day_df["l"], day_df["c"]):
        color = "#2ca02c" if c >= o else "#d62728"
        ax.vlines(x, l, h, color=color, linewidth=1.0, zorder=3)
        ax.hlines(o, x - width, x, color=color, linewidth=1.2, zorder=3)
        ax.hlines(c, x, x + width, color=color, linewidth=1.2, zorder=3)


def _plot_candlestick(ax: plt.Axes, day_df: pd.DataFrame) -> None:
    xs = mdates.date2num(day_df["dt_local"].dt.to_pydatetime())
    width = 0.0009
    for x, o, h, l, c in zip(xs, day_df["o"], day_df["h"], day_df["l"], day_df["c"]):
        color = "#2ca02c" if c >= o else "#d62728"
        ax.vlines(x, l, h, color=color, linewidth=1.0, zorder=3)
        body_low = min(o, c)
        body_h = max(abs(c - o), 1e-8)
        ax.add_patch(
            Rectangle(
                (x - width / 2.0, body_low),
                width,
                body_h,
                facecolor=color,
                edgecolor=color,
                linewidth=1.0,
                zorder=4,
            )
        )


def _build_matplotlib(day_df: pd.DataFrame, title: str) -> None:
    if SHOW_VOLUME and "v" in day_df.columns:
        fig, (ax_price, ax_vol) = plt.subplots(
            2,
            1,
            figsize=FIGSIZE,
            sharex=True,
            gridspec_kw={"height_ratios": [3, 1]},
        )
    else:
        fig, ax_price = plt.subplots(1, 1, figsize=FIGSIZE)
        ax_vol = None

    day = str(day_df["date_local"].iloc[0])
    x_min = day_df["dt_local"].min()
    x_max = day_df["dt_local"].max()

    _shade_sessions(ax_price, day, x_min, x_max)
    if ax_vol is not None:
        _shade_sessions(ax_vol, day, x_min, x_max)

    if CHART_STYLE == "line":
        ax_price.plot(day_df["dt_local"], day_df["c"], color="#1f77b4", linewidth=1.4, label="Close")
    elif CHART_STYLE == "candlestick":
        _plot_candlestick(ax_price, day_df)
    else:
        _plot_ohlc(ax_price, day_df)

    if SHOW_VWAP and "vw" in day_df.columns:
        ax_price.plot(day_df["dt_local"], day_df["vw"], color="#1f77b4", linewidth=1.2, alpha=0.9, label="VWAP")

    if ax_vol is not None:
        ax_vol.bar(day_df["dt_local"], day_df["v"], width=0.0009, color="#7f8c8d", alpha=0.85)
        ax_vol.set_ylabel("Vol")
        ax_vol.grid(True, axis="y", alpha=0.25)

    ax_price.set_title(title)
    ax_price.set_ylabel("Precio")
    ax_price.grid(True, alpha=0.25)
    if SHOW_VWAP and "vw" in day_df.columns:
        ax_price.legend(loc="upper right")

    locator = mdates.HourLocator(interval=1, tz=day_df["dt_local"].dt.tz)
    formatter = mdates.DateFormatter("%H:%M", tz=day_df["dt_local"].dt.tz)
    target_ax = ax_vol if ax_vol is not None else ax_price
    target_ax.xaxis.set_major_locator(locator)
    target_ax.xaxis.set_major_formatter(formatter)
    target_ax.set_xlabel(f"Hora local ({TIMEZONE})")

    plt.setp(target_ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    plt.show()


def show_ohlcv_1m_day_sessions(
    data_root: Path = DATA_ROOT,
    parquet_path: Path | None = PARQUET_PATH,
    default_ticker: str | None = DEFAULT_TICKER,
    default_month: str | None = DEFAULT_MONTH,
    default_day: str | None = DEFAULT_DAY,
) -> None:
    cache: dict[str, pd.DataFrame] = {}
    company_lookup = _build_company_lookup(COMPANY_LOOKUP_PATH)

    if parquet_path is not None:
        fp = Path(parquet_path)
        df = _load_month_df(fp)
        available_days = sorted(df["date_local"].dropna().unique().tolist())
        initial_day = default_day if default_day in available_days else available_days[0]
        selector = widgets.Dropdown(
            options=available_days,
            value=initial_day,
            description="Día:",
            layout=widgets.Layout(width="320px"),
        )

        def _plot_fixed(day: str) -> None:
            day_df = df[df["date_local"] == day].copy().sort_values("dt_local")
            if day_df.empty:
                print(f"No hay datos para {day}")
                return
            ticker = str(day_df["ticker"].iloc[0]).upper()
            company = company_lookup.get(ticker, "")
            title = f"{ticker} | {day} | 1m intradía"
            if company:
                title = f"{ticker} | {company} | {day} | 1m intradía"
            _build_matplotlib(day_df, title)

        out = widgets.interactive_output(_plot_fixed, {"day": selector})
        print("Path:", fp)
        print("Rows:", len(df))
        print("Columns:", list(df.columns))
        display(selector, out)
        return

    tickers = _iter_tickers(data_root)
    ticker_value = default_ticker if default_ticker in tickers else tickers[0]

    ticker_widget = widgets.Dropdown(
        options=tickers,
        value=ticker_value,
        description="Ticker:",
        layout=widgets.Layout(width="260px"),
    )
    month_widget = widgets.Dropdown(description="Mes:", layout=widgets.Layout(width="220px"))
    day_widget = widgets.Dropdown(description="Día:", layout=widgets.Layout(width="220px"))
    info_out = widgets.Output()
    plot_out = widgets.Output()

    def _load_selected_month() -> pd.DataFrame | None:
        month_file = month_widget.value
        if not month_file:
            return None
        if month_file not in cache:
            cache[month_file] = _load_month_df(Path(month_file))
        return cache[month_file]

    def _refresh_months(*_args) -> None:
        months = _month_files_for_ticker(data_root, ticker_widget.value)
        month_options = [(label, str(fp)) for label, fp in months]
        month_widget.options = month_options
        if not month_options:
            month_widget.value = None
            day_widget.options = []
            with info_out:
                clear_output(wait=True)
                print(f"Ticker: {ticker_widget.value}")
                print("No se encontraron meses disponibles")
            with plot_out:
                clear_output(wait=True)
            return
        valid_labels = {label for label, _ in month_options}
        if default_month in valid_labels:
            month_widget.value = dict(month_options)[default_month]
        else:
            month_widget.value = month_options[0][1]
        _refresh_days()

    def _refresh_days(*_args) -> None:
        df = _load_selected_month()
        if df is None or df.empty:
            day_widget.options = []
            return
        available_days = sorted(df["date_local"].dropna().unique().tolist())
        day_widget.options = available_days
        if default_day in available_days:
            day_widget.value = default_day
        else:
            day_widget.value = available_days[0]
        _refresh_plot()

    def _refresh_plot(*_args) -> None:
        df = _load_selected_month()
        with info_out:
            clear_output(wait=True)
            if df is None or df.empty:
                print("Sin datos para la selección actual")
                return
            company = company_lookup.get(str(ticker_widget.value).upper(), "")
            print("DATA_ROOT:", data_root)
            if company:
                print("Ticker:", f"{ticker_widget.value} | {company}")
            else:
                print("Ticker:", ticker_widget.value)
            print("Mes:", month_widget.label)
            print("Path:", month_widget.value)
            print("Rows:", len(df))
            print("Columns:", list(df.columns))

        with plot_out:
            clear_output(wait=True)
            if df is None or df.empty or not day_widget.value:
                print("No hay datos para graficar")
                return
            day_df = df[df["date_local"] == day_widget.value].copy().sort_values("dt_local")
            if day_df.empty:
                print(f"No hay datos para {day_widget.value}")
                return
            ticker = str(day_df["ticker"].iloc[0]).upper()
            company = company_lookup.get(ticker, "")
            title = f"{ticker} | {day_widget.value} | 1m intradía"
            if company:
                title = f"{ticker} | {company} | {day_widget.value} | 1m intradía"
            _build_matplotlib(day_df, title)

    ticker_widget.observe(_refresh_months, names="value")
    month_widget.observe(_refresh_days, names="value")
    day_widget.observe(_refresh_plot, names="value")

    _refresh_months()
    display(widgets.HBox([ticker_widget, month_widget, day_widget]), info_out, plot_out)


show_ohlcv_1m_day_sessions()
