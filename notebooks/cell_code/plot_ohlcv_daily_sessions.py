from __future__ import annotations

from pathlib import Path
import json

import ipywidgets as widgets
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd
from IPython.display import clear_output, display


DATA_ROOT = Path(globals().get("DATA_ROOT", r"D:\ohlcv_daily"))
PARQUET_PATH = globals().get("PARQUET_PATH", None)

DEFAULT_TICKER = globals().get("TICKER", None)
DEFAULT_YEAR = globals().get("YEAR", None)
DEFAULT_MONTH = globals().get("MONTH", None)

CHART_STYLE = str(globals().get("CHART_STYLE", "candlestick")).lower()  # candlestick | ohlc | line
SHOW_VOLUME = bool(globals().get("SHOW_VOLUME", True))
SHOW_VWAP = bool(globals().get("SHOW_VWAP", True))
FIGSIZE = tuple(globals().get("FIGSIZE", (16, 8)))
UNIVERSE_NAMES_PATH = Path(
    globals().get(
        "UNIVERSE_NAMES_PATH",
        r"C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti\tickers_all.parquet",
    )
)
TICKER_CACHE_PATH = Path(
    globals().get(
        "TICKER_CACHE_PATH",
        r"C:\Users\AlexJ\.codex\memories\ohlcv_daily_tickers_index.json",
    )
)


def _load_name_map() -> dict[str, str]:
    if not UNIVERSE_NAMES_PATH.exists():
        return {}
    try:
        names_df = pd.read_parquet(UNIVERSE_NAMES_PATH, columns=["ticker", "name"])
    except Exception:
        return {}
    if names_df.empty or "ticker" not in names_df.columns or "name" not in names_df.columns:
        return {}
    names_df = names_df.dropna(subset=["ticker", "name"]).copy()
    names_df["ticker"] = names_df["ticker"].astype(str).str.upper().str.strip()
    names_df["name"] = names_df["name"].astype(str).str.strip()
    names_df = names_df.drop_duplicates(subset=["ticker"], keep="first")
    return dict(zip(names_df["ticker"], names_df["name"]))


def _company_label(ticker: str, name_map: dict[str, str]) -> str:
    name = name_map.get(str(ticker).upper().strip(), "")
    return f"{ticker} | {name}" if name else str(ticker)


def _load_year_df(parquet_path: Path) -> pd.DataFrame:
    if not parquet_path.exists():
        raise FileNotFoundError(f"No existe parquet: {parquet_path}")

    df = pd.read_parquet(parquet_path).copy()
    if df.empty:
        raise ValueError(f"El parquet esta vacio: {parquet_path}")

    required = ["ticker", "date", "o", "h", "l", "c"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().all():
        raise ValueError("No se pudo parsear date")

    df["month_key"] = df["date"].dt.strftime("%Y-%m")
    return df.sort_values("date").reset_index(drop=True)


def _scan_tickers(data_root: Path) -> list[str]:
    if not data_root.exists():
        raise FileNotFoundError(f"No existe DATA_ROOT: {data_root}")
    tickers = []
    for p in sorted(data_root.glob("ticker=*")):
        if p.is_dir():
            tickers.append(p.name.split("=", 1)[1])
    if not tickers:
        raise ValueError(f"No se encontraron tickers en {data_root}")
    return tickers


def _load_tickers(data_root: Path) -> list[str]:
    if TICKER_CACHE_PATH.exists():
        try:
            payload = json.loads(TICKER_CACHE_PATH.read_text(encoding="utf-8"))
            if payload.get("data_root") == str(data_root):
                tickers = payload.get("tickers", [])
                if tickers:
                    return sorted(set(str(x) for x in tickers))
        except Exception:
            pass
    tickers = _scan_tickers(data_root)
    TICKER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TICKER_CACHE_PATH.write_text(
        json.dumps({"data_root": str(data_root), "tickers": tickers}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return tickers


def _year_files_for_ticker(data_root: Path, ticker: str) -> list[tuple[str, Path]]:
    ticker_dir = data_root / f"ticker={ticker}"
    out: list[tuple[str, Path]] = []
    if not ticker_dir.exists():
        return out
    for year_dir in sorted(ticker_dir.glob("year=*")):
        if not year_dir.is_dir():
            continue
        year_s = year_dir.name.split("=", 1)[1]
        fp = year_dir / f"day_aggs_{ticker}_{year_s}.parquet"
        if fp.exists():
            out.append((year_s, fp))
    return out


def _plot_ohlc(ax: plt.Axes, df: pd.DataFrame) -> None:
    xs = mdates.date2num(df["date"].dt.to_pydatetime())
    width = 0.35
    for x, o, h, l, c in zip(xs, df["o"], df["h"], df["l"], df["c"]):
        color = "#2ca02c" if c >= o else "#d62728"
        ax.vlines(x, l, h, color=color, linewidth=1.0, zorder=3)
        ax.hlines(o, x - width, x, color=color, linewidth=1.2, zorder=3)
        ax.hlines(c, x, x + width, color=color, linewidth=1.2, zorder=3)


def _plot_candlestick(ax: plt.Axes, df: pd.DataFrame) -> None:
    xs = mdates.date2num(df["date"].dt.to_pydatetime())
    width = 0.65
    for x, o, h, l, c in zip(xs, df["o"], df["h"], df["l"], df["c"]):
        color = "#2ca02c" if c >= o else "#d62728"
        body_low = min(o, c)
        body_height = max(abs(c - o), 0.001)
        ax.vlines(x, l, h, color=color, linewidth=1.0, zorder=3)
        ax.add_patch(
            Rectangle(
                (x - width / 2.0, body_low),
                width,
                body_height,
                facecolor=color,
                edgecolor=color,
                linewidth=1.0,
                zorder=4,
            )
        )


def _build_matplotlib(df: pd.DataFrame, label: str) -> None:
    if SHOW_VOLUME and "v" in df.columns:
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

    if CHART_STYLE == "line":
        ax_price.plot(df["date"], df["c"], color="#1f77b4", linewidth=1.5, label="Close")
    elif CHART_STYLE == "ohlc":
        _plot_ohlc(ax_price, df)
    else:
        _plot_candlestick(ax_price, df)

    if SHOW_VWAP and "vw" in df.columns:
        vwap = pd.to_numeric(df["vw"], errors="coerce")
        if vwap.notna().any():
            ax_price.plot(df["date"], vwap, color="#1f77b4", linewidth=1.2, alpha=0.9, label="VWAP")
            ax_price.legend(loc="upper left")

    if ax_vol is not None:
        ax_vol.bar(df["date"], df["v"], width=0.8, color="#7f8c8d", alpha=0.85)
        ax_vol.set_ylabel("Vol")
        ax_vol.grid(True, axis="y", alpha=0.25)

    ax_price.set_title(label)
    ax_price.set_ylabel("Precio")
    ax_price.grid(True, alpha=0.25)

    locator = mdates.WeekdayLocator(interval=1)
    formatter = mdates.DateFormatter("%Y-%m-%d")
    target_ax = ax_vol if ax_vol is not None else ax_price
    target_ax.xaxis.set_major_locator(locator)
    target_ax.xaxis.set_major_formatter(formatter)
    target_ax.set_xlabel("Fecha")

    plt.setp(target_ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    plt.show()


def show_ohlcv_daily_sessions(
    data_root: Path = DATA_ROOT,
    parquet_path: Path | None = PARQUET_PATH,
    default_ticker: str | None = DEFAULT_TICKER,
    default_year: str | None = str(DEFAULT_YEAR) if DEFAULT_YEAR is not None else None,
    default_month: str | None = str(DEFAULT_MONTH) if DEFAULT_MONTH is not None else None,
) -> None:
    cache: dict[str, pd.DataFrame] = {}
    name_map = _load_name_map()

    if parquet_path is not None:
        fp = Path(parquet_path)
        df = _load_year_df(fp)
        month_value = default_month if default_month in set(df["month_key"]) else df["month_key"].iloc[0]
        month_df = df[df["month_key"] == month_value].copy()
        label = f"{_company_label(df['ticker'].iloc[0], name_map)} | diario | {month_value}"
        print("Path:", fp)
        print("Rows year:", len(df))
        print("Rows month:", len(month_df))
        print("Columns:", list(df.columns))
        _build_matplotlib(month_df, label)
        return

    tickers = _load_tickers(data_root)
    initials = sorted({ticker[0] for ticker in tickers if ticker})
    ticker_by_initial = {initial: [t for t in tickers if t.startswith(initial)] for initial in initials}

    ticker_value = default_ticker if default_ticker in tickers else tickers[0]
    initial_value = ticker_value[0]

    initial_widget = widgets.Dropdown(
        options=initials,
        value=initial_value,
        description="Inicial:",
        layout=widgets.Layout(width="150px"),
    )
    ticker_widget = widgets.Dropdown(description="Ticker:", layout=widgets.Layout(width="360px"))
    year_widget = widgets.Dropdown(description="Ano:", layout=widgets.Layout(width="160px"))
    month_widget = widgets.Dropdown(description="Mes:", layout=widgets.Layout(width="180px"))
    info_out = widgets.Output()
    plot_out = widgets.Output()

    def _load_selected_year() -> pd.DataFrame | None:
        year_file = year_widget.value
        if not year_file:
            return None
        if year_file not in cache:
            cache[year_file] = _load_year_df(Path(year_file))
        return cache[year_file]

    def _refresh_tickers(*_args) -> None:
        ticker_options = ticker_by_initial.get(initial_widget.value, [])
        ticker_widget.options = ticker_options
        if not ticker_options:
            ticker_widget.value = None
            _refresh_years()
            return
        if ticker_value in ticker_options:
            ticker_widget.value = ticker_value
        else:
            ticker_widget.value = ticker_options[0]
        _refresh_years()

    def _refresh_years(*_args) -> None:
        if not ticker_widget.value:
            year_widget.options = []
            month_widget.options = []
            _refresh_plot()
            return
        years = _year_files_for_ticker(data_root, ticker_widget.value)
        year_options = [(label, str(fp)) for label, fp in years]
        year_widget.options = year_options
        if not year_options:
            year_widget.value = None
            month_widget.options = []
            month_widget.value = None
            _refresh_plot()
            return
        valid_labels = {label for label, _ in year_options}
        if default_year in valid_labels:
            year_widget.value = dict(year_options)[default_year]
        else:
            year_widget.value = year_options[0][1]
        _refresh_months()

    def _refresh_months(*_args) -> None:
        df = _load_selected_year()
        if df is None or df.empty:
            month_widget.options = []
            month_widget.value = None
            _refresh_plot()
            return
        month_keys = sorted(df["month_key"].dropna().unique().tolist())
        month_widget.options = month_keys
        month_widget.value = default_month if default_month in month_keys else month_keys[0]
        _refresh_plot()

    def _refresh_plot(*_args) -> None:
        df = _load_selected_year()
        month_key = month_widget.value
        with info_out:
            clear_output(wait=True)
            if df is None or df.empty or not month_key:
                print("Sin datos para la seleccion actual")
                return
            month_df = df[df["month_key"] == month_key].copy()
            print("DATA_ROOT:", data_root)
            print("Ticker:", _company_label(ticker_widget.value, name_map))
            print("Ano:", year_widget.label)
            print("Mes:", month_key)
            print("Path:", year_widget.value)
            print("Rows year:", len(df))
            print("Rows month:", len(month_df))
            print("Columns:", list(df.columns))
            print("Ticker cache:", TICKER_CACHE_PATH)

        with plot_out:
            clear_output(wait=True)
            if df is None or df.empty or not month_key:
                print("No hay datos para graficar")
                return
            month_df = df[df["month_key"] == month_key].copy()
            label = f"{_company_label(ticker_widget.value, name_map)} | diario | {month_key}"
            _build_matplotlib(month_df, label)

    initial_widget.observe(_refresh_tickers, names="value")
    ticker_widget.observe(_refresh_years, names="value")
    year_widget.observe(_refresh_months, names="value")
    month_widget.observe(_refresh_plot, names="value")

    _refresh_tickers()
    display(widgets.HBox([initial_widget, ticker_widget, year_widget, month_widget]), info_out, plot_out)


show_ohlcv_daily_sessions()
