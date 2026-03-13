# -*- coding: utf-8 -*-
# PASO 16J-TV - TradingView official widget embed (direct window in notebook)
from IPython.display import display, HTML, Markdown

try:
    import ipywidgets as widgets
except Exception:
    widgets = None


def _tv_html(symbol: str, interval: str, width: str = "100%", height: int = 720, theme: str = "light"):
    # Use a unique container id to avoid collisions when re-rendering.
    safe = symbol.replace(":", "_").replace(".", "_").replace("-", "_")
    cid = f"tv_{safe}_{interval}_{abs(hash((symbol, interval, height, theme))) % 10_000_000}"

    return f"""
    <div class="tradingview-widget-container" style="width:{width};height:{height}px;">
      <div id="{cid}" style="width:{width};height:{height}px;"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
      <script type="text/javascript">
      if (typeof TradingView !== "undefined") {{
        new TradingView.widget({{
          "autosize": true,
          "symbol": "{symbol}",
          "interval": "{interval}",
          "timezone": "Etc/UTC",
          "theme": "{theme}",
          "style": "1",
          "locale": "en",
          "toolbar_bg": "#f1f3f6",
          "enable_publishing": false,
          "allow_symbol_change": true,
          "container_id": "{cid}"
        }});
      }}
      </script>
    </div>
    """


if widgets is None:
    display(Markdown("ipywidgets no disponible. Render fijo para ejemplo `NASDAQ:ALBT`."))
    display(HTML(_tv_html("NASDAQ:ALBT", "D", "100%", 720, "light")))
else:
    exchange_dd = widgets.Dropdown(
        options=["NASDAQ", "NYSE", "AMEX", "OTC"],
        value="NASDAQ",
        description="Exchange",
    )
    ticker_txt = widgets.Text(value="ALBT", description="Ticker")
    interval_dd = widgets.Dropdown(
        options=[
            ("1m", "1"),
            ("5m", "5"),
            ("15m", "15"),
            ("1h", "60"),
            ("4h", "240"),
            ("1D", "D"),
            ("1W", "W"),
            ("1M", "M"),
        ],
        value="D",
        description="TF",
    )
    theme_dd = widgets.Dropdown(options=["light", "dark"], value="light", description="Theme")
    height_slider = widgets.IntSlider(value=720, min=420, max=1200, step=20, description="Height")
    btn = widgets.Button(description="Render TradingView")
    out = widgets.Output()

    def _run(_=None):
        with out:
            out.clear_output(wait=True)
            t = (ticker_txt.value or "").strip().upper()
            if not t:
                print("Ticker vacio.")
                return
            symbol = f"{exchange_dd.value}:{t}"
            display(Markdown(f"### TradingView | `{symbol}` | TF `{interval_dd.value}`"))
            display(
                HTML(
                    _tv_html(
                        symbol=symbol,
                        interval=interval_dd.value,
                        width="100%",
                        height=int(height_slider.value),
                        theme=theme_dd.value,
                    )
                )
            )

    btn.on_click(_run)

    display(Markdown("### Paso 16J-TV - Ventana oficial de TradingView (embed directo)"))
    display(widgets.HBox([exchange_dd, ticker_txt, interval_dd]))
    display(widgets.HBox([theme_dd, height_slider, btn]))
    display(out)
    _run()

