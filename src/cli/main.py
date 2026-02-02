import sys
import typer
from rich import print

from src.core.settings import settings

app = typer.Typer(help="Trading Backtester CLI")


@app.command()
def doctor():
    """Quick environment sanity checks."""
    print("[bold]Environment OK[/bold]")
    print(
        {
            "R2_BUCKET": settings.R2_BUCKET,
            "R2_ENDPOINT": settings.R2_ENDPOINT,
            "DATA_CACHE_DIR": settings.DATA_CACHE_DIR,
            "RUNS_DIR": settings.RUNS_DIR,
            "PROJECT_TZ": settings.PROJECT_TZ,
        }
    )


if __name__ == "__main__":
    # Allow `python -m src.cli.main doctor` even if subcommand registration fails.
    if len(sys.argv) > 1 and sys.argv[1] == "doctor":
        doctor()
    else:
        app()
