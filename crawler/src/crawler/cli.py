import asyncio
import logging
import threading
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, Optional

import typer

from .directory import FileEntry, fetch_directory
from .fetcher import make_session
from .loader import load_all, load_backfill, load_current, load_enrichments, load_series
from .settings import BLS_DIRECTORY_URL
from .state import State

app = typer.Typer(add_completion=False, help="BLS time series file crawler")

# Loader signature shared by all category loader functions
LoaderFunction = Callable[
    [list[FileEntry], Any, asyncio.Semaphore, State, Path],
    Coroutine[Any, Any, None],
]

# Dispatches CLI category names to their loader functions
_LOADERS: dict[str, LoaderFunction] = {
    "current": load_current,
    "backfill": load_backfill,
    "series": load_series,
    "enrichment": load_enrichments,
}

# Derived once so the CLI help text and validation stay in sync with _LOADERS
CATEGORIES: list[str] = list(_LOADERS.keys())


async def _crawl(output: Path, state_file: Path, parallelism: int, category: str | None) -> None:
    """Fetch the BLS directory listing, then run the selected loader(s) in parallel."""
    state = State(state_file)
    state.load()
    semaphore = asyncio.Semaphore(parallelism)
    async with make_session() as session:
        entries = await fetch_directory(session, BLS_DIRECTORY_URL)
        if category is None:
            await load_all(entries, session, semaphore, state, output)
        else:
            await _LOADERS[category](entries, session, semaphore, state, output)
    # aiohttp schedules SSL transport finalisation as event-loop callbacks after session.close();
    # yielding here lets those callbacks run before asyncio.run() tears down the loop.
    await asyncio.sleep(0)


@app.command()
def sync(
    category: Optional[str] = typer.Argument(
        None,
        help=f"Category to sync: {', '.join(CATEGORIES)}. Omit to sync all.",
    ),
    output: Path = typer.Option(Path("data"), help="Output directory for downloaded files"),
    state_file: Path = typer.Option(Path("crawler_state.json"), help="JSON state file path"),
    parallelism: int = typer.Option(5, help="Max concurrent downloads"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Fetch new and updated files from the BLS directory."""
    if category is not None and category not in _LOADERS:
        raise typer.BadParameter(
            f"Unknown category '{category}'. Choose from: {', '.join(CATEGORIES)}"
        )
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # Run in a dedicated thread so asyncio.run() works even when the caller
    # already has a running event loop (e.g. Databricks serverless / IPython).
    exc: list[BaseException] = []

    def _run() -> None:
        try:
            asyncio.run(_crawl(output, state_file, parallelism, category))
        except BaseException as e:
            exc.append(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join()
    if exc:
        raise exc[0]


def main() -> None:
    # Databricks serverless runtime calls the entry point inside an IPython kernel that
    # (a) already has a running event loop, and (b) treats SystemExit(0) as a task failure.
    # standalone_mode=False prevents Typer from calling sys.exit() on success.
    try:
        app(standalone_mode=False)
    except SystemExit as e:
        if e.code not in (0, None):
            raise
