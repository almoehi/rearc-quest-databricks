import asyncio
import json
import logging
from collections.abc import Awaitable
from pathlib import Path
from typing import Any

import aiohttp

from .classifier import classify
from .directory import FileEntry
from .fetcher import fetch_text
from .fsutil import mkdir_p
from .settings import POPULATION_FILENAME, POPULATION_URL
from .state import State

logger = logging.getLogger(__name__)


def _records_to_tsv(payload: dict[str, Any]) -> str:
    """Convert Tesseract .jsonrecords payload to TSV: .columns as header, .data rows in column order."""
    # TODO(prod): extend to support pagination based on the .page field - to load ALL data
    columns: list[str] = payload["columns"]
    header = "\t".join(columns)
    rows = [
        "\t".join("" if row[c] is None else str(row[c]) for c in columns)
        for row in payload["data"]
    ]
    return "\n".join([header, *rows]) + "\n"


async def _load_population(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    output: Path,
) -> None:
    """Fetch US population data from DataUSA; skips the write if content is unchanged."""
    logger.info("fetch %s", POPULATION_FILENAME)
    text = await fetch_text(session, POPULATION_URL, semaphore)
    tsv = _records_to_tsv(json.loads(text))

    dest = output / "enrichment" / POPULATION_FILENAME
    if dest.exists() and dest.read_text(encoding="utf-8") == tsv:
        logger.info("skip %s (unchanged)", POPULATION_FILENAME)
        return

    mkdir_p(dest.parent)
    dest.write_text(tsv, encoding="utf-8")
    logger.info("done %s", POPULATION_FILENAME)


async def _fetch_and_write(
    entry: FileEntry,
    category: str,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> None:
    """Download one BLS file and write it under output/{category}/; skips if unchanged."""
    if state.is_current(entry.url, entry.last_modified):
        logger.info("skip %s (unchanged)", entry.filename)
        return

    logger.info("fetch %s → %s/", entry.filename, category)
    content = await fetch_text(session, entry.url, semaphore)

    dest = output / category
    mkdir_p(dest)
    (dest / entry.filename).write_text(content, encoding="utf-8")

    state.record(entry.url, entry.last_modified)
    logger.info("done %s", entry.filename)


def _tasks(
    category: str,
    entries: list[FileEntry],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> list[Awaitable[None]]:
    """Build a coroutine per entry that belongs to category; ready to pass to gather."""
    return [
        _fetch_and_write(e, category, session, semaphore, state, output)
        for e in entries
        if classify(e.filename) == category
    ]


async def load_current(
    entries: list[FileEntry],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> None:
    """Sync all pr.data.*.Current files in parallel."""
    await asyncio.gather(*_tasks("current", entries, session, semaphore, state, output))


async def load_backfill(
    entries: list[FileEntry],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> None:
    """Sync all pr.data.*.AllData files in parallel."""
    await asyncio.gather(*_tasks("backfill", entries, session, semaphore, state, output))


async def load_series(
    entries: list[FileEntry],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> None:
    """Sync the pr.series inventory file."""
    await asyncio.gather(*_tasks("series", entries, session, semaphore, state, output))


async def load_enrichments(
    entries: list[FileEntry],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> None:
    """Sync all BLS lookup tables and the DataUSA population dataset in parallel."""
    await asyncio.gather(
        *_tasks("enrichment", entries, session, semaphore, state, output),
        _load_population(session, semaphore, output),
    )


async def load_all(
    entries: list[FileEntry],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    state: State,
    output: Path,
) -> None:
    """Sync all four categories concurrently."""
    await asyncio.gather(
        load_current(entries, session, semaphore, state, output),
        load_backfill(entries, session, semaphore, state, output),
        load_series(entries, session, semaphore, state, output),
        load_enrichments(entries, session, semaphore, state, output),
    )
