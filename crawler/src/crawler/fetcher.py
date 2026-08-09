import asyncio
import logging
import random

import aiohttp

from .settings import BASE_BACKOFF, MAX_BACKOFF, MAX_RETRIES, USER_AGENT

logger = logging.getLogger(__name__)


def make_session() -> aiohttp.ClientSession:
    """Create the shared HTTP session with the crawler User-Agent header."""
    return aiohttp.ClientSession(headers={"User-Agent": USER_AGENT})


async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
) -> str:
    """Fetch URL as text with exponential back-off; honours 429 Retry-After."""
    async with semaphore:
        backoff = BASE_BACKOFF
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url) as resp:
                    if resp.status == 429:
                        wait = float(resp.headers.get("Retry-After", backoff))
                        wait = min(wait + random.uniform(0, 1), MAX_BACKOFF)
                        logger.warning("Rate limited on %s, retrying in %.1fs", url, wait)
                        await asyncio.sleep(wait)
                        backoff = min(backoff * 2, MAX_BACKOFF)
                        continue
                    resp.raise_for_status()
                    return await resp.text(encoding="utf-8", errors="replace")
            except aiohttp.ClientError as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                wait = min(backoff + random.uniform(0, 1), MAX_BACKOFF)
                logger.warning(
                    "Error fetching %s (attempt %d/%d): %s, retrying in %.1fs",
                    url, attempt + 1, MAX_RETRIES, exc, wait,
                )
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, MAX_BACKOFF)
        raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
