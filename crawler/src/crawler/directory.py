import re
from dataclasses import dataclass

import aiohttp

# Domain prefix used to turn relative hrefs from the BLS HTML listing into absolute URLs
_BASE: str = "https://download.bls.gov"

# Matches one file row in the BLS IIS directory listing, e.g.:
#   8/6/2026  8:30 AM     102 <A HREF="/pub/.../pr.class">pr.class</A>
_ENTRY_RE: re.Pattern[str] = re.compile(
    r'(\d+/\d+/\d{4}\s+\d+:\d+\s+[AP]M)\s+(\d+)\s+'
    r'<[Aa]\s+[Hh][Rr][Ee][Ff]="(/pub/time\.series/pr/[^"]+)">([^<]+)</[Aa]>',
)


@dataclass
class FileEntry:
    """One downloadable file from the BLS directory listing."""

    url: str
    filename: str
    last_modified: str
    size_bytes: int

# TODO(prod): research if there's easier way to access the files (ie. direct FTP listing)
def parse_directory_html(html: str) -> list[FileEntry]:
    """Extract file entries from a BLS IIS directory listing page."""
    entries = []
    for m in _ENTRY_RE.finditer(html):
        last_modified, size, href, filename = (
            m.group(1), m.group(2), m.group(3), m.group(4)
        )
        entries.append(FileEntry(
            url=f"{_BASE}{href}",
            filename=filename.strip(),
            last_modified=last_modified.strip(),
            size_bytes=int(size),
        ))
    return entries


async def fetch_directory(
    session: aiohttp.ClientSession, base_url: str
) -> list[FileEntry]:
    """Fetch the BLS directory page and return its parsed file entries."""
    async with session.get(base_url) as resp:
        resp.raise_for_status()
        html = await resp.text()
    return parse_directory_html(html)
