import json
import os
from pathlib import Path

from .fsutil import mkdir_p


class State:
    """URL → last_modified map persisted as JSON.

    Loaded once at startup; updated atomically after each successful fetch
    so a mid-run crash leaves the file consistent.
    """

    def __init__(self, path: Path) -> None:
        self._path: Path = path
        self._data: dict[str, str | None] = {}

    def load(self) -> None:
        """Populate in-memory state from disk; no-op when no state file exists yet."""
        if self._path.exists():
            self._data = json.loads(self._path.read_text())

    def is_current(self, url: str, last_modified: str | None) -> bool:
        """True when url was previously fetched at the same Last-Modified value."""
        return url in self._data and self._data[url] == last_modified

    def record(self, url: str, last_modified: str | None) -> None:
        """Persist a successful fetch so future runs can skip unchanged files."""
        self._data[url] = last_modified
        mkdir_p(self._path.parent)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2))
        os.replace(tmp, self._path)  # atomic on POSIX
