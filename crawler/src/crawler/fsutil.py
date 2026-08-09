import errno
from pathlib import Path


def mkdir_p(path: Path) -> None:
    """mkdir -p that tolerates ENOTSUP on UC volume virtual ancestors (catalog/schema level)."""
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        if e.errno == errno.ENOTSUP:
            # UC volume virtual paths return ENOTSUP rather than EEXIST when mkdir is called
            # on the catalog or schema component of the path.  All our target dirs are exactly
            # one level inside the volume root, so creating just the leaf is sufficient.
            path.mkdir(exist_ok=True)
        else:
            raise
