from .settings import SKIP_FILES


def classify(filename: str) -> str | None:
    """Map a BLS filename to its ingestion category, or None to skip."""
    if filename in SKIP_FILES:
        return None
    parts = filename.split(".")
    if len(parts) >= 4 and parts[0] == "pr" and parts[1] == "data":
        if parts[-1] == "Current":
            return "current"
        if parts[-1] == "AllData":
            return "backfill"
    if filename == "pr.series":
        return "series"
    return "enrichment"
