# crawler

Incrementally downloads BLS Productivity & Costs time-series files and US population data from DataUSA, writing them as tab-separated files organised by category. Re-runs skip unchanged files based on `Last-Modified` headers (BLS) or content comparison (DataUSA).

## Project layout

```
crawler/
├── src/crawler/
│   ├── settings.py      # all tuneable constants and external URLs
│   ├── classifier.py    # maps BLS filenames to ingestion categories
│   ├── directory.py     # fetches and parses the BLS HTML directory listing
│   ├── fetcher.py       # HTTP GET with exponential back-off and 429 handling
│   ├── fsutil.py        # filesystem utilities (mkdir_p tolerant of UC volume paths)
│   ├── loader.py        # per-category parallel download and write logic
│   ├── state.py         # incremental state persisted as JSON (URL → last-modified)
│   └── cli.py           # Typer CLI entry point
└── tests/
    ├── test_classifier.py
    ├── test_directory.py
    └── test_state.py
```

Output structure written under `--output` (default `data/`):

```
data/
  current/      # pr.data.*.Current  — latest data chunks
  backfill/     # pr.data.*.AllData  — full historical data
  series/       # pr.series          — series inventory / index
  enrichment/   # pr.class, pr.duration, pr.footnote, pr.measure,
                # pr.period, pr.seasonal, pr.sector, us_population
```

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `aiohttp` | ≥ 3.9 | async HTTP |
| `typer` | ≥ 0.12 | CLI |
| `pytest` | ≥ 8.0 | tests (dev) |

Requires Python ≥ 3.11.

## How to run

```bash
# Sync all categories (current + backfill + series + enrichment)
uv run --project crawler crawler

# Sync a single category
uv run --project crawler crawler current
uv run --project crawler crawler backfill
uv run --project crawler crawler series
uv run --project crawler crawler enrichment

# Common options
uv run --project crawler crawler \
  --output ./data \
  --state-file crawler_state.json \
  --parallelism 5 \
  --verbose

# Run tests
uv run --project crawler pytest
```

State is written to `crawler_state.json` after each successful file fetch. Delete it (or the relevant entry) to force a re-download.
