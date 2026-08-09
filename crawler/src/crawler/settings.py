# BLS productivity time-series directory; all file hrefs in the listing are relative to this domain
BLS_DIRECTORY_URL: str = "https://download.bls.gov/pub/time.series/pr/"

# BLS terms of use require identifying the requestor in every HTTP request
USER_AGENT: str = "rearc-quest/1.0 (mail.hrapp@googlemail.com)"

# Retry / back-off tunables for transient HTTP errors and 429s
BASE_BACKOFF: float = 1.0   # initial wait in seconds before first retry
MAX_BACKOFF: float = 60.0   # ceiling so exponential growth doesn't stall the run
MAX_RETRIES: int = 5

# Metadata / contact files that appear in the BLS listing but carry no series data
SKIP_FILES: frozenset[str] = frozenset({"pr.contacts", "pr.txt"})

# DataUSA ACS total-population series: Year × Nation (single page at current data volume)
POPULATION_URL: str = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
# Extension-free to match BLS enrichment naming convention (e.g. pr.class, pr.measure)
POPULATION_FILENAME: str = "us_population"
