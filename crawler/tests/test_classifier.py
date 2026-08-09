import pytest

from crawler.classifier import classify


@pytest.mark.parametrize("filename,expected", [
    ("pr.data.0.Current", "current"),
    ("pr.data.1.Current", "current"),
    ("pr.data.99.Current", "current"),
    ("pr.data.0.AllData", "backfill"),
    ("pr.data.1.AllData", "backfill"),
    ("pr.series", "series"),
    ("pr.class", "enrichment"),
    ("pr.duration", "enrichment"),
    ("pr.footnote", "enrichment"),
    ("pr.measure", "enrichment"),
    ("pr.period", "enrichment"),
    ("pr.seasonal", "enrichment"),
    ("pr.sector", "enrichment"),
    ("pr.contacts", None),
    ("pr.txt", None),
])
def test_classify(filename, expected):
    assert classify(filename) == expected
