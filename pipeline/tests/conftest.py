import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent / "dlt"))


@pytest.fixture(scope="session")
def spark():
    try:
        return (
            SparkSession.builder
            .master("local[1]")
            .appName("bls-pipeline-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(f"Spark unavailable (Java not installed?): {e}")
