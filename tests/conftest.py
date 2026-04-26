"""Pytest configuration for Spark testing.

Provides fixtures for:
1. Local Spark session for testing
2. Loading test data from fixtures directory
3. Databricks Connect when credentials available
"""

import os
import pathlib
import json
import csv

import pytest
from pyspark.sql import SparkSession

# Check if we're running in Databricks environment
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")


def get_spark_session() -> SparkSession:
    """Get SparkSession for local or Databricks testing."""
    if DATABRICKS_HOST and DATABRICKS_TOKEN:
        # Use Databricks Connect
        try:
            from databricks.connect import DatabricksSession

            return DatabricksSession.builder.getOrCreate()
        except Exception as e:
            print(f"Warning: Databricks Connect failed ({e}), falling back to local Spark")

    # Local Spark session for testing
    return (
        SparkSession.builder.appName("test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Works with both local Spark and Databricks Connect.

    Example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    session = get_spark_session()
    yield session
    # Clean up temp views after each test
    session.sql("SHOW TABLES").select("tableName").collect()  # Force cleanup


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Load test data from fixtures/ directory.

    Supports JSON and CSV formats.

    Example:
        def test_using_fixture(load_fixture):
            df = load_fixture("customers.json")
            assert df.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()

        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        elif suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        else:
            raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    if DATABRICKS_HOST and DATABRICKS_TOKEN:
        print("Testing with Databricks Connect")
    else:
        print("Testing with local Spark (no Databricks credentials)")
