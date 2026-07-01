"""Shared pytest fixtures for NY IEDR data platform tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing.
    
    Session-scoped fixture that creates a single SparkSession
    for all tests to share, improving test performance.
    """
    spark = SparkSession.builder \
        .appName("ny-iedr-test") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark
    
    # Cleanup after all tests complete
    spark.stop()
