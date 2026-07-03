"""Shared pytest fixtures for NY IEDR data platform tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Get or create a SparkSession for testing.
    
    Session-scoped fixture that reuses the existing Databricks SparkSession
    on Serverless compute (Spark Connect) or creates a local session for
    development environments.
    
    Note: On Databricks Serverless, DO NOT set .master() as Spark Connect
    is already configured and cannot coexist with a local master.
    """
    # Get existing session or create one (works on Databricks Serverless + local)
    spark = SparkSession.builder \
        .appName("ny-iedr-test") \
        .getOrCreate()
    
    yield spark
    
    # Don't stop the session on Databricks (it's shared with the cluster)
    # Only stop if running locally
    if spark.conf.get("spark.master", "").startswith("local"):
        spark.stop()
