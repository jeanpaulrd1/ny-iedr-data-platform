"""Shared pytest fixtures for NY IEDR data platform tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Get the active SparkSession for testing.
    
    Session-scoped fixture that reuses the existing Databricks SparkSession.
    On Databricks Serverless, a SparkSession is always active and available.
    
    Note: This fixture does NOT create a new session - it uses the one
    provided by the Databricks runtime environment.
    """
    # Get the active session (available in Databricks notebooks and compute)
    active_session = SparkSession.getActiveSession()
    
    if active_session is None:
        raise RuntimeError(
            "No active SparkSession found. "
            "Tests must be run on Databricks compute where a SparkSession is pre-configured."
        )
    
    yield active_session
    
    # Don't stop the session - it's managed by Databricks
