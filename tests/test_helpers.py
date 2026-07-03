"""Unit tests for helper utilities in pipelines/utils/helpers.py."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Import helpers - adjust path based on test execution context
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.utils.helpers import (
    add_lineage_columns,
    normalize_null_sentinels,
    strip_utf8_bom
)


class TestStripUtf8Bom:
    """Tests for strip_utf8_bom function."""
    
    def test_removes_bom_from_single_column(self, spark):
        """Test that UTF-8 BOM is removed from column names."""
        df = spark.createDataFrame([("value1",)], ["\ufeffDER_ID"])
        result = strip_utf8_bom(df)
        
        assert result.columns == ["DER_ID"]
        assert result.count() == 1
    
    def test_removes_bom_from_multiple_columns(self, spark):
        """Test BOM removal from multiple columns."""
        df = spark.createDataFrame(
            [("val1", "val2", "val3")],
            ["\ufeffcol1", "\ufeffcol2", "col3"]
        )
        result = strip_utf8_bom(df)
        
        assert result.columns == ["col1", "col2", "col3"]
    
    def test_no_bom_unchanged(self, spark):
        """Test that columns without BOM are unchanged."""
        df = spark.createDataFrame([("value1",)], ["DER_ID"])
        result = strip_utf8_bom(df)
        
        assert result.columns == ["DER_ID"]
        assert result.count() == 1
    
    def test_empty_dataframe(self, spark):
        """Test BOM removal on empty DataFrame."""
        schema = StructType([StructField("\ufeffDER_ID", StringType(), True)])
        df = spark.createDataFrame([], schema)
        result = strip_utf8_bom(df)
        
        assert result.columns == ["DER_ID"]
        assert result.count() == 0


class TestNormalizeNullSentinels:
    """Tests for normalize_null_sentinels function."""
    
    def test_converts_all_null_sentinels(self, spark):
        """Test that all null sentinel strings are converted to NULL."""
        df = spark.createDataFrame([
            ("valid", "NULL", "null", "N/A", "NA", ""),
        ], ["col1", "col2", "col3", "col4", "col5", "col6"])
        
        result = normalize_null_sentinels(df, ["col2", "col3", "col4", "col5", "col6"])
        row = result.collect()[0]
        
        # col1 unchanged
        assert row.col1 == "valid"
        # All null sentinels converted to NULL
        assert row.col2 is None  # "NULL"
        assert row.col3 is None  # "null"
        assert row.col4 is None  # "N/A"
        assert row.col5 is None  # "NA"
        assert row.col6 is None  # ""
    
    def test_preserves_valid_values(self, spark):
        """Test that non-sentinel values are preserved."""
        df = spark.createDataFrame([
            ("value1", "value2", "123"),
        ], ["col1", "col2", "col3"])
        
        result = normalize_null_sentinels(df, ["col1", "col2", "col3"])
        row = result.collect()[0]
        
        assert row.col1 == "value1"
        assert row.col2 == "value2"
        assert row.col3 == "123"
    
    def test_empty_columns_list(self, spark):
        """Test with empty columns list (no-op)."""
        df = spark.createDataFrame([("NULL",)], ["col1"])
        result = normalize_null_sentinels(df, [])
        
        # No transformation applied
        assert result.collect()[0].col1 == "NULL"
    
    def test_mixed_null_and_valid_values(self, spark):
        """Test DataFrame with mix of nulls and valid values."""
        df = spark.createDataFrame([
            ("value", "NULL"),
            ("NA", "valid"),
            ("", "123"),
        ], ["col1", "col2"])
        
        result = normalize_null_sentinels(df, ["col1", "col2"])
        rows = result.collect()
        
        assert rows[0].col1 == "value"
        assert rows[0].col2 is None
        assert rows[1].col1 is None
        assert rows[1].col2 == "valid"
        assert rows[2].col1 is None
        assert rows[2].col2 == "123"


class TestAddLineageColumns:
    """Tests for add_lineage_columns function."""
    
    def test_adds_core_lineage_columns(self, spark):
        """Test that core lineage columns are added."""
        df = spark.createDataFrame([("data1",)], ["value"])
        
        result = add_lineage_columns(df, source_file_col=None)
        
        # Check core columns exist
        assert "ingestion_timestamp" in result.columns
        assert "ingestion_date" in result.columns
        assert "pipeline_update_id" in result.columns
        
        # Check pipeline_update_id format
        row = result.collect()[0]
        assert row.pipeline_update_id.startswith("run_")
        assert len(row.pipeline_update_id) == 30  # run_YYYYMMDD_HHmmss_<8char>
    
    def test_bronze_layer_with_source_file(self, spark):
        """Test lineage columns for Bronze layer (with source_file)."""
        # Create DataFrame with _metadata columns
        df = spark.createDataFrame([("data1",)], ["value"])
        df = df.withColumn("_metadata.file_path", 
                          F.lit("/Volumes/dev/bronze/landing/utility_1/circuits/file.csv"))
        df = df.withColumn("_metadata.file_size", F.lit(1024))
        df = df.withColumn("_metadata.file_modification_time", 
                          F.lit("2024-01-01T00:00:00"))
        
        result = add_lineage_columns(df, 
                                     source_file_col="_metadata.file_path",
                                     include_file_signature=True)
        
        # Check all expected columns exist
        assert "ingestion_timestamp" in result.columns
        assert "ingestion_date" in result.columns
        assert "pipeline_update_id" in result.columns
        assert "source_file" in result.columns
        assert "utility_id" in result.columns
        assert "file_signature" in result.columns
        
        # Check utility_id extraction
        row = result.collect()[0]
        assert row.utility_id == "utility_1"
        assert len(row.file_signature) == 64  # SHA-256
    
    def test_utility_id_extraction_various_paths(self, spark):
        """Test utility_id extraction from various path formats."""
        test_cases = [
            ("/Volumes/dev/bronze/landing/utility_1/circuits/file.csv", "utility_1"),
            ("/Volumes/prod/bronze/landing/utility_2/der_installed/data.csv", "utility_2"),
            ("/Volumes/test/bronze/landing/con_edison/der_planned/test.csv", "con_edison"),
        ]
        
        for path, expected_utility_id in test_cases:
            df = spark.createDataFrame([("data",)], ["value"])
            df = df.withColumn("_metadata.file_path", F.lit(path))
            df = df.withColumn("_metadata.file_size", F.lit(1024))
            df = df.withColumn("_metadata.file_modification_time", F.lit("2024-01-01"))
            
            result = add_lineage_columns(df, source_file_col="_metadata.file_path")
            row = result.collect()[0]
            
            assert row.utility_id == expected_utility_id, \
                f"Path {path} should extract {expected_utility_id}, got {row.utility_id}"
    
    def test_utility_id_unknown_when_landing_missing(self, spark):
        """Test that utility_id is 'unknown' when 'landing' not in path."""
        df = spark.createDataFrame([("data",)], ["value"])
        df = df.withColumn("_metadata.file_path", 
                          F.lit("/Volumes/dev/bronze/raw/utility_1/circuits/file.csv"))
        df = df.withColumn("_metadata.file_size", F.lit(1024))
        df = df.withColumn("_metadata.file_modification_time", F.lit("2024-01-01"))
        
        result = add_lineage_columns(df, source_file_col="_metadata.file_path")
        row = result.collect()[0]
        
        assert row.utility_id == "unknown"
    
    def test_silver_gold_layer_without_source_file(self, spark):
        """Test lineage columns for Silver/Gold layer (no source_file)."""
        df = spark.createDataFrame([("data1",)], ["value"])
        
        result = add_lineage_columns(df, source_file_col=None)
        
        # Check core columns exist
        assert "ingestion_timestamp" in result.columns
        assert "ingestion_date" in result.columns
        assert "pipeline_update_id" in result.columns
        
        # Check Bronze-specific columns don't exist
        assert "source_file" not in result.columns
        assert "utility_id" not in result.columns
        assert "file_signature" not in result.columns
    
    def test_file_signature_without_metadata(self, spark):
        """Test file_signature when metadata columns are missing (uses defaults)."""
        df = spark.createDataFrame([("data",)], ["value"])
        df = df.withColumn("_metadata.file_path", 
                          F.lit("/Volumes/dev/bronze/landing/utility_1/circuits/file.csv"))
        # No file_size or file_modification_time columns
        
        result = add_lineage_columns(df, 
                                     source_file_col="_metadata.file_path",
                                     include_file_signature=True)
        
        # Should not fail, should use defaults
        row = result.collect()[0]
        assert len(row.file_signature) == 64  # SHA-256


# Run tests with: pytest tests/test_helpers.py -v
