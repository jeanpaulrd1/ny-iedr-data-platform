"""Unit tests for Bronze layer ingestion logic.

Tests verify the correctness of Bronze layer ingestion patterns:
- Auto Loader configuration and schema inference
- File path extraction and utility identification
- Lineage column generation
- UTF-8 BOM handling in raw data
- NULL sentinel normalization
- Data quality expectations

Note: These test the helper logic; full Auto Loader execution
requires DLT runtime integration testing.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime, date

# Import test fixtures
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.utils.helpers import (
    strip_utf8_bom,
    normalize_null_sentinels,
    add_lineage_columns
)


class TestAutoLoaderPatterns:
    """Tests for Auto Loader file ingestion patterns."""
    
    def test_file_path_extraction(self, spark):
        """Test extracting utility_id from file path."""
        df = spark.createDataFrame([
            ("/Volumes/dev/bronze/landing/utility1/circuits/file.csv",),
        ], ["_input_file"])
        
        # Split path and extract utility_id (after 'landing')
        result = df.withColumn(
            "utility_id",
            F.split(F.col("_input_file"), "/")[5]
        )
        
        row = result.collect()[0]
        assert row.utility_id == "utility1"
    
    def test_ingestion_date_extraction(self, spark):
        """Test extracting ingestion date from timestamp."""
        df = spark.createDataFrame([
            ("data1",),
        ], ["value"])
        
        result = df.withColumn("ingestion_date", F.current_date())
        
        assert "ingestion_date" in result.columns
        row = result.collect()[0]
        assert row.ingestion_date is not None


class TestSchemaInference:
    """Tests for schema inference from raw files."""
    
    def test_infers_string_columns(self, spark):
        """Test that CSV columns are inferred as strings by default."""
        # Simulate Auto Loader inferring all columns as STRING initially
        df = spark.createDataFrame([
            ("circuit1", "10.5", "8.0"),
        ], ["circuit_id", "max_capacity", "min_capacity"])
        
        # Check all columns are string type (before casting)
        for col_name, dtype in df.dtypes:
            assert dtype == "string"
    
    def test_handles_empty_values(self, spark):
        """Test that empty strings are preserved (before NULL normalization)."""
        df = spark.createDataFrame([
            ("circuit1", "", ""),
        ], ["circuit_id", "field1", "field2"])
        
        row = df.collect()[0]
        assert row.field1 == ""
        assert row.field2 == ""


class TestUtf8BomHandling:
    """Tests for UTF-8 BOM (Byte Order Mark) handling."""
    
    def test_strips_bom_from_column_names(self, spark):
        """Test that BOM character is removed from column names."""
        # Simulate BOM prefix on first column (common with Excel-exported CSVs)
        df = spark.createDataFrame([
            ("circuit1", "10.5"),
        ], ["\ufeffCircuits_Phase3_CIRCUIT", "NYHCPV_csv_FMAXHC"])
        
        result = strip_utf8_bom(df)
        
        # BOM should be stripped
        assert "Circuits_Phase3_CIRCUIT" in result.columns
        assert "\ufeffCircuits_Phase3_CIRCUIT" not in result.columns
    
    def test_preserves_non_bom_columns(self, spark):
        """Test that columns without BOM are unchanged."""
        df = spark.createDataFrame([
            ("circuit1", "10.5"),
        ], ["Circuits_Phase3_CIRCUIT", "NYHCPV_csv_FMAXHC"])
        
        result = strip_utf8_bom(df)
        
        # Columns should remain the same
        assert set(result.columns) == set(df.columns)


class TestLineageColumns:
    """Tests for lineage metadata generation."""
    
    def test_adds_ingestion_timestamp(self, spark):
        """Test that ingestion_timestamp is auto-generated."""
        df = spark.createDataFrame([
            ("feeder1", "10.5"),
        ], ["feeder_id", "capacity"])
        
        result = add_lineage_columns(df)
        
        assert "ingestion_timestamp" in result.columns
        row = result.collect()[0]
        # Verify timestamp is a datetime (auto-generated, can't predict exact value)
        assert row.ingestion_timestamp is not None
        assert isinstance(row.ingestion_timestamp, datetime)
    
    def test_adds_ingestion_date(self, spark):
        """Test that ingestion_date is auto-generated."""
        df = spark.createDataFrame([
            ("feeder1",),
        ], ["feeder_id"])
        
        result = add_lineage_columns(df)
        
        assert "ingestion_date" in result.columns
        row = result.collect()[0]
        # Verify date is today's date (auto-generated)
        assert row.ingestion_date is not None
        assert isinstance(row.ingestion_date, date)
    
    def test_adds_pipeline_update_id(self, spark):
        """Test that pipeline_update_id is auto-generated."""
        df = spark.createDataFrame([
            ("feeder1",),
        ], ["feeder_id"])
        
        result = add_lineage_columns(df)
        
        assert "pipeline_update_id" in result.columns
        row = result.collect()[0]
        # Verify run ID format: run_YYYYMMDD_HHmmss_<hash>
        assert row.pipeline_update_id.startswith("run_")
        assert len(row.pipeline_update_id.split("_")) == 4  # run, date, time, hash
    
    def test_preserves_original_columns(self, spark):
        """Test that lineage columns don't overwrite original data."""
        df = spark.createDataFrame([
            ("feeder1", "10.5", "utility1"),
        ], ["feeder_id", "capacity", "utility_id"])
        
        result = add_lineage_columns(df)
        
        # Original columns preserved
        assert "feeder_id" in result.columns
        assert "capacity" in result.columns
        assert "utility_id" in result.columns
        
        # Lineage added
        assert "ingestion_timestamp" in result.columns
        assert "ingestion_date" in result.columns
        assert "pipeline_update_id" in result.columns


class TestNullSentinelHandling:
    """Tests for NULL sentinel normalization in raw data."""
    
    def test_normalizes_common_null_strings(self, spark):
        """Test that common NULL sentinels are converted to true NULL."""
        df = spark.createDataFrame([
            ("NULL", "null", "N/A", "n/a", "NA", ""),
        ], ["col1", "col2", "col3", "col4", "col5", "col6"])
        
        result = normalize_null_sentinels(df, ["col1", "col2", "col3", "col4", "col5", "col6"])
        row = result.collect()[0]
        
        # All should be converted to None
        assert row.col1 is None
        assert row.col2 is None
        assert row.col3 is None
        assert row.col4 is None
        assert row.col5 is None
        assert row.col6 is None
    
    def test_preserves_valid_values(self, spark):
        """Test that non-NULL sentinel values are preserved."""
        df = spark.createDataFrame([
            ("circuit1", "10.5", "utility1"),
        ], ["circuit_id", "capacity", "utility_id"])
        
        result = normalize_null_sentinels(df, ["circuit_id", "capacity", "utility_id"])
        row = result.collect()[0]
        
        # Valid values should be preserved
        assert row.circuit_id == "circuit1"
        assert row.capacity == "10.5"
        assert row.utility_id == "utility1"


class TestDataQualityExpectations:
    """Tests for DLT data quality expectations."""
    
    def test_detects_null_feeder_id(self, spark):
        """Test that NULL feeder_id violations are flagged."""
        df = spark.createDataFrame([
            ("feeder1", "10.5"),
            (None, "8.0"),  # Violation
        ], ["feeder_id", "capacity"])
        
        # Simulate DQ check
        result = df.withColumn("is_valid", F.col("feeder_id").isNotNull())
        
        rows = result.collect()
        assert rows[0].is_valid == True
        assert rows[1].is_valid == False
    
    def test_detects_invalid_capacity(self, spark):
        """Test that invalid capacity values are flagged."""
        df = spark.createDataFrame([
            ("feeder1", "10.5"),
            ("feeder2", "not_a_number"),  # Violation
        ], ["feeder_id", "capacity"])
        
        # Simulate DQ check: try casting to double using try_cast (tolerates malformed input)
        result = df.withColumn(
            "is_valid",
            F.expr("try_cast(capacity as double)").isNotNull()
        )
        
        rows = result.collect()
        assert rows[0].is_valid == True
        assert rows[1].is_valid == False


class TestMultiFileIngestion:
    """Tests for multi-file ingestion patterns."""
    
    def test_aggregates_records_from_multiple_files(self, spark):
        """Test that records from multiple files are combined."""
        df = spark.createDataFrame([
            ("/landing/utility1/circuits/file1.csv", "feeder1"),
            ("/landing/utility1/circuits/file2.csv", "feeder2"),
        ], ["_input_file", "feeder_id"])
        
        # Should have 2 records
        assert df.count() == 2
    
    def test_tracks_source_file_per_record(self, spark):
        """Test that each record tracks its source file."""
        df = spark.createDataFrame([
            ("/landing/utility1/circuits/file1.csv", "feeder1"),
            ("/landing/utility1/circuits/file2.csv", "feeder2"),
        ], ["_input_file", "feeder_id"])
        
        # Each record should have distinct source file
        result = df.groupBy("_input_file").agg(F.count("*").alias("record_count"))
        
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["record_count"] == 1
    
    def test_handles_multiple_utilities(self, spark):
        """Test ingestion of files from different utilities."""
        df = spark.createDataFrame([
            ("/landing/utility1/circuits/data.csv", "feeder1"),
            ("/landing/utility2/circuits/data.csv", "feeder2"),
        ], ["_input_file", "feeder_id"])
        
        result = df.withColumn(
            "utility_id",
            F.split(F.col("_input_file"), "/")[2]
        ).groupBy("utility_id").agg(F.count("*").alias("count"))
        
        rows = result.collect()
        # Convert to dict for easier assertion - use bracket notation to avoid Row.count() method
        utility_counts = {row.utility_id: row["count"] for row in rows}
        
        assert len(utility_counts) == 2
        assert utility_counts["utility1"] == 1
        assert utility_counts["utility2"] == 1
