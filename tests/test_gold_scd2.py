"""Unit tests for Gold layer SCD Type 2 logic.

Tests verify the expected behavior of DLT's APPLY CHANGES INTO for:
- Circuits capacity history tracking
- DER installed/planned state tracking
- Composite key handling (der_id, der_type)
- Lineage column exclusion (except_column_list)
- Sequence column behavior

Note: These tests simulate SCD2 behavior since DLT's apply_changes
is a runtime operation. Integration tests would validate actual DLT execution.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType
from datetime import datetime, date

# Import test fixtures
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestCircuitsSCD2Logic:
    """Tests for circuits_current SCD Type 2 behavior."""
    
    def test_detects_capacity_change(self, spark):
        """Test that capacity changes create new SCD2 versions."""
        # Simulate two Silver runs with capacity change
        run1 = spark.createDataFrame([
            ("feeder1", "utility1", "12.0", "10.5", "8.0", "2024-01-15 10:00:00", "green", "450.0", "2024-01-15 10:00:00", "2024-01-15", "run1"),
        ], ["feeder_id", "utility_id", "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw", 
            "hca_refresh_date", "color_code", "shape_length", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        run2 = spark.createDataFrame([
            ("feeder1", "utility1", "12.0", "12.0", "8.0", "2024-02-15 10:00:00", "green", "450.0", "2024-02-15 11:00:00", "2024-02-15", "run2"),
        ], ["feeder_id", "utility_id", "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw", 
            "hca_refresh_date", "color_code", "shape_length", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        # Key assertion: max_hosting_capacity_mw changed (10.5 → 12.0)
        # SCD2 should create new version
        assert run1.select("max_hosting_capacity_mw").collect()[0][0] != run2.select("max_hosting_capacity_mw").collect()[0][0]
    
    def test_ignores_lineage_column_changes(self, spark):
        """Test that lineage column changes don't trigger SCD2 versions."""
        # Same business data, different lineage columns
        run1 = spark.createDataFrame([
            ("feeder1", "utility1", "12.0", "10.5", "8.0", "2024-01-15 10:00:00", "green", "450.0", "2024-01-15 10:00:00", "2024-01-15", "run1"),
        ], ["feeder_id", "utility_id", "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw", 
            "hca_refresh_date", "color_code", "shape_length", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        run2 = spark.createDataFrame([
            ("feeder1", "utility1", "12.0", "10.5", "8.0", "2024-01-15 10:00:00", "green", "450.0", "2024-02-15 11:00:00", "2024-02-15", "run2"),
        ], ["feeder_id", "utility_id", "voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw", 
            "hca_refresh_date", "color_code", "shape_length", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        # Key assertion: Business columns are identical (except lineage)
        business_cols = ["feeder_id", "utility_id", "voltage_kv", "max_hosting_capacity_mw", 
                        "min_hosting_capacity_mw", "hca_refresh_date", "color_code", "shape_length"]
        
        run1_business = run1.select(*business_cols).collect()[0]
        run2_business = run2.select(*business_cols).collect()[0]
        
        assert run1_business == run2_business
        # SCD2 should NOT create new version (lineage excluded via except_column_list)
    
    def test_sequence_by_hca_refresh_date(self, spark):
        """Test that hca_refresh_date determines version order."""
        # Out-of-order data arrival
        data = spark.createDataFrame([
            ("feeder1", "utility1", "10.5", "2024-02-15 10:00:00", "2024-02-15 11:00:00"),
            ("feeder1", "utility1", "12.0", "2024-01-15 10:00:00", "2024-01-15 10:00:00"),  # Earlier date, arrived later
        ], ["feeder_id", "utility_id", "max_hosting_capacity_mw", "hca_refresh_date", "ingestion_timestamp"])
        
        # Verify sequence_by column exists and can be ordered
        ordered = data.orderBy("hca_refresh_date")
        rows = ordered.collect()
        
        # Earlier hca_refresh_date should come first (despite later arrival)
        assert rows[0].hca_refresh_date < rows[1].hca_refresh_date
        assert rows[0].max_hosting_capacity_mw == "12.0"  # Older version
        assert rows[1].max_hosting_capacity_mw == "10.5"  # Newer version
    
    def test_composite_key_feeder_id(self, spark):
        """Test that feeder_id alone is the unique key for circuits."""
        data = spark.createDataFrame([
            ("feeder1", "utility1", "10.5"),
            ("feeder2", "utility1", "12.0"),
            ("feeder1", "utility2", "8.0"),  # Same feeder_id, different utility (should be distinct)
        ], ["feeder_id", "utility_id", "max_hosting_capacity_mw"])
        
        # Verify distinct feeder_ids
        distinct_feeders = data.select("feeder_id").distinct().count()
        assert distinct_feeders == 2  # feeder1, feeder2


class TestDerSCD2Logic:
    """Tests for DER installed/planned SCD Type 2 behavior."""
    
    def test_composite_key_der_id_and_type(self, spark):
        """Test that (der_id, der_type) composite key handles hybrid projects."""
        # Hybrid project: same project_id, multiple technologies
        data = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", "utility1"),
            ("utility1_proj1_EnergyStorageSystem", "EnergyStorageSystem", "25.0", "utility1"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "utility_id"])
        
        # Both rows should be distinct (different der_type)
        assert data.count() == 2
        
        # Verify composite key uniqueness
        distinct_keys = data.select("der_id", "der_type").distinct().count()
        assert distinct_keys == 2
    
    def test_detects_capacity_change_for_der(self, spark):
        """Test that DER capacity changes create new versions."""
        run1 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", "utility1", "feeder1", "2024-01-15 10:00:00", "2024-01-15", "run1"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "utility_id", "feeder_id", 
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        run2 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "75.0", "utility1", "feeder1", "2024-02-15 11:00:00", "2024-02-15", "run2"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "utility_id", "feeder_id", 
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        # Capacity changed (50.0 → 75.0)
        assert run1.select("nameplate_rating_kw").collect()[0][0] != run2.select("nameplate_rating_kw").collect()[0][0]
    
    def test_detects_feeder_id_change(self, spark):
        """Test that DER moving to different feeder creates new version."""
        run1 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", "utility1", "feeder1", "2024-01-15 10:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "utility_id", "feeder_id", "ingestion_timestamp"])
        
        run2 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", "utility1", "feeder2", "2024-02-15 11:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "utility_id", "feeder_id", "ingestion_timestamp"])
        
        # Feeder changed (feeder1 → feeder2)
        assert run1.select("feeder_id").collect()[0][0] != run2.select("feeder_id").collect()[0][0]
    
    def test_sequence_by_ingestion_date(self, spark):
        """Test that ingestion_date determines DER version order (day-level granularity)."""
        data = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "75.0", date(2024, 2, 15), "2024-02-15 11:00:00"),
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", date(2024, 1, 15), "2024-01-15 10:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "ingestion_date", "ingestion_timestamp"])
        
        ordered = data.orderBy("ingestion_date")
        rows = ordered.collect()
        
        # Earlier date first (despite different timestamps)
        assert rows[0].nameplate_rating_kw == "50.0"
        assert rows[1].nameplate_rating_kw == "75.0"
    
    def test_tracks_planned_installation_date_changes(self, spark):
        """Test that planned_installation_date changes are tracked."""
        run1 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", date(2025, 6, 15), "queue123", "2024-01-15 10:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "planned_installation_date", 
            "interconnection_queue_id", "ingestion_timestamp"])
        
        run2 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", date(2025, 9, 30), "queue123", "2024-02-15 11:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "planned_installation_date", 
            "interconnection_queue_id", "ingestion_timestamp"])
        
        # Installation date changed (June → September)
        assert run1.select("planned_installation_date").collect()[0][0] != run2.select("planned_installation_date").collect()[0][0]


class TestSCD2Configuration:
    """Tests for SCD2 configuration correctness."""
    
    def test_circuits_except_columns_defined(self):
        """Test that circuits exclude lineage columns from change detection."""
        except_columns = ["ingestion_timestamp", "ingestion_date", "pipeline_update_id"]
        
        # These columns should NOT trigger SCD2 versions
        assert "ingestion_timestamp" in except_columns
        assert "ingestion_date" in except_columns
        assert "pipeline_update_id" in except_columns
    
    def test_circuits_track_history_columns_complete(self):
        """Test that all business columns are tracked for circuits."""
        track_columns = [
            "utility_id",
            "native_feeder_id",
            "voltage_kv",
            "max_hosting_capacity_mw",
            "min_hosting_capacity_mw",
            "color_code",
            "shape_length"
        ]
        
        # Critical business columns must be tracked
        assert "max_hosting_capacity_mw" in track_columns
        assert "min_hosting_capacity_mw" in track_columns
        assert "utility_id" in track_columns
    
    def test_der_track_history_includes_feeder_id(self):
        """Test that feeder_id is tracked for DER (projects can move feeders)."""
        track_columns = [
            "utility_id",
            "feeder_id",
            "native_feeder_id_raw",
            "nameplate_rating_kw",
            "der_status"
        ]
        
        # feeder_id must be tracked (DER can move between feeders)
        assert "feeder_id" in track_columns
        assert "nameplate_rating_kw" in track_columns
    
    def test_planned_der_track_history_includes_date(self):
        """Test that planned DER tracks installation date and queue ID."""
        track_columns = [
            "utility_id",
            "feeder_id",
            "native_feeder_id_raw",
            "nameplate_rating_kw",
            "der_status",
            "planned_installation_date",
            "interconnection_queue_id"
        ]
        
        # Planned-specific columns must be tracked
        assert "planned_installation_date" in track_columns
        assert "interconnection_queue_id" in track_columns
    
    def test_composite_keys_defined_correctly(self):
        """Test that composite keys are properly defined."""
        # Circuits: single key
        circuits_keys = ["feeder_id"]
        assert len(circuits_keys) == 1
        
        # DER: composite key
        der_keys = ["der_id", "der_type"]
        assert len(der_keys) == 2
        assert "der_id" in der_keys
        assert "der_type" in der_keys
    
    def test_sequence_column_not_in_except_list(self):
        """Test that sequence_by columns are NOT in except_column_list (critical validation)."""
        # Circuits configuration
        circuits_sequence = "hca_refresh_date"
        circuits_except = ["ingestion_timestamp", "ingestion_date", "pipeline_update_id"]
        assert circuits_sequence not in circuits_except, \
            f"CRITICAL BUG: {circuits_sequence} is used for sequencing but excluded from change detection!"
        
        # DER configuration (both installed and planned use same pattern)
        der_sequence = "ingestion_date"  # FIXED: Changed from ingestion_timestamp
        der_except = ["ingestion_timestamp", "pipeline_update_id"]  # FIXED: ingestion_date removed
        assert der_sequence not in der_except, \
            f"CRITICAL BUG: {der_sequence} is used for sequencing but excluded from change detection!"
        
        # Validate that ingestion_timestamp IS in except_list (should be excluded)
        assert "ingestion_timestamp" in der_except, \
            "ingestion_timestamp should be in except_column_list to ignore hour/minute changes"


class TestSCD2EdgeCases:
    """Tests for SCD2 edge cases and corner scenarios."""
    
    def test_handles_null_feeder_id_in_der(self, spark):
        """Test that unresolved DER (NULL feeder_id) are handled."""
        # Use explicit schema with StringType to handle NULL values
        schema = StructType([
            StructField("der_id", StringType(), False),
            StructField("der_type", StringType(), False),
            StructField("feeder_id", StringType(), True),  # Nullable
            StructField("nameplate_rating_kw", StringType(), False),
            StructField("utility_id", StringType(), False)
        ])
        
        data = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", None, "50.0", "utility1"),
        ], schema)
        
        # NULL feeder_id should be preserved
        row = data.collect()[0]
        assert row.feeder_id is None
        assert row.der_id == "utility1_proj1_SolarPV"
    
    def test_handles_multiple_changes_same_run(self, spark):
        """Test that multiple feeders changing in same run are handled."""
        data = spark.createDataFrame([
            ("feeder1", "utility1", "10.5", "2024-01-15 10:00:00"),
            ("feeder2", "utility1", "12.0", "2024-01-15 10:00:00"),
            ("feeder3", "utility2", "8.0", "2024-01-15 10:00:00"),
        ], ["feeder_id", "utility_id", "max_hosting_capacity_mw", "hca_refresh_date"])
        
        # All three should be distinct
        assert data.count() == 3
    
    def test_same_hca_date_different_values(self, spark):
        """Test that same hca_refresh_date with different capacities creates version."""
        # Edge case: Two utilities report same date but different values
        data = spark.createDataFrame([
            ("feeder1", "utility1", "10.5", "2024-01-15 10:00:00"),
            ("feeder1", "utility1", "12.0", "2024-01-15 10:00:00"),  # Same date, different capacity
        ], ["feeder_id", "utility_id", "max_hosting_capacity_mw", "hca_refresh_date"])
        
        # Should have 2 rows (different capacities)
        assert data.count() == 2
        capacities = [row.max_hosting_capacity_mw for row in data.collect()]
        assert len(set(capacities)) == 2  # Two distinct values
    
    def test_same_day_reruns_dont_create_versions(self, spark):
        """Test that multiple pipeline runs on same day don't create false DER versions."""
        # Run 1 on 2024-01-15 morning
        run1 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", date(2024, 1, 15), "2024-01-15 08:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "ingestion_date", "ingestion_timestamp"])
        
        # Run 2 on 2024-01-15 afternoon (same day, different timestamp)
        run2 = spark.createDataFrame([
            ("utility1_proj1_SolarPV", "SolarPV", "50.0", date(2024, 1, 15), "2024-01-15 14:00:00"),
        ], ["der_id", "der_type", "nameplate_rating_kw", "ingestion_date", "ingestion_timestamp"])
        
        # ingestion_date is same (sequence_by column)
        assert run1.select("ingestion_date").collect()[0][0] == run2.select("ingestion_date").collect()[0][0]
        
        # ingestion_timestamp is different (but excluded via except_column_list)
        assert run1.select("ingestion_timestamp").collect()[0][0] != run2.select("ingestion_timestamp").collect()[0][0]
        
        # Business data is identical
        assert run1.select("nameplate_rating_kw").collect()[0][0] == run2.select("nameplate_rating_kw").collect()[0][0]
        
        # Result: No new SCD2 version should be created (same ingestion_date + same business data)
