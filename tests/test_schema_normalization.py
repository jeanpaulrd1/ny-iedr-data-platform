"""Unit tests for schema normalization utilities (pipelines/utils/schema_normalization.py).

Tests all utility-specific transformations:
- Utility 1 segment aggregation (circuits)
- Utility 1 DER unpivot (wide → narrow)
- Canonical field mapping (both utilities)
- Edge cases: NULL handling, empty DataFrames, hybrid DER projects
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime

# Import schema normalization functions
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.utils.schema_normalization import (
    aggregate_utility1_segments,
    unpivot_utility1_der,
    map_circuits_to_canonical,
    map_der_to_canonical,
    UTILITY1_ID,
    UTILITY2_ID
)


class TestAggregateUtility1Segments:
    """Tests for aggregate_utility1_segments function."""
    
    def test_aggregates_segments_to_feeder(self, spark):
        """Test that multiple segment rows aggregate to single feeder row."""
        df = spark.createDataFrame([
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-15", "green", 100.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-15", "green", 150.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-15", "green", 200.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
        ], ["Circuits_Phase3_CIRCUIT", "utility_id", "NYHCPV_csv_FMAXHC", "NYHCPV_csv_FMINHC", 
            "NYHCPV_csv_FVOLTAGE", "NYHCPV_csv_FHCADATE", "NYHCPV_csv_NMAPCOLOR", "Shape_Length",
            "NYHCPV_csv_FFEEDER", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = aggregate_utility1_segments(df)
        
        # Should aggregate to 1 row
        assert result.count() == 1
        row = result.collect()[0]
        
        # Check feeder_id prefix
        assert row.feeder_id == "utility1_circuit1"
        
        # Shape_Length should be SUM (100 + 150 + 200 = 450)
        assert row.shape_length_raw == 450.0
    
    def test_max_hosting_capacity_not_sum(self, spark):
        """Test that hosting capacity uses MAX not SUM (capacity repeats across segments)."""
        df = spark.createDataFrame([
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-15", "green", 100.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-15", "green", 150.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
        ], ["Circuits_Phase3_CIRCUIT", "utility_id", "NYHCPV_csv_FMAXHC", "NYHCPV_csv_FMINHC", 
            "NYHCPV_csv_FVOLTAGE", "NYHCPV_csv_FHCADATE", "NYHCPV_csv_NMAPCOLOR", "Shape_Length",
            "NYHCPV_csv_FFEEDER", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = aggregate_utility1_segments(df)
        row = result.collect()[0]
        
        # Max capacity should be MAX (10.5), not SUM (21.0)
        assert row.max_hosting_capacity_raw == "10.5"
        assert row.min_hosting_capacity_raw == "8.0"
    
    def test_most_recent_hca_date(self, spark):
        """Test that most recent HCA refresh date is selected."""
        df = spark.createDataFrame([
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-10", "green", 100.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-20", "green", 150.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
            ("circuit1", "utility1", "10.5", "8.0", "12.0", "2024-01-15", "green", 200.0, "feeder1", "2024-01-15", "2024-01-15", "run1"),
        ], ["Circuits_Phase3_CIRCUIT", "utility_id", "NYHCPV_csv_FMAXHC", "NYHCPV_csv_FMINHC", 
            "NYHCPV_csv_FVOLTAGE", "NYHCPV_csv_FHCADATE", "NYHCPV_csv_NMAPCOLOR", "Shape_Length",
            "NYHCPV_csv_FFEEDER", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = aggregate_utility1_segments(df)
        row = result.collect()[0]
        
        # Should pick most recent date (2024-01-20)
        assert row.hca_refresh_date_raw == "2024-01-20"
    
    def test_handles_null_values(self, spark):
        """Test that NULL values in optional fields are handled gracefully."""
        df = spark.createDataFrame([
            ("circuit1", "utility1", "10.5", None, "12.0", "2024-01-15", None, None, "feeder1", "2024-01-15", "2024-01-15", "run1"),
        ], ["Circuits_Phase3_CIRCUIT", "utility_id", "NYHCPV_csv_FMAXHC", "NYHCPV_csv_FMINHC", 
            "NYHCPV_csv_FVOLTAGE", "NYHCPV_csv_FHCADATE", "NYHCPV_csv_NMAPCOLOR", "Shape_Length",
            "NYHCPV_csv_FFEEDER", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = aggregate_utility1_segments(df)
        
        # Should not fail on NULL values
        assert result.count() == 1
        row = result.collect()[0]
        assert row.min_hosting_capacity_raw is None
        assert row.color_code_raw is None


class TestUnpivotUtility1Der:
    """Tests for unpivot_utility1_der function."""
    
    def test_unpivots_single_technology(self, spark):
        """Test unpivoting single technology (solar only)."""
        df = spark.createDataFrame([
            ("proj1", "circuit1", "utility1", "100.0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "2024-01-15", "2024-01-15", "run1"),
        ], ["ProjectID", "ProjectCircuitID", "utility_id", "SolarPV", "EnergyStorageSystem", "Wind", 
            "MicroTurbine", "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
            "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine", "SteamTurbine", 
            "Other", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = unpivot_utility1_der(df, include_installation_date=False)
        
        # Should produce 1 row (only SolarPV has non-zero capacity)
        assert result.count() == 1
        row = result.collect()[0]
        
        assert row.der_type == "SolarPV"
        assert row.nameplate_rating_kw == 100.0
        assert row.der_id == "utility1_proj1_SolarPV"
        assert row.feeder_id == "utility1_circuit1"
    
    def test_unpivots_hybrid_project(self, spark):
        """Test unpivoting hybrid project (multiple technologies)."""
        df = spark.createDataFrame([
            ("proj1", "circuit1", "utility1", "50.0", "25.0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "2024-01-15", "2024-01-15", "run1"),
        ], ["ProjectID", "ProjectCircuitID", "utility_id", "SolarPV", "EnergyStorageSystem", "Wind", 
            "MicroTurbine", "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
            "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine", "SteamTurbine", 
            "Other", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = unpivot_utility1_der(df, include_installation_date=False)
        
        # Should produce 2 rows (SolarPV + EnergyStorageSystem)
        assert result.count() == 2
        rows = result.collect()
        
        # Check composite der_id includes technology type
        der_ids = sorted([r.der_id for r in rows])
        assert der_ids == ["utility1_proj1_EnergyStorageSystem", "utility1_proj1_SolarPV"]
        
        # Check capacities
        solar_row = [r for r in rows if r.der_type == "SolarPV"][0]
        storage_row = [r for r in rows if r.der_type == "EnergyStorageSystem"][0]
        assert solar_row.nameplate_rating_kw == 50.0
        assert storage_row.nameplate_rating_kw == 25.0
    
    def test_filters_zero_capacity(self, spark):
        """Test that zero capacity technologies are filtered out."""
        df = spark.createDataFrame([
            ("proj1", "circuit1", "utility1", "100.0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "2024-01-15", "2024-01-15", "run1"),
        ], ["ProjectID", "ProjectCircuitID", "utility_id", "SolarPV", "EnergyStorageSystem", "Wind", 
            "MicroTurbine", "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
            "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine", "SteamTurbine", 
            "Other", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = unpivot_utility1_der(df, include_installation_date=False)
        
        # Only 1 row (SolarPV), 13 zero-capacity techs filtered out
        assert result.count() == 1
    
    def test_handles_null_feeder_id(self, spark):
        """Test that NULL ProjectCircuitID produces NULL feeder_id (unresolved DER)."""
        df = spark.createDataFrame([
            ("proj1", None, "utility1", "100.0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "2024-01-15", "2024-01-15", "run1"),
        ], ["ProjectID", "ProjectCircuitID", "utility_id", "SolarPV", "EnergyStorageSystem", "Wind", 
            "MicroTurbine", "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
            "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine", "SteamTurbine", 
            "Other", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = unpivot_utility1_der(df, include_installation_date=False)
        row = result.collect()[0]
        
        # Unresolved DER: feeder_id should be NULL
        assert row.feeder_id is None
        assert row.native_feeder_id_raw is None
        # But der_id should still exist
        assert row.der_id == "utility1_proj1_SolarPV"
    
    def test_includes_installation_date_when_requested(self, spark):
        """Test that planned_installation_date_raw column is included for planned DER."""
        # Create schema with InServiceDate column
        df = spark.createDataFrame([
            ("proj1", "circuit1", "utility1", "100.0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "2025-06-15", "2024-01-15", "2024-01-15", "run1"),
        ], ["ProjectID", "ProjectCircuitID", "utility_id", "SolarPV", "EnergyStorageSystem", "Wind", 
            "MicroTurbine", "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
            "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine", "SteamTurbine", 
            "Other", "InServiceDate", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = unpivot_utility1_der(df, include_installation_date=True)
        
        # Should have planned_installation_date_raw column
        assert "planned_installation_date_raw" in result.columns
        row = result.collect()[0]
        assert row.planned_installation_date_raw == "2025-06-15"
    
    def test_installation_date_null_when_not_included(self, spark):
        """Test that installed DER gets NULL planned_installation_date_raw."""
        df = spark.createDataFrame([
            ("proj1", "circuit1", "utility1", "100.0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "2024-01-15", "2024-01-15", "run1"),
        ], ["ProjectID", "ProjectCircuitID", "utility_id", "SolarPV", "EnergyStorageSystem", "Wind", 
            "MicroTurbine", "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
            "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine", "SteamTurbine", 
            "Other", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = unpivot_utility1_der(df, include_installation_date=False)
        
        # Should still have the column but all NULL
        assert "planned_installation_date_raw" in result.columns
        row = result.collect()[0]
        assert row.planned_installation_date_raw is None


class TestMapCircuitsToCanonical:
    """Tests for map_circuits_to_canonical function."""
    
    def test_maps_utility1_fields(self, spark):
        """Test mapping utility1 intermediate schema to canonical."""
        df = spark.createDataFrame([
            ("utility1", "utility1_circuit1", "feeder1", "12.0", "10.5", "8.0", "2024-01-15 10:30:00", "green", "450.0", "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "feeder_id", "native_feeder_id", "voltage_kv_raw", "max_hosting_capacity_raw", 
            "min_hosting_capacity_raw", "hca_refresh_date_raw", "color_code_raw", "shape_length_raw",
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_circuits_to_canonical(df)
        row = result.collect()[0]
        
        # Check types are cast
        assert isinstance(row.voltage_kv, float)
        assert isinstance(row.max_hosting_capacity_mw, float)
        assert isinstance(row.shape_length, float)
        assert row.voltage_kv == 12.0
        assert row.max_hosting_capacity_mw == 10.5
    
    def test_maps_utility2_fields_with_timezone(self, spark):
        """Test mapping utility2 with timezone offset in hca_refresh_date."""
        df = spark.createDataFrame([
            ("utility2", "utility2_feeder1", "feeder1", "13.2", "15.0", "12.0", "2024/01/15 10:30:00+00:00", "red", "600.0", "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "feeder_id", "native_feeder_id", "voltage_kv_raw", "max_hosting_capacity_raw", 
            "min_hosting_capacity_raw", "hca_refresh_date_raw", "color_code_raw", "shape_length_raw",
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_circuits_to_canonical(df)
        row = result.collect()[0]
        
        # Check timestamp parsing with timezone
        assert row.hca_refresh_date is not None
        assert row.max_hosting_capacity_mw == 15.0
    
    def test_preserves_lineage_columns(self, spark):
        """Test that lineage columns are preserved."""
        df = spark.createDataFrame([
            ("utility1", "utility1_circuit1", "feeder1", "12.0", "10.5", "8.0", "2024-01-15 10:30:00", "green", "450.0", "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "feeder_id", "native_feeder_id", "voltage_kv_raw", "max_hosting_capacity_raw", 
            "min_hosting_capacity_raw", "hca_refresh_date_raw", "color_code_raw", "shape_length_raw",
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_circuits_to_canonical(df)
        
        # Lineage columns should exist
        assert "ingestion_timestamp" in result.columns
        assert "ingestion_date" in result.columns
        assert "pipeline_update_id" in result.columns


class TestMapDerToCanonical:
    """Tests for map_der_to_canonical function."""
    
    def test_maps_installed_der(self, spark):
        """Test mapping installed DER to canonical schema."""
        df = spark.createDataFrame([
            ("utility1", "utility1_proj1_SolarPV", "utility1_circuit1", "circuit1", "SolarPV", "100.0", None, None, "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "der_id", "feeder_id", "native_feeder_id_raw", "der_type", "nameplate_rating_kw",
            "planned_installation_date_raw", "interconnection_queue_id", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_der_to_canonical(df, der_table_type="installed")
        row = result.collect()[0]
        
        assert row.der_status == "installed"
        assert row.nameplate_rating_kw == 100.0
        # Installed DER should not have planned_installation_date column
        assert "planned_installation_date" not in result.columns
    
    def test_maps_planned_der_with_date(self, spark):
        """Test mapping planned DER with installation date."""
        df = spark.createDataFrame([
            ("utility2", "utility2_queue123", "utility2_feeder1", "feeder1", "Solar", "150.0", "6/15/2025", "queue123", "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "der_id", "feeder_id", "native_feeder_id_raw", "der_type", "nameplate_rating_kw",
            "planned_installation_date_raw", "interconnection_queue_id", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_der_to_canonical(df, der_table_type="planned")
        row = result.collect()[0]
        
        assert row.der_status == "planned"
        assert "planned_installation_date" in result.columns
        assert row.planned_installation_date is not None
        assert row.interconnection_queue_id == "queue123"
    
    def test_handles_unresolved_feeder(self, spark):
        """Test that unresolved DER (NULL feeder_id) passes through."""
        df = spark.createDataFrame([
            ("utility1", "utility1_proj1_SolarPV", None, None, "SolarPV", "100.0", None, None, "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "der_id", "feeder_id", "native_feeder_id_raw", "der_type", "nameplate_rating_kw",
            "planned_installation_date_raw", "interconnection_queue_id", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_der_to_canonical(df, der_table_type="installed")
        row = result.collect()[0]
        
        # Unresolved DER should pass through with NULL feeder_id
        assert row.feeder_id is None
        assert row.der_id == "utility1_proj1_SolarPV"
    
    def test_preserves_composite_der_id(self, spark):
        """Test that composite der_id (ProjectID_TechType) is preserved."""
        df = spark.createDataFrame([
            ("utility1", "utility1_proj1_SolarPV", "utility1_circuit1", "circuit1", "SolarPV", "50.0", None, None, "2024-01-15", "2024-01-15", "run1"),
            ("utility1", "utility1_proj1_EnergyStorageSystem", "utility1_circuit1", "circuit1", "EnergyStorageSystem", "25.0", None, None, "2024-01-15", "2024-01-15", "run1"),
        ], ["utility_id", "der_id", "feeder_id", "native_feeder_id_raw", "der_type", "nameplate_rating_kw",
            "planned_installation_date_raw", "interconnection_queue_id", "ingestion_timestamp", "ingestion_date", "pipeline_update_id"])
        
        result = map_der_to_canonical(df, der_table_type="installed")
        
        # Both rows should have unique der_id
        assert result.count() == 2
        der_ids = [r.der_id for r in result.collect()]
        assert len(set(der_ids)) == 2  # Both unique
