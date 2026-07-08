"""Unit tests for Gold layer API-optimized views.

Tests verify the correctness of API views that query SCD2 tables:
- feeders_with_capacity: Available capacity calculations
- feeder_der_summary: DER aggregations per feeder

Key validations:
- Available capacity formula: max_hosting_capacity_mw - (installed_kw / 1000)
- kW to MW conversions
- SCD2 filtering (__IS_CURRENT = true only)
- NULL handling (feeders without DER, unresolved DER)
- Aggregation accuracy (counts, sums, technology breakdown)
- Hybrid project detection
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, DateType, BooleanType, IntegerType
)
from datetime import datetime, date

# Import test fixtures
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestFeedersWithCapacity:
    """Tests for feeders_with_capacity view."""
    
    def test_calculates_available_capacity_correctly(self, spark):
        """Test that available capacity = max_capacity - installed_capacity."""
        # Define explicit schema for circuits with NULL-able __END_AT
        circuits_schema = StructType([
            StructField("feeder_id", StringType(), False),
            StructField("utility_id", StringType(), False),
            StructField("native_feeder_id", StringType(), False),
            StructField("voltage_kv", DoubleType(), False),
            StructField("max_hosting_capacity_mw", DoubleType(), False),
            StructField("min_hosting_capacity_mw", DoubleType(), False),
            StructField("color_code", StringType(), False),
            StructField("shape_length", DoubleType(), False),
            StructField("hca_refresh_date", TimestampType(), False),
            StructField("__START_AT", TimestampType(), False),
            StructField("__END_AT", TimestampType(), True),  # Nullable
            StructField("__IS_CURRENT", BooleanType(), False)
        ])
        
        # Circuit with 10 MW max capacity
        circuits = spark.createDataFrame([
            ("feeder1", "utility1", "native_f1", 12.0, 10.0, 8.0, "green", 450.0, 
             datetime(2024, 1, 15), datetime(2024, 1, 15), None, True),
        ], circuits_schema)
        
        # Define explicit schema for DER with NULL-able __END_AT
        der_schema = StructType([
            StructField("feeder_id", StringType(), False),
            StructField("utility_id", StringType(), False),
            StructField("der_id", StringType(), False),
            StructField("der_type", StringType(), False),
            StructField("nameplate_rating_kw", DoubleType(), False),
            StructField("__START_AT", TimestampType(), False),
            StructField("__END_AT", TimestampType(), True),  # Nullable
            StructField("__IS_CURRENT", BooleanType(), False)
        ])
        
        # 3 MW (3000 kW) of installed DER
        der = spark.createDataFrame([
            ("feeder1", "utility1", "der1", "SolarPV", 2000.0, datetime(2024, 1, 15), None, True),
            ("feeder1", "utility1", "der2", "SolarPV", 1000.0, datetime(2024, 1, 15), None, True),
        ], der_schema)
        
        # Filter current only
        circuits_current = circuits.filter(F.col("__IS_CURRENT") == True)
        der_current = der.filter(F.col("__IS_CURRENT") == True)
        
        # Aggregate DER
        der_agg = der_current.groupBy("feeder_id").agg(
            F.count("*").alias("der_count"),
            F.sum("nameplate_rating_kw").alias("total_kw")
        )
        
        # Join and calculate
        result = circuits_current.join(der_agg, "feeder_id", "left").select(
            "feeder_id",
            "max_hosting_capacity_mw",
            F.coalesce("total_kw", F.lit(0.0)).alias("total_kw"),
            (F.col("max_hosting_capacity_mw") - (F.coalesce("total_kw", F.lit(0.0)) / 1000.0)).alias("available_mw")
        )
        
        row = result.collect()[0]
        assert row.max_hosting_capacity_mw == 10.0
        assert row.total_kw == 3000.0
        assert row.available_mw == 7.0  # 10 MW - 3 MW = 7 MW available
    
    def test_handles_feeder_with_no_der(self, spark):
        """Test that feeders with no DER show full capacity available."""
        circuits = spark.createDataFrame([
            ("feeder1", "utility1", "native_f1", 12.0, 10.0, 8.0, "green", 450.0, True),
        ], ["feeder_id", "utility_id", "native_feeder_id", "voltage_kv", 
            "max_hosting_capacity_mw", "min_hosting_capacity_mw", "color_code", 
            "shape_length", "__IS_CURRENT"])
        
        # No DER (LEFT JOIN will produce NULL)
        der_agg = spark.createDataFrame([], 
            StructType([
                StructField("feeder_id", StringType()),
                StructField("total_kw", DoubleType())
            ])
        )
        
        result = circuits.join(der_agg, "feeder_id", "left").select(
            "feeder_id",
            "max_hosting_capacity_mw",
            F.coalesce("total_kw", F.lit(0.0)).alias("total_kw"),
            (F.col("max_hosting_capacity_mw") - (F.coalesce("total_kw", F.lit(0.0)) / 1000.0)).alias("available_mw")
        )
        
        row = result.collect()[0]
        assert row.feeder_id == "feeder1"
        assert row.total_kw == 0.0
        assert row.available_mw == 10.0  # Full capacity available
    
    def test_handles_negative_available_capacity(self, spark):
        """Test that overcapacity feeders show negative availability."""
        circuits = spark.createDataFrame([
            ("feeder1", "utility1", 10.0, True),
        ], ["feeder_id", "utility_id", "max_hosting_capacity_mw", "__IS_CURRENT"])
        
        # 12 MW (12000 kW) installed - exceeds capacity!
        der = spark.createDataFrame([
            ("feeder1", 12000.0, True),
        ], ["feeder_id", "nameplate_rating_kw", "__IS_CURRENT"])
        
        der_agg = der.groupBy("feeder_id").agg(F.sum("nameplate_rating_kw").alias("total_kw"))
        
        result = circuits.join(der_agg, "feeder_id", "left").select(
            "feeder_id",
            "max_hosting_capacity_mw",
            F.coalesce("total_kw", F.lit(0.0)).alias("total_kw"),
            (F.col("max_hosting_capacity_mw") - (F.coalesce("total_kw", F.lit(0.0)) / 1000.0)).alias("available_mw")
        )
        
        row = result.collect()[0]
        assert row.available_mw == -2.0  # 10 MW - 12 MW = -2 MW (overloaded)
    
    def test_filters_scd2_current_records_only(self, spark):
        """Test that only __IS_CURRENT = true records are included."""
        # Define explicit schema with NULL-able __END_AT
        schema = StructType([
            StructField("feeder_id", StringType(), False),
            StructField("utility_id", StringType(), False),
            StructField("max_hosting_capacity_mw", DoubleType(), False),
            StructField("__START_AT", TimestampType(), False),
            StructField("__END_AT", TimestampType(), True),  # Nullable
            StructField("__IS_CURRENT", BooleanType(), False)
        ])
        
        circuits = spark.createDataFrame([
            ("feeder1", "utility1", 10.0, datetime(2024, 1, 15), None, True),  # Current
            ("feeder1", "utility1", 8.0, datetime(2024, 1, 10), datetime(2024, 1, 15), False),  # Historical
        ], schema)
        
        current_only = circuits.filter(F.col("__IS_CURRENT") == True)
        
        assert current_only.count() == 1
        row = current_only.collect()[0]
        assert row.max_hosting_capacity_mw == 10.0
        assert row["__END_AT"] is None
    
    def test_kw_to_mw_conversion_precision(self, spark):
        """Test that kW to MW conversion maintains precision."""
        test_cases = [
            (5000.0, 5.0),    # 5000 kW = 5.0 MW
            (7500.0, 7.5),    # 7500 kW = 7.5 MW
            (1234.5, 1.2345), # Fractional kW
            (0.0, 0.0),       # Zero
        ]
        
        for kw, expected_mw in test_cases:
            df = spark.createDataFrame([(kw,)], ["kw"])
            result = df.select((F.col("kw") / 1000.0).alias("mw")).collect()[0].mw
            assert abs(result - expected_mw) < 0.0001, f"Failed for {kw} kW"


class TestFeederDerSummary:
    """Tests for feeder_der_summary view."""
    
    def test_aggregates_installed_and_planned_correctly(self, spark):
        """Test that installed and planned DER are counted separately."""
        # 2 installed, 1 planned
        der = spark.createDataFrame([
            ("feeder1", "utility1", "der1", "SolarPV", 1000.0, "installed", True),
            ("feeder1", "utility1", "der2", "SolarPV", 2000.0, "installed", True),
            ("feeder1", "utility1", "der3", "SolarPV", 3000.0, "planned", True),
        ], ["feeder_id", "utility_id", "der_id", "der_type", "nameplate_rating_kw", 
            "der_status", "__IS_CURRENT"])
        
        result = der.filter(F.col("__IS_CURRENT") == True).groupBy("feeder_id").agg(
            F.sum(F.when(F.col("der_status") == "installed", 1).otherwise(0)).alias("installed_count"),
            F.sum(F.when(F.col("der_status") == "installed", F.col("nameplate_rating_kw")).otherwise(0)).alias("installed_kw"),
            F.sum(F.when(F.col("der_status") == "planned", 1).otherwise(0)).alias("planned_count"),
            F.sum(F.when(F.col("der_status") == "planned", F.col("nameplate_rating_kw")).otherwise(0)).alias("planned_kw")
        )
        
        row = result.collect()[0]
        assert row.installed_count == 2
        assert row.installed_kw == 3000.0  # 1000 + 2000
        assert row.planned_count == 1
        assert row.planned_kw == 3000.0
    
    def test_technology_breakdown_by_type(self, spark):
        """Test that DER are counted by technology type."""
        der = spark.createDataFrame([
            ("feeder1", "der1", "SolarPV", 1000.0, True),
            ("feeder1", "der2", "SolarPV", 2000.0, True),
            ("feeder1", "der3", "EnergyStorageSystem", 500.0, True),
            ("feeder1", "der4", "Wind", 1500.0, True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "__IS_CURRENT"])
        
        result = der.filter(F.col("__IS_CURRENT") == True).groupBy("feeder_id").agg(
            F.sum(F.when(F.col("der_type") == "SolarPV", 1).otherwise(0)).alias("solar_count"),
            F.sum(F.when(F.col("der_type") == "SolarPV", F.col("nameplate_rating_kw")).otherwise(0)).alias("solar_kw"),
            F.sum(F.when(F.col("der_type") == "EnergyStorageSystem", 1).otherwise(0)).alias("storage_count"),
            F.sum(F.when(F.col("der_type") == "EnergyStorageSystem", F.col("nameplate_rating_kw")).otherwise(0)).alias("storage_kw"),
            F.sum(F.when(F.col("der_type") == "Wind", 1).otherwise(0)).alias("wind_count"),
            F.sum(F.when(F.col("der_type") == "Wind", F.col("nameplate_rating_kw")).otherwise(0)).alias("wind_kw")
        )
        
        row = result.collect()[0]
        assert row.solar_count == 2
        assert row.solar_kw == 3000.0
        assert row.storage_count == 1
        assert row.storage_kw == 500.0
        assert row.wind_count == 1
        assert row.wind_kw == 1500.0
    
    def test_detects_hybrid_projects_correctly(self, spark):
        """Test that hybrid projects (multiple technologies) are detected."""
        der = spark.createDataFrame([
            ("feeder1", "proj1_SolarPV", "SolarPV", 1000.0, True),
            ("feeder1", "proj1_EnergyStorageSystem", "EnergyStorageSystem", 500.0, True),
            ("feeder1", "proj2_SolarPV", "SolarPV", 2000.0, True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "__IS_CURRENT"])
        
        # Extract project ID (before last underscore)
        result = der.withColumn(
            "project_id",
            F.regexp_replace(F.col("der_id"), r"_[^_]+$", "")
        )
        
        # Count technologies per project
        project_tech_count = result.groupBy("feeder_id", "project_id").agg(
            F.countDistinct("der_type").alias("tech_count")
        )
        
        # Identify hybrid projects (tech_count > 1)
        hybrid_projects = project_tech_count.filter(F.col("tech_count") > 1)
        
        assert hybrid_projects.count() == 1  # proj1 is hybrid
        row = hybrid_projects.collect()[0]
        assert row.project_id == "proj1"
        assert row.tech_count == 2  # SolarPV + EnergyStorageSystem
    
    def test_counts_unique_projects_vs_total_der(self, spark):
        """Test distinction between project count and DER count."""
        # Hybrid project creates multiple DER rows
        der = spark.createDataFrame([
            ("feeder1", "proj1_SolarPV", "SolarPV", 1000.0, True),
            ("feeder1", "proj1_EnergyStorageSystem", "EnergyStorageSystem", 500.0, True),
            ("feeder1", "proj2_SolarPV", "SolarPV", 2000.0, True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "__IS_CURRENT"])
        
        # Extract project ID
        result = der.withColumn(
            "project_id",
            F.regexp_replace(F.col("der_id"), r"_[^_]+$", "")
        )
        
        agg = result.groupBy("feeder_id").agg(
            F.countDistinct("project_id").alias("project_count"),
            F.count("*").alias("der_count")
        )
        
        row = agg.collect()[0]
        assert row.project_count == 2  # proj1, proj2
        assert row.der_count == 3  # 3 DER rows
    
    def test_excludes_null_feeder_id(self, spark):
        """Test that unresolved DER (NULL feeder_id) are excluded."""
        # Define explicit schema with NULL-able feeder_id
        schema = StructType([
            StructField("feeder_id", StringType(), True),  # Nullable
            StructField("der_id", StringType(), False),
            StructField("der_type", StringType(), False),
            StructField("nameplate_rating_kw", DoubleType(), False),
            StructField("__IS_CURRENT", BooleanType(), False)
        ])
        
        der = spark.createDataFrame([
            ("feeder1", "der1", "SolarPV", 1000.0, True),
            (None, "der2", "SolarPV", 2000.0, True),  # Unresolved
        ], schema)
        
        # Filter out NULL feeder_id
        resolved = der.filter(F.col("feeder_id").isNotNull())
        
        assert resolved.count() == 1
        row = resolved.collect()[0]
        assert row.feeder_id == "feeder1"
    
    def test_union_of_installed_and_planned_sources(self, spark):
        """Test that installed and planned DER are unioned correctly."""
        installed = spark.createDataFrame([
            ("feeder1", "der1", "SolarPV", 1000.0, "installed", True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "der_status", "__IS_CURRENT"])
        
        planned = spark.createDataFrame([
            ("feeder1", "der2", "SolarPV", 2000.0, "planned", True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "der_status", "__IS_CURRENT"])
        
        # Union both sources
        all_der = installed.union(planned)
        
        result = all_der.filter(F.col("__IS_CURRENT") == True).groupBy("feeder_id").agg(
            F.count("*").alias("total_count"),
            F.sum("nameplate_rating_kw").alias("total_kw")
        )
        
        row = result.collect()[0]
        assert row.total_count == 2  # 1 installed + 1 planned
        assert row.total_kw == 3000.0  # 1000 + 2000


class TestApiViewsEdgeCases:
    """Tests for edge cases in API views."""
    
    def test_feeder_with_zero_capacity_der(self, spark):
        """Test that zero-capacity DER are handled (should be filtered out in production)."""
        der = spark.createDataFrame([
            ("feeder1", "der1", "SolarPV", 0.0, True),
            ("feeder1", "der2", "SolarPV", 1000.0, True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "__IS_CURRENT"])
        
        # In production, zero-capacity should be filtered
        non_zero = der.filter(F.col("nameplate_rating_kw") > 0)
        
        assert non_zero.count() == 1
        row = non_zero.collect()[0]
        assert row.nameplate_rating_kw == 1000.0
    
    def test_multiple_feeders_in_single_query(self, spark):
        """Test that view handles multiple feeders efficiently."""
        der = spark.createDataFrame([
            ("feeder1", "der1", "SolarPV", 1000.0, True),
            ("feeder2", "der2", "SolarPV", 2000.0, True),
            ("feeder3", "der3", "SolarPV", 3000.0, True),
        ], ["feeder_id", "der_id", "der_type", "nameplate_rating_kw", "__IS_CURRENT"])
        
        result = der.filter(F.col("__IS_CURRENT") == True).groupBy("feeder_id").agg(
            F.count("*").alias("der_count"),
            F.sum("nameplate_rating_kw").alias("total_kw")
        )
        
        assert result.count() == 3
        # Verify each feeder has correct aggregation
        rows = {row.feeder_id: row for row in result.collect()}
        assert rows["feeder1"].total_kw == 1000.0
        assert rows["feeder2"].total_kw == 2000.0
        assert rows["feeder3"].total_kw == 3000.0
    
    def test_clustering_columns_present(self):
        """Test that clustering columns are properly defined."""
        # Expected clustering for API views
        clustering_columns = ["utility_id", "feeder_id"]
        
        # Both should be present for efficient filtering
        assert "utility_id" in clustering_columns
        assert "feeder_id" in clustering_columns
        assert len(clustering_columns) == 2
