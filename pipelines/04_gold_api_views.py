"""Gold Layer DLT Pipeline - API-Optimized Views.

Creates materialized views optimized for API queries and dashboards.
Reads from Gold SCD Type 2 tables and exposes current state only.

Key Features:
- Filters __IS_CURRENT = true (excludes historical versions)
- Calculates available feeder capacity (installed + planned DER)
- Aggregates DER installations per feeder (all 14 technology types)
- Liquid clustering optimized for common API query patterns
- No partitioning (query-optimized views, not time-series)
- SCD2 metadata (__START_AT, __END_AT) excluded from API responses

Views:
1. feeders_with_capacity: Current feeder capacity and availability (installed + planned)
2. feeder_der_summary: DER count and capacity aggregated by feeder (all 14 types)

Technology Types (14 total - MUST MATCH unpivot_utility1_der in schema_normalization.py):
- SolarPV, EnergyStorageSystem, Wind, MicroTurbine, SynchronousGenerator
- InductionGenerator, FarmWaste, FuelCell, CombinedHeatandPower, GasTurbine
- Hydro, InternalCombustionEngine, SteamTurbine, Other
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==============================================================================
# HELPER: IS_CURRENT FILTER
# ==============================================================================

def filter_current(df: DataFrame) -> DataFrame:
    """Filter SCD2 table to current rows only, resilient to column name case.

    DLT's APPLY CHANGES INTO generates SCD2 metadata columns. The column name
    case differs by runtime:
      - Classic DLT:    __IS_CURRENT, __START_AT, __END_AT  (uppercase)
      - Serverless DLT: __is_current, __start_at, __end_at  (lowercase)

    Hardcoding either case throws AnalysisException on the other runtime.
    This helper resolves the actual column name at runtime before filtering.
    """
    end_at_col = next(
        (c for c in df.columns if c.lower() == "__end_at"),
        None
    )
    if end_at_col is None:
        raise ValueError(
            f"SCD2 metadata column '__END_AT' not found. "
            f"Available columns: {df.columns}"
        )
    return df.filter(F.col(end_at_col).isNull())


# ==============================================================================
# GOLD VIEW: FEEDERS WITH CAPACITY
# ==============================================================================

@dlt.table(
    name="feeders_with_capacity",
    comment="API-optimized view of current feeder capacity with calculated availability (installed + planned DER)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold"
    },
    cluster_by=["utility_id", "feeder_id"]
)
def feeders_with_capacity():
    """Current feeder capacity with calculated availability.
    
    available_capacity_mw business assumption:
        = max_hosting_capacity_mw - (installed_capacity_kw + planned_capacity_kw) / 1000
    Planned DER is subtracted because the application uses a pessimistic view:
    capacity reserved for projects in the queue is not considered available.
    If the application requires an optimistic view (installed only), remove
    planned_capacity_kw from this calculation.
    This assumption should be confirmed with the product team before production.
    
    Clustering:
    - utility_id: Primary dimension for multi-tenant queries
    - feeder_id: Secondary dimension for direct feeder lookups
    
    Use Cases:
    - Find feeders with available capacity > X MW
    - Rank feeders by available capacity within a utility
    - Compare installed vs planned capacity per feeder
    - API endpoint: GET /feeders?min_capacity=5.0&utility=utility1
    """
    # Read current circuits (SCD2, __IS_CURRENT = true only)
    circuits = filter_current(dlt.read("circuits_current"))
    
    # Read current installed DER
    der_installed = filter_current(dlt.read("der_installed_current"))
    
    # Read current planned DER
    der_planned = filter_current(dlt.read("der_planned_current"))
    
    # Group by (feeder_id, utility_id) — not feeder_id alone.
    # feeder_id is utility-prefixed so cross-utility collision is practically
    # impossible, but grouping on both columns is explicit and robust against
    # any future prefix bug or utility_id mismatch.
    installed_by_feeder = der_installed.groupBy("feeder_id", "utility_id").agg(
        F.count("*").alias("installed_der_count"),
        F.sum("nameplate_rating_kw").alias("installed_capacity_kw")
    )
    
    planned_by_feeder = der_planned.groupBy("feeder_id", "utility_id").agg(
        F.count("*").alias("planned_der_count"),
        F.sum("nameplate_rating_kw").alias("planned_capacity_kw")
    )
    
    # Join circuits with installed and planned DER
    # Joining on both feeder_id AND utility_id ensures multi-tenant safety
    result = circuits.join(
        installed_by_feeder,
        on=["feeder_id", "utility_id"],
        how="left"
    ).join(
        planned_by_feeder,
        on=["feeder_id", "utility_id"],
        how="left"
    ).select(
        F.col("feeder_id"),
        F.col("utility_id"),
        F.col("native_feeder_id"),
        F.col("voltage_kv"),
        F.col("max_hosting_capacity_mw"),
        F.col("min_hosting_capacity_mw"),
        
        # Installed DER metrics
        F.coalesce(F.col("installed_der_count"), F.lit(0)).alias("installed_der_count"),
        F.coalesce(F.col("installed_capacity_kw"), F.lit(0.0)).alias("installed_capacity_kw"),
        
        # Planned DER metrics
        F.coalesce(F.col("planned_der_count"), F.lit(0)).alias("planned_der_count"),
        F.coalesce(F.col("planned_capacity_kw"), F.lit(0.0)).alias("planned_capacity_kw"),
        
        # Total DER metrics
        (
            F.coalesce(F.col("installed_der_count"), F.lit(0)) +
            F.coalesce(F.col("planned_der_count"), F.lit(0))
        ).alias("total_der_count"),
        (
            F.coalesce(F.col("installed_capacity_kw"), F.lit(0.0)) +
            F.coalesce(F.col("planned_capacity_kw"), F.lit(0.0))
        ).alias("total_der_capacity_kw"),
        
        # Pessimistic available capacity: subtracts installed + planned
        (
            F.col("max_hosting_capacity_mw") - 
            (
                (F.coalesce(F.col("installed_capacity_kw"), F.lit(0.0)) +
                 F.coalesce(F.col("planned_capacity_kw"), F.lit(0.0))) / 1000.0
            )
        ).alias("available_capacity_mw"),
        
        F.col("color_code"),
        F.col("shape_length"),
        F.col("hca_refresh_date").alias("capacity_as_of_date")
        # SCD2 metadata (__START_AT, __END_AT) intentionally excluded.
        # This is a current-state API view. Historical queries belong on
        # circuits_current directly, not this view.
    )
    
    return result



# ==============================================================================
# GOLD VIEW: FEEDER DER SUMMARY
# ==============================================================================

@dlt.table(
    name="feeder_der_summary",
    comment="API-optimized view of DER aggregations per feeder - counts, capacity, status breakdown (all 14 technology types)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold"
    },
    cluster_by=["feeder_id", "utility_id"]
)
def feeder_der_summary():
    """Aggregated DER metrics per feeder (installed + planned).
    
    Aggregates:
    - Total DER counts (installed, planned, by technology)
    - Total nameplate capacity (installed, planned, by technology)
    - All 14 technology types supported
    - Hybrid project identification (feeders with multiple DER types)
    
    Clustering:
    - feeder_id: Primary lookup dimension
    - utility_id: Multi-tenant filtering
    
    Use Cases:
    - Get all DER on a specific feeder
    - Compare installed vs planned DER capacity
    - Identify feeders with hybrid projects (solar + storage)
    - Technology distribution analysis across all 14 types
    - API endpoint: GET /feeders/{feeder_id}/der-summary
    """
    # Read current DER (installed + planned)
    der_installed = filter_current(dlt.read("der_installed_current"))
    der_planned = filter_current(dlt.read("der_planned_current"))
    
    # Use a new column name (installation_status) so we don't overwrite
    # der_status from source — overwriting would mask any DQ issue where
    # der_status is unexpectedly NULL.
    der_installed_tagged = der_installed.select(
        "feeder_id", "utility_id", "der_id", "der_type", "nameplate_rating_kw"
    ).withColumn("installation_status", F.lit("installed"))
    
    der_planned_tagged = der_planned.select(
        "feeder_id", "utility_id", "der_id", "der_type", "nameplate_rating_kw"
    ).withColumn("installation_status", F.lit("planned"))
    
    # Union all DER (installed + planned)
    all_der = der_installed_tagged.unionByName(der_planned_tagged)
    
    # Technology list from unpivot_utility1_der in schema_normalization.py
    # IMPORTANT: These names must match exactly what Silver produces.
    # Any mismatch causes that technology's aggregations to silently return 0.
    tech_types = [
        "SolarPV", "EnergyStorageSystem", "Wind", "CombinedHeatAndPower",
        "Biomass", "Biogas", "Geothermal", "HydroElectric", "InternalCombustion",
        "Microturbine", "NaturalGas", "SteamTurbine", "Waste", "Other"
    ]
    
    # Base aggregations
    agg_expressions = [
        F.count("*").alias("total_der_count"),
        F.countDistinct("der_id").alias("unique_project_count"),
        
        F.sum(F.when(F.col("installation_status") == "installed", 1).otherwise(0)).alias("installed_count"),
        F.sum(F.when(F.col("installation_status") == "installed", F.col("nameplate_rating_kw")).otherwise(0)).alias("installed_capacity_kw"),
        
        F.sum(F.when(F.col("installation_status") == "planned", 1).otherwise(0)).alias("planned_count"),
        F.sum(F.when(F.col("installation_status") == "planned", F.col("nameplate_rating_kw")).otherwise(0)).alias("planned_capacity_kw"),
    ]
    
    # Per-technology aggregations
    for tech in tech_types:
        agg_expressions.extend([
            F.sum(F.when(F.col("der_type") == tech, 1).otherwise(0)).alias(f"{tech.lower()}_count"),
            F.sum(F.when(F.col("der_type") == tech, F.col("nameplate_rating_kw")).otherwise(0)).alias(f"{tech.lower()}_capacity_kw")
        ])
    
    agg_expressions.append(F.countDistinct("der_type").alias("technology_type_count"))
    
    result = all_der.filter(F.col("feeder_id").isNotNull()).groupBy(
        "feeder_id", "utility_id"
    ).agg(*agg_expressions).withColumn(
        "has_hybrid_projects",
        F.when(F.col("technology_type_count") > 1, True).otherwise(False)
    )
    
    return result
