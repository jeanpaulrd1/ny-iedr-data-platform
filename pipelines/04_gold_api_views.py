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

Technology Types: imported from schema_normalization.UTILITY1_DER_TECH_COLUMNS.
Do NOT hardcode a second copy of this list here. A prior version of this file
maintained its own 14-name list that drifted from schema_normalization.py —
different casing, different names, missing entries, phantom entries — causing
10 of 14 per-technology aggregation columns to silently return zero for every
feeder. Importing the constant makes that class of bug structurally impossible.
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    from pipelines.utils.schema_normalization import UTILITY1_DER_TECH_COLUMNS
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.schema_normalization import UTILITY1_DER_TECH_COLUMNS


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
    name="dev_iedr.gold.feeders_with_capacity",
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
    circuits = filter_current(dlt.read("dev_iedr.gold.circuits_current"))
    
    # Read current installed DER
    der_installed = filter_current(dlt.read("dev_iedr.gold.der_installed_current"))
    
    # Read current planned DER
    der_planned = filter_current(dlt.read("dev_iedr.gold.der_planned_current"))
    
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
        F.col("color_hex"),
        F.col("color_name"),
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
    name="dev_iedr.gold.feeder_der_summary",
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
    der_installed = filter_current(dlt.read("dev_iedr.gold.der_installed_current"))
    der_planned = filter_current(dlt.read("dev_iedr.gold.der_planned_current"))
    
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
    
    # Technology list — imported from schema_normalization.py, never hardcoded here.
    # This guarantees these names always match exactly what Silver produces,
    # since both this file and the unpivot read from the same constant.
    tech_types = UTILITY1_DER_TECH_COLUMNS
    
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


# ==============================================================================
# GOLD VIEW: FEEDER MAP LAYER (Map Renderer)
# ==============================================================================

@dlt.table(
    name="dev_iedr.gold.feeder_map_layer",
    comment="Denormalized feeder data for map rendering - single query, zero joins required",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold"
    },
    cluster_by=["utility_id", "grid_area_code"]
)
def feeder_map_layer():
    """Purpose-built table for map rendering - all data in one query.
    
    Combines circuits_current + feeder_der_summary with geospatial fields.
    Map renderer queries this table with:
      WHERE utility_id = ? AND grid_area_code IN (...)
    
    Returns one row per feeder with ALL data needed to render:
    - Visual properties (color, capacity for heatmap)
    - DER counts (icon badges from feeder_der_summary)
    - Grid hierarchy (district grouping fallback)
    - Geometry (WKT LineString when available)
    - Map render level (geometry/district/point_fallback)
    
    Liquid Clustering: (utility_id, grid_area_code)
    - Optimizes for multi-tenant filtering and district-level zoom
    - Handles skew automatically (utility2 has 1909 feeders, utility1 has 269)
    
    Current-Only: Filters to __END_AT IS NULL (no historical versions)
    - Map shows current capacity and DER state only
    - For historical maps, query circuits_current with __START_AT <= date AND __END_AT > date
    
    Returns:
        DataFrame with all fields needed for map rendering
    """
    # Read current circuits
    circuits = filter_current(dlt.read("dev_iedr.gold.circuits_current"))
    
    # Read DER summary (existing feeder_der_summary table with all technology breakdowns)
    der_summary = dlt.read("dev_iedr.gold.feeder_der_summary")
    
    # Join circuits + DER summary (LEFT JOIN - include feeders with zero DER)
    joined = circuits.join(
        der_summary,
        on=["feeder_id", "utility_id"],
        how="left"
    )
    
    # Select and compute map rendering fields
    return joined.select(
        # Identity
        circuits.feeder_id,
        circuits.utility_id,
        circuits.native_feeder_id,
        
        # Grid hierarchy (for district grouping fallback when geometry unavailable)
        # utility2: grid_state_code=36, grid_area_code=13/30/39/etc
        # utility1: NULLs (no underscore-delimited format)
        circuits.grid_state_code,
        circuits.grid_area_code,
        circuits.grid_circuit_id,
        
        # Visual properties
        circuits.color_code,  # Raw utility color code (for reference)
        circuits.color_hex,   # Standardized hex for map rendering (e.g. #13AFED)
        circuits.color_name,  # Normalized color name (e.g. SKYBLUE)
        circuits.max_hosting_capacity_mw,
        circuits.min_hosting_capacity_mw,
        
        # Available capacity calculation (same logic as feeders_with_capacity)
        (
            circuits.max_hosting_capacity_mw - 
            F.coalesce(der_summary.installed_capacity_kw / 1000.0, F.lit(0.0)) -
            F.coalesce(der_summary.planned_capacity_kw / 1000.0, F.lit(0.0))
        ).alias("available_capacity_mw"),
        
        # DER aggregates (icon badges) - using field names from existing feeder_der_summary
        F.coalesce(der_summary.installed_count, F.lit(0)).alias("installed_der_count"),
        F.coalesce(der_summary.planned_count, F.lit(0)).alias("planned_der_count"),
        F.coalesce(der_summary.installed_capacity_kw, F.lit(0.0)).alias("total_installed_capacity_kw"),
        F.coalesce(der_summary.planned_capacity_kw, F.lit(0.0)).alias("total_planned_capacity_kw"),
        
        # Geometry (WKT LineString or NULL)
        circuits.geom_wkt,
        
        # Map render level (frontend graceful degradation)
        F.when(
            circuits.geom_wkt.isNotNull(),
            F.lit("geometry")  # Has WKT LineString - render precise feeder line
        ).when(
            circuits.grid_area_code.isNotNull(),
            F.lit("district")  # Has grid hierarchy - cluster by district
        ).otherwise(
            F.lit("point_fallback")  # No geometry or hierarchy - render as point
        ).alias("map_render_level"),
        
        # Metadata (tooltips)
        circuits.hca_refresh_date.alias("capacity_as_of_date"),
        circuits.hca_refresh_date.alias("data_as_of_date")
    )
