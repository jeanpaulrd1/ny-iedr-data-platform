"""Gold Layer Aggregation Views - Map-Optimized Tables.

Provides purpose-built tables for specific application queries:
- feeder_map_layer: Optimized for map rendering (single query, no joins, all render data)
- feeder_der_summary: Pre-aggregated DER counts and capacity per feeder
- feeders_with_capacity: Feeder capacity with available hosting capacity calculations

These tables read from Gold SCD2 tables (circuits_current, der_*_current) and materialize
the current state with pre-computed aggregations for fast API response times.

Design Principles:
- Denormalized: All data needed for a specific UI/API call in one table
- Current-only: Reads from SCD2 tables where __END_AT IS NULL
- Liquid Clustering: Optimized for common query patterns (utility_id, grid_area_code)
- Full-Refresh: Rebuilt on each pipeline run (source is SCD2, these are snapshots)
"""

import dlt
from pyspark.sql import functions as F


# ==============================================================================
# GOLD VIEW: FEEDER DER SUMMARY (Pre-Aggregation)
# ==============================================================================

@dlt.table(
    name="dev_iedr.gold.feeder_der_summary",
    comment="Pre-aggregated DER counts and capacity per feeder - current state only"
)
def feeder_der_summary():
    """Aggregate installed + planned DER counts and capacity per feeder.
    
    Reads from SCD2 tables, filters to current records only (__END_AT IS NULL),
    and pre-computes DER statistics for each feeder.
    
    Used by:
    - feeder_map_layer (icon badges)
    - API endpoint: /api/feeders/{feeder_id}/der_summary
    
    Returns:
        DataFrame with columns: feeder_id, utility_id, installed_der_count, 
        planned_der_count, total_installed_capacity_kw, total_planned_capacity_kw
    """
    # Read current installed DER
    installed = dlt.read("dev_iedr.gold.der_installed_current").filter(F.col("__END_AT").isNull())
    
    # Read current planned DER  
    planned = dlt.read("dev_iedr.gold.der_planned_current").filter(F.col("__END_AT").isNull())
    
    # Aggregate installed by feeder
    installed_agg = installed.groupBy("feeder_id", "utility_id").agg(
        F.count("*").alias("installed_der_count"),
        F.sum("nameplate_rating_kw").alias("total_installed_capacity_kw")
    )
    
    # Aggregate planned by feeder
    planned_agg = planned.groupBy("feeder_id", "utility_id").agg(
        F.count("*").alias("planned_der_count"),
        F.sum("nameplate_rating_kw").alias("total_planned_capacity_kw")
    )
    
    # Full outer join to include feeders with only installed OR only planned DER
    return installed_agg.join(
        planned_agg,
        on=["feeder_id", "utility_id"],
        how="full_outer"
    ).select(
        F.coalesce(installed_agg.feeder_id, planned_agg.feeder_id).alias("feeder_id"),
        F.coalesce(installed_agg.utility_id, planned_agg.utility_id).alias("utility_id"),
        F.coalesce(F.col("installed_der_count"), F.lit(0)).alias("installed_der_count"),
        F.coalesce(F.col("planned_der_count"), F.lit(0)).alias("planned_der_count"),
        F.coalesce(F.col("total_installed_capacity_kw"), F.lit(0.0)).alias("total_installed_capacity_kw"),
        F.coalesce(F.col("total_planned_capacity_kw"), F.lit(0.0)).alias("total_planned_capacity_kw")
    )


# ==============================================================================
# GOLD VIEW: FEEDERS WITH CAPACITY (Application-Ready)
# ==============================================================================

@dlt.table(
    name="dev_iedr.gold.feeders_with_capacity",
    comment="Feeder capacity with available hosting capacity calculations - current state only"
)
def feeders_with_capacity():
    """Calculate available hosting capacity per feeder.
    
    Reads circuits_current (where __END_AT IS NULL) and feeder_der_summary,
    computes available_capacity_mw = max_hosting_capacity - total_installed_capacity.
    
    Used by:
    - API endpoint: /api/feeders?min_capacity={threshold}
    - feeder_map_layer (capacity heatmap coloring)
    
    Returns:
        DataFrame with feeder_id, utility_id, max_hosting_capacity_mw, 
        installed_capacity_mw, available_capacity_mw
    """
    # Read current circuits
    circuits = dlt.read("dev_iedr.gold.circuits_current").filter(F.col("__END_AT").isNull())
    
    # Read DER summary
    der_summary = dlt.read("dev_iedr.gold.feeder_der_summary")
    
    # Join and calculate available capacity
    return circuits.join(
        der_summary,
        on=["feeder_id", "utility_id"],
        how="left"
    ).select(
        circuits.feeder_id,
        circuits.utility_id,
        circuits.native_feeder_id,
        circuits.max_hosting_capacity_mw,
        circuits.min_hosting_capacity_mw,
        F.coalesce(der_summary.total_installed_capacity_kw / 1000.0, F.lit(0.0)).alias("installed_capacity_mw"),
        (
            circuits.max_hosting_capacity_mw - 
            F.coalesce(der_summary.total_installed_capacity_kw / 1000.0, F.lit(0.0))
        ).alias("available_capacity_mw"),
        circuits.hca_refresh_date.alias("capacity_as_of_date")
    )


# ==============================================================================
# GOLD VIEW: FEEDER MAP LAYER (Map Renderer)
# ==============================================================================

@dlt.table(
    name="dev_iedr.gold.feeder_map_layer",
    comment="Denormalized feeder data for map rendering - single query, zero joins required",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
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
    - DER counts (icon badges)
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
    circuits = dlt.read("dev_iedr.gold.circuits_current").filter(F.col("__END_AT").isNull())
    
    # Read DER summary
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
        circuits.color_code,
        circuits.max_hosting_capacity_mw,
        circuits.min_hosting_capacity_mw,
        
        # Available capacity calculation
        (
            circuits.max_hosting_capacity_mw - 
            F.coalesce(der_summary.total_installed_capacity_kw / 1000.0, F.lit(0.0))
        ).alias("available_capacity_mw"),
        
        # DER aggregates (icon badges)
        F.coalesce(der_summary.installed_der_count, F.lit(0)).alias("installed_der_count"),
        F.coalesce(der_summary.planned_der_count, F.lit(0)).alias("planned_der_count"),
        F.coalesce(der_summary.total_installed_capacity_kw, F.lit(0.0)).alias("total_installed_capacity_kw"),
        F.coalesce(der_summary.total_planned_capacity_kw, F.lit(0.0)).alias("total_planned_capacity_kw"),
        
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
        circuits.ingestion_date.alias("data_as_of_date")
    )
