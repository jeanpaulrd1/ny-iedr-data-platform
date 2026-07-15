"""Silver Layer DLT Pipeline for NY IEDR Platform.

Transforms Bronze raw data into standardized, validated tables (full-refresh).
Creates common data model across all utilities for downstream Gold consumption.

Key Architecture Decisions:
- Silver = Full-refresh standardization layer (no SCD2 history)
- Gold = SCD2 history + API-optimized views
- N-utility support via registry pattern (add utilities without changing pipeline code)

Design Pattern:
- Utility Registry: Each utility's transformation logic is defined in utility_registry.py
- To add utility3: Write 3 transformer functions, register in UTILITY_REGISTRY
- Pipeline automatically processes all registered utilities via dynamic loop
- NO hardcoded utility IDs in this file (scalable to N utilities)

Key Corrections Applied:
1. Single-pass CASE WHEN (not filter-union per utility)
2. Real CSV column names from actual data
3. Full-refresh Silver (SCD2 deferred to Gold)
4. NO expect_or_drop on DER feeder_id (unresolved pass through)
5. NO left_semi join (referential validation at Gold)
6. Utility 1 DER unpivot implemented
7. Utility 1 circuits segment aggregation implemented
8. DQ metrics read from staging (canonical column names)
9. NO partitioning on Silver (pure liquid clustering)
10. DER composite key (der_id, der_type) for unpivot uniqueness

Additional Fixes (Round 2):
11. No .count() actions inside DLT functions
12. Intermediate schema alignment between utilities
13. DQ metrics uses canonical column names (not utility-specific)
14. InServiceDate handling for planned DER (conditional)
15. SCPAP lints addressed

Additional Enhancements (Round 3):
16. N-utility support via utility_registry.py
17. Dynamic loop over registered utilities (no hardcoded utility IDs)
18. Fully-qualified table names (dev_iedr.bronze.*, dev_iedr.silver.*)

Observability Enhancements (Round 4):
19. Freshness monitoring: Track last_refresh_date and days_since_refresh
20. Volume baseline tracking: Enable historical trend analysis

DQ Gap Fixes (Round 5):
21. Quarantine non-canonical der_type values via expect_or_quarantine
22. Track unmapped utility2 DER_TYPE values in dedicated DQ table
23. Alert on schema drift and mapping gaps

Table Strategy:
- Silver tables (@dlt.table): Full-refresh transformations from Bronze
- Gold tables: SCD2 via APPLY CHANGES INTO + API views
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import helper functions and utility registry
from pipelines.utils.schema_normalization import (
    map_circuits_to_canonical,
    map_der_to_canonical,
    UTILITY1_DER_TECH_COLUMNS
)
from pipelines.utils.utility_registry import get_registered_utilities
from pipelines.utils.dq_metrics import track_unmapped_der_types


# ==============================================================================
# BRONZE → SILVER: CIRCUITS (FEEDER-LEVEL)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.circuits_standardized",
    comment="Standardized feeder-level circuits across all utilities (full-refresh)"
)
@dlt.expect_or_drop("valid_feeder_id", "feeder_id IS NOT NULL")
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_hca_refresh_date", "hca_refresh_date IS NOT NULL")
def circuits_standardized():
    """Standardize circuit data from all utilities to feeder-level common schema.
    
    Architecture:
    - Reads from Bronze circuits_raw (all utilities)
    - Applies utility-specific transformations from registry
    - Unions results to single standardized table
    - Full-refresh: Rebuilt on each pipeline run
    
    Transformations Applied:
    - utility1: Aggregate segment-level → feeder-level (MODE capacity)
    - utility2: Direct feeder-level mapping
    - All: Standardize color codes, null sentinels, column names
    
    Returns:
        Feeder-level circuits DataFrame with unified schema
    """
    bronze_circuits = dlt.read("dev_iedr.bronze.circuits_raw")
    
    # Get all registered utilities dynamically
    registered_utilities = get_registered_utilities()
    
    # Transform each utility's data
    transformed_dfs = []
    for utility_id, config in registered_utilities.items():
        utility_df = bronze_circuits.filter(F.col("utility_id") == utility_id)
        transformed_df = config.circuits_transformer(utility_df)
        transformed_dfs.append(transformed_df)
    
    # Union all utilities (allowMissingColumns=True for intermediate schema)
    combined = transformed_dfs[0]
    for df in transformed_dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    
    # Map to canonical schema (removes _raw suffixes, standardizes types)
    canonical = map_circuits_to_canonical(combined)
    
    return canonical


# ==============================================================================
# BRONZE → SILVER: DER INSTALLED (NARROW FORMAT)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.der_installed_standardized",
    comment="Standardized installed DER across all utilities (full-refresh, narrow format)"
)
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_der_type", "der_type IS NOT NULL")
@dlt.expect_or_quarantine(
    "canonical_der_type_only",
    f"der_type IN {tuple(UTILITY1_DER_TECH_COLUMNS)}"
)
def der_installed_standardized():
    """Standardize installed DER data from all utilities to narrow common schema.
    
    Architecture:
    - Reads from Bronze der_installed_raw (all utilities)
    - Applies utility-specific transformations from registry
    - Unions results to single standardized table
    - Full-refresh: Rebuilt on each pipeline run
    
    Transformations Applied:
    - utility1: Unpivot wide format (14 tech columns) → narrow
    - utility2: Direct narrow mapping
    - All: Standardize DER types, null sentinels, column names
    
    Data Quality:
    - Quarantines rows with non-canonical der_type values
    - Unresolved feeders (feeder_id IS NULL) pass through for DQ tracking
    
    Returns:
        Narrow-format DER DataFrame with unified schema
    """
    bronze_der_installed = dlt.read("dev_iedr.bronze.der_installed_raw")
    
    # Get all registered utilities dynamically
    registered_utilities = get_registered_utilities()
    
    # Transform each utility's data
    transformed_dfs = []
    for utility_id, config in registered_utilities.items():
        utility_df = bronze_der_installed.filter(F.col("utility_id") == utility_id)
        transformed_df = config.der_installed_transformer(utility_df)
        transformed_dfs.append(transformed_df)
    
    # Union all utilities (allowMissingColumns=True for intermediate schema)
    combined = transformed_dfs[0]
    for df in transformed_dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    
    # Map to canonical schema (removes _raw suffixes, standardizes types)
    canonical = map_der_to_canonical(combined, der_table_type="installed")
    
    return canonical


# ==============================================================================
# BRONZE → SILVER: DER PLANNED (NARROW FORMAT)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.der_planned_standardized",
    comment="Standardized planned DER across all utilities (full-refresh, narrow format)"
)
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_der_type", "der_type IS NOT NULL")
@dlt.expect_or_quarantine(
    "canonical_der_type_only",
    f"der_type IN {tuple(UTILITY1_DER_TECH_COLUMNS)}"
)
def der_planned_standardized():
    """Standardize planned DER data from all utilities to narrow common schema.
    
    Architecture:
    - Reads from Bronze der_planned_raw (all utilities)
    - Applies utility-specific transformations from registry
    - Unions results to single standardized table
    - Full-refresh: Rebuilt on each pipeline run
    
    Transformations Applied:
    - utility1: Unpivot wide format (14 tech columns) → narrow
    - utility2: Direct narrow mapping
    - All: Standardize DER types, null sentinels, column names
    
    Data Quality:
    - Quarantines rows with non-canonical der_type values
    - Unresolved feeders (feeder_id IS NULL) pass through for DQ tracking
    
    Returns:
        Narrow-format DER DataFrame with unified schema
    """
    bronze_der_planned = dlt.read("dev_iedr.bronze.der_planned_raw")
    
    # Get all registered utilities dynamically
    registered_utilities = get_registered_utilities()
    
    # Transform each utility's data
    transformed_dfs = []
    for utility_id, config in registered_utilities.items():
        utility_df = bronze_der_planned.filter(F.col("utility_id") == utility_id)
        transformed_df = config.der_planned_transformer(utility_df)
        transformed_dfs.append(transformed_df)
    
    # Union all utilities (allowMissingColumns=True for intermediate schema)
    combined = transformed_dfs[0]
    for df in transformed_dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    
    # Map to canonical schema (removes _raw suffixes, standardizes types)
    canonical = map_der_to_canonical(combined, der_table_type="planned")
    
    return canonical


# ==============================================================================
# DATA QUALITY METRICS (STREAMING APPEND)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.data_quality_metrics_silver",
    comment="Data quality metrics with freshness monitoring (streaming append - new metrics per run)"
)
def data_quality_metrics_silver():
    """Compute data quality metrics from Silver standardized tables.
    
    Metrics Tracked:
    - Total record counts per utility
    - Null key counts (critical business keys)
    - Negative capacity counts (impossible values)
    - Unresolved feeder counts (DER with NULL feeder_id)
    - Freshness monitoring (circuits only):
      * last_refresh_date: Most recent hca_refresh_date from data
      * days_since_refresh: Days between pipeline run and last data refresh
    
    Streaming Pattern:
    - Uses readStream from Silver tables (streaming source)
    - Appends new metrics each pipeline run
    - Enables time-series trend analysis of data quality
    - Gold layer can aggregate for dashboards
    - skipChangeCommits=true: Silver tables are full-refresh (overwrite), which creates
    change commits that streaming readers must skip to avoid failures
    
    Observability:
    - Freshness monitoring detects stale data (circuits)
    - Volume tracking enables anomaly detection (via historical queries)
    - Unresolved feeder tracking highlights missing linkage data
    
    Returns:
        Data quality metrics DataFrame (one row per table/utility/run)
    """
    # Read from Silver tables (streaming) with skipChangeCommits
    # Silver is full-refresh, so we must skip change commits to handle overwrites
    circuits = (
    spark.readStream
    .option("skipChangeCommits", "true")
    .table("dev_iedr.silver.circuits_standardized")
    )

    der_installed = (
    spark.readStream
    .option("skipChangeCommits", "true")
    .table("dev_iedr.silver.der_installed_standardized")
    )
    
    der_planned = (
    spark.readStream
    .option("skipChangeCommits", "true")
    .table("dev_iedr.silver.der_planned_standardized")
    )
    
    # Circuits DQ metrics WITH FRESHNESS MONITORING
    circuits_dq = circuits.groupBy("utility_id", "ingestion_date", "pipeline_update_id").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("feeder_id").isNull(), 1).otherwise(0)).alias("null_key_count"),
        F.sum(F.when(F.col("max_hosting_capacity_mw") < 0, 1).otherwise(0)).alias("negative_capacity_count"),
        F.lit(0).alias("unresolved_feeder_count"),  # Circuits always have feeder_id (expect_or_drop)
        # FRESHNESS MONITORING
        F.max("hca_refresh_date").alias("last_refresh_date"),
        F.datediff(F.current_date(), F.max("hca_refresh_date")).alias("days_since_refresh")
    ).withColumn("table_name", F.lit("circuits"))
    
    # DER Installed DQ metrics (no freshness - DER doesn't have refresh dates)
    der_installed_dq = der_installed.groupBy("utility_id", "ingestion_date", "pipeline_update_id").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("der_id").isNull(), 1).otherwise(0)).alias("null_key_count"),
        F.sum(F.when(F.col("nameplate_rating_kw") < 0, 1).otherwise(0)).alias("negative_capacity_count"),
        F.sum(F.when(F.col("feeder_id").isNull() | (F.col("feeder_id") == ""), 1).otherwise(0)).alias("unresolved_feeder_count"),
        F.lit(None).cast("date").alias("last_refresh_date"),  # NULL for DER tables
        F.lit(None).cast("int").alias("days_since_refresh")   # NULL for DER tables
    ).withColumn("table_name", F.lit("der_installed"))
    
    # DER Planned DQ metrics (no freshness - DER doesn't have refresh dates)
    der_planned_dq = der_planned.groupBy("utility_id", "ingestion_date", "pipeline_update_id").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("der_id").isNull(), 1).otherwise(0)).alias("null_key_count"),
        F.sum(F.when(F.col("nameplate_rating_kw") < 0, 1).otherwise(0)).alias("negative_capacity_count"),
        F.sum(F.when(F.col("feeder_id").isNull() | (F.col("feeder_id") == ""), 1).otherwise(0)).alias("unresolved_feeder_count"),
        F.lit(None).cast("date").alias("last_refresh_date"),  # NULL for DER tables
        F.lit(None).cast("int").alias("days_since_refresh")   # NULL for DER tables
    ).withColumn("table_name", F.lit("der_planned"))
    
    # Union all DQ metrics
    all_dq = circuits_dq.unionByName(der_installed_dq).unionByName(der_planned_dq)
    
    return all_dq


# ==============================================================================
# DATA QUALITY: UNMAPPED DER_TYPE TRACKING
# ==============================================================================

@dlt.table(
    name="dev_iedr.dq.unmapped_der_types",
    comment="Tracks utility2 DER_TYPE values not in canonical mapping (streaming append)"
)
def unmapped_der_types_metric():
    """Track utility2 DER_TYPE values that don't map to canonical names.
    
    This table alerts on:
    - New DER_TYPE values from utility2 not in UTILITY2_DER_TYPE_MAP
    - Typos in the mapping dictionary
    - Casing/spacing variations not handled
    
    Quarantined Rows:
    Rows that fail the canonical_der_type_only expectation are automatically
    quarantined to:
    - dev_iedr.silver.__der_installed_standardized_quarantine
    - dev_iedr.silver.__der_planned_standardized_quarantine
    
    This table provides aggregated metrics for alerting/monitoring.
    
    Streaming Pattern:
    - Reads from Silver DER tables (skipChangeCommits=true)
    - Appends new metrics each pipeline run
    - Empty result = all der_type values are canonical (good state)
    - Non-empty = unmapped values detected (alert)
    
    Returns:
        DataFrame with unmapped der_type metrics per (utility_id, der_type)
    """
    # Read from Silver DER tables (streaming) with skipChangeCommits
    der_installed = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("dev_iedr.silver.der_installed_standardized")
    )
    
    der_planned = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("dev_iedr.silver.der_planned_standardized")
    )
    
    # Union installed + planned
    all_der = der_installed.unionByName(der_planned, allowMissingColumns=True)
    
    # Track unmapped values
    return track_unmapped_der_types(all_der)
