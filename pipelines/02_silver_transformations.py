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

Table Strategy:
- Silver tables (@dlt.table): Full-refresh transformations from Bronze
- Gold tables: SCD2 via APPLY CHANGES INTO + API views
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import helper functions and utility registry
try:
    from pipelines.utils.schema_normalization import (
        map_circuits_to_canonical,
        map_der_to_canonical,
    )
    from pipelines.utils.utility_registry import get_registered_utilities
except ImportError:
    from utils.schema_normalization import (
        map_circuits_to_canonical,
        map_der_to_canonical,
    )
    from utils.utility_registry import get_registered_utilities


# ==============================================================================
# SILVER TABLE: CIRCUITS STANDARDIZED (Full-Refresh)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.circuits_standardized",
    comment="Standardized circuit/feeder data - feeder-level, common schema across all utilities (full-refresh)"
)
@dlt.expect_or_drop("valid_feeder_id", "feeder_id IS NOT NULL AND feeder_id != ''")
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_hca_refresh_date", "hca_refresh_date IS NOT NULL")  # Required for SCD2 sequence_by
def circuits_standardized():
    """Transform Bronze circuits into canonical Silver schema.
    
    Design Pattern (N-Utility Support):
    - Reads all registered utilities from utility_registry.py
    - Each utility's transformer function handles its specific schema
    - Intermediate schemas are aligned (same column names across utilities)
    - Union all utilities into single standardized table
    - Map to canonical schema (common data model)
    
    Per-Utility Transformations (examples):
    - Utility 1: Aggregate segments → feeders (MAX hosting capacity, not SUM)
    - Utility 2: Pass through (already feeder-level)
    - Utility 3: [Add your transformation logic to utility_registry.py]
    
    Common Transformations:
    - Normalize null sentinels ("NULL", "null", "" → SQL NULL)
    - Map to canonical schema (single-pass CASE WHEN)
    - Cast data types (STRING → DOUBLE/TIMESTAMP)
    - Validate business rules (feeder_id NOT NULL)
    
    Full-Refresh: This table is rebuilt on each pipeline run.
    Gold layer handles SCD2 history tracking.
    
    Returns:
        Standardized circuits DataFrame (feeder-grain)
    """
    # Read from Bronze
    df = dlt.read("dev_iedr.bronze.circuits_raw")
    
    # Get all registered utilities
    utilities = get_registered_utilities()
    
    # Transform each utility's data using its registered transformer
    utility_dfs = []
    for utility_id, config in utilities.items():
        utility_df = df.filter(F.col("utility_id") == utility_id)
        transformed = config.circuits_transformer(utility_df)
        utility_dfs.append(transformed)
    
    # Union all utilities
    # allowMissingColumns=True: Some utilities may have extra intermediate columns
    # (e.g., utility1 retains "Circuits_Phase3_CIRCUIT" from groupBy).
    # map_circuits_to_canonical's explicit select drops extras cleanly.
    combined = utility_dfs[0]
    for utility_df in utility_dfs[1:]:
        combined = combined.unionByName(utility_df, allowMissingColumns=True)
    
    # Map to canonical schema (common data model)
    canonical = map_circuits_to_canonical(combined)
    
    return canonical


# ==============================================================================
# SILVER TABLE: DER INSTALLED STANDARDIZED (Full-Refresh)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.der_installed_standardized",
    comment="Standardized installed DER - common schema, unresolved feeder_id preserved (full-refresh)"
)
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_der_type", "der_type IS NOT NULL")
def der_installed_standardized():
    """Transform Bronze DER installed into canonical Silver schema.
    
    Design Pattern (N-Utility Support):
    - Reads all registered utilities from utility_registry.py
    - Each utility's transformer handles its specific DER format
    - Utility 1: Unpivot 14 technology columns → narrow (der_id, der_type, capacity) rows
    - Utility 2: Already narrow format, just rename columns
    - Union all utilities into single standardized table
    
    Per-Utility Transformations:
    - Utility 1: Wide → narrow unpivot (14 tech types)
    - Utility 2: Already narrow, rename columns
    - Utility 3: [Add your transformation logic to utility_registry.py]
    
    Common Transformations:
    - Normalize null sentinels ("NULL", "null", "" → SQL NULL)
    - Map to canonical schema (single-pass CASE WHEN)
    - Cast data types (STRING → DOUBLE)
    - Validate business rules (utility_id, der_type NOT NULL)
    - feeder_id NULL preserved (no expect_or_drop - tracked in DQ metrics)
    
    Full-Refresh: This table is rebuilt on each pipeline run.
    Gold layer handles SCD2 history tracking.
    
    Returns:
        Standardized DER installed DataFrame (DER-grain)
    """
    # Read from Bronze
    df = dlt.read("dev_iedr.bronze.der_installed_raw")
    
    # Get all registered utilities
    utilities = get_registered_utilities()
    
    # Transform each utility's data using its registered transformer
    utility_dfs = []
    for utility_id, config in utilities.items():
        utility_df = df.filter(F.col("utility_id") == utility_id)
        transformed = config.der_installed_transformer(utility_df)
        utility_dfs.append(transformed)
    
    # Union all utilities
    combined = utility_dfs[0]
    for utility_df in utility_dfs[1:]:
        combined = combined.unionByName(utility_df, allowMissingColumns=True)
    
    # Map to canonical schema (common data model)
    canonical = map_der_to_canonical(combined, der_table_type="installed")
    
    return canonical


# ==============================================================================
# SILVER TABLE: DER PLANNED STANDARDIZED (Full-Refresh)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.der_planned_standardized",
    comment="Standardized planned DER - common schema, unresolved feeder_id preserved, queue status tracking (full-refresh)"
)
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_der_type", "der_type IS NOT NULL")
def der_planned_standardized():
    """Transform Bronze DER planned into canonical Silver schema.
    
    Design Pattern (N-Utility Support):
    - Reads all registered utilities from utility_registry.py
    - Each utility's transformer handles its specific DER format
    - Utility 1: Unpivot 14 technology columns + installation_date
    - Utility 2: Already narrow format, rename + add interconnection_queue_id
    - Union all utilities into single standardized table
    
    Per-Utility Transformations:
    - Utility 1: Wide → narrow unpivot (14 tech types) + installation_date
    - Utility 2: Already narrow, rename + interconnection_queue_id
    - Utility 3: [Add your transformation logic to utility_registry.py]
    
    Common Transformations:
    - Normalize null sentinels ("NULL", "null", "" → SQL NULL)
    - Map to canonical schema (single-pass CASE WHEN)
    - Cast data types (STRING → DOUBLE/TIMESTAMP)
    - Validate business rules (utility_id, der_type NOT NULL)
    - feeder_id NULL preserved (no expect_or_drop - tracked in DQ metrics)
    - installation_date and interconnection_queue_id tracked for planned status
    
    Full-Refresh: This table is rebuilt on each pipeline run.
    Gold layer handles SCD2 history tracking.
    
    Returns:
        Standardized DER planned DataFrame (DER-grain)
    """
    # Read from Bronze
    df = dlt.read("dev_iedr.bronze.der_planned_raw")
    
    # Get all registered utilities
    utilities = get_registered_utilities()
    
    # Transform each utility's data using its registered transformer
    utility_dfs = []
    for utility_id, config in utilities.items():
        utility_df = df.filter(F.col("utility_id") == utility_id)
        transformed = config.der_planned_transformer(utility_df)
        utility_dfs.append(transformed)
    
    # Union all utilities
    combined = utility_dfs[0]
    for utility_df in utility_dfs[1:]:
        combined = combined.unionByName(utility_df, allowMissingColumns=True)
    
    # Map to canonical schema (common data model)
    # Add interconnection_queue_id column if missing (utility1 doesn't have it)
    if "interconnection_queue_id" not in combined.columns:
        combined = combined.withColumn("interconnection_queue_id", F.lit(None).cast(StringType()))
    
    canonical = map_der_to_canonical(combined, der_table_type="planned")
    
    return canonical


# ==============================================================================
# SILVER TABLE: DATA QUALITY METRICS (Streaming Append)
# ==============================================================================

@dlt.table(
    name="dev_iedr.silver.data_quality_metrics_silver",
    comment="Data quality metrics computed from Silver tables (streaming append - new metrics per run)"
)
def data_quality_metrics_silver():
    """Compute data quality metrics from Silver standardized tables.
    
    Metrics Tracked:
    - Total record counts per utility
    - Null key counts (critical business keys)
    - Negative capacity counts (impossible values)
    - Unresolved feeder counts (DER with NULL feeder_id)
    
    Streaming Pattern:
    - Uses readStream from Silver tables (streaming source)
    - Appends new metrics each pipeline run
    - Enables time-series trend analysis of data quality
    - Gold layer can aggregate for dashboards
    - skipChangeCommits=true: Silver tables are full-refresh (overwrite), which creates
    change commits that streaming readers must skip to avoid failures
    
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
    
    # Circuits DQ metrics
    circuits_dq = circuits.groupBy("utility_id", "ingestion_date", "pipeline_update_id").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("feeder_id").isNull(), 1).otherwise(0)).alias("null_key_count"),
        F.sum(F.when(F.col("max_hosting_capacity_mw") < 0, 1).otherwise(0)).alias("negative_capacity_count"),
        F.lit(0).alias("unresolved_feeder_count")  # Circuits always have feeder_id (expect_or_drop)
    ).withColumn("table_name", F.lit("circuits"))
    
    # DER Installed DQ metrics
    der_installed_dq = der_installed.groupBy("utility_id", "ingestion_date", "pipeline_update_id").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("der_id").isNull(), 1).otherwise(0)).alias("null_key_count"),
        F.sum(F.when(F.col("nameplate_rating_kw") < 0, 1).otherwise(0)).alias("negative_capacity_count"),
        F.sum(F.when(F.col("feeder_id").isNull() | (F.col("feeder_id") == ""), 1).otherwise(0)).alias("unresolved_feeder_count")
    ).withColumn("table_name", F.lit("der_installed"))
    
    # DER Planned DQ metrics
    der_planned_dq = der_planned.groupBy("utility_id", "ingestion_date", "pipeline_update_id").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("der_id").isNull(), 1).otherwise(0)).alias("null_key_count"),
        F.sum(F.when(F.col("nameplate_rating_kw") < 0, 1).otherwise(0)).alias("negative_capacity_count"),
        F.sum(F.when(F.col("feeder_id").isNull() | (F.col("feeder_id") == ""), 1).otherwise(0)).alias("unresolved_feeder_count")
    ).withColumn("table_name", F.lit("der_planned"))
    
    # Union all DQ metrics
    all_dq = circuits_dq.unionByName(der_installed_dq).unionByName(der_planned_dq)
    
    return all_dq
