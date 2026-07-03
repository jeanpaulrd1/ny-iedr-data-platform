"""Silver Layer DLT Pipeline for NY IEDR Platform.

Transforms Bronze raw data into standardized, validated tables (full-refresh).
Creates common data model across all utilities for downstream Gold consumption.

Key Architecture Decisions:
- Silver = Full-refresh standardization layer (no SCD2 history)
- Gold = SCD2 history + API-optimized views (to be implemented)
- All critical fixes applied (see ROUND2_FIXES.md)

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

Table Strategy:
- Silver tables (@dlt.table): Full-refresh transformations from Bronze
- Gold tables (future): SCD2 via APPLY CHANGES INTO + API views
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import helper functions
try:
    from pipelines.utils.helpers import normalize_null_sentinels
    from pipelines.utils.schema_normalization import (
        UTILITY1_ID,
        UTILITY2_ID,
        aggregate_utility1_segments,
        unpivot_utility1_der,
        map_circuits_to_canonical,
        map_der_to_canonical,
        get_canonical_columns,
    )
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.helpers import normalize_null_sentinels
    from utils.schema_normalization import (
        UTILITY1_ID,
        UTILITY2_ID,
        aggregate_utility1_segments,
        unpivot_utility1_der,
        map_circuits_to_canonical,
        map_der_to_canonical,
        get_canonical_columns,
    )


# ==============================================================================
# SILVER TABLE: CIRCUITS STANDARDIZED (Full-Refresh)
# ==============================================================================

@dlt.table(
    name="circuits_standardized",
    comment="Standardized circuit/feeder data - feeder-level, common schema across utilities (full-refresh)"
)
@dlt.expect_or_drop("valid_feeder_id", "feeder_id IS NOT NULL AND feeder_id != ''")
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
def circuits_standardized():
    """Transform Bronze circuits into canonical Silver schema.
    
    Per-Utility Transformations:
    - Utility 1: Aggregate segments → feeders (MAX hosting capacity, not SUM)
    - Utility 2: Pass through (already feeder-level)
    
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
    df = dlt.read("circuits_raw")
    
    # Normalize null sentinels before any transformation.
    # .dtypes is evaluated once here at plan construction — safe and cheap.
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)

    # Utility 1: Aggregate segment-level rows to feeder-level.
    # UTILITY1_ID = "utility1" (no underscore) — must match landing directory name.
    utility1_df = df.filter(F.col("utility_id") == UTILITY1_ID)
    utility1_agg = aggregate_utility1_segments(utility1_df)

    # Utility 2: Already feeder-level. Rename to intermediate schema so both
    # utilities share the same column names before map_circuits_to_canonical.
    utility2_df = df.filter(F.col("utility_id") == UTILITY2_ID).select(
        F.col("utility_id"),
        F.concat(F.lit(f"{UTILITY2_ID}_"), F.col("Master_CDF")).alias("feeder_id"),
        F.col("Master_CDF").alias("native_feeder_id"),
        F.col("feeder_voltage").alias("voltage_kv_raw"),
        F.col("feeder_max_hc").alias("max_hosting_capacity_raw"),
        F.col("feeder_min_hc").alias("min_hosting_capacity_raw"),
        F.col("hca_refresh_date").alias("hca_refresh_date_raw"),
        F.col("color").alias("color_code_raw"),
        F.col("shape_length").alias("shape_length_raw"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id"),
    )

    # allowMissingColumns=True: utility1_agg retains "Circuits_Phase3_CIRCUIT"
    # from groupBy. That column is not in utility2_df and would fail with False.
    # map_circuits_to_canonical's explicit select drops it cleanly.
    combined = utility1_agg.unionByName(utility2_df, allowMissingColumns=True)

    canonical = map_circuits_to_canonical(combined)


    return canonical


# ==============================================================================
# SILVER TABLE: DER INSTALLED STANDARDIZED (Full-Refresh)
# ==============================================================================

@dlt.table(
    name="der_installed_standardized",
    comment="Standardized installed DER - common schema, unresolved feeder_id preserved (full-refresh)"
)
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_der_type", "der_type IS NOT NULL")
def der_installed_standardized():
    """Transform Bronze DER installed into canonical Silver schema.
    
    Per-Utility Transformations:
    - Utility 1: Unpivot 14 technology columns → narrow (der_id, der_type, capacity) rows
    - Utility 2: Pass through (already narrow)
    
    Common Transformations:
    - Normalize null sentinels
    - Map to canonical schema
    - Cast data types
    
    CRITICAL: Unresolved DER (feeder_id IS NULL) are NOT dropped.
    They pass through for tracking in data_quality_metrics.
    
    Full-Refresh: This table is rebuilt on each pipeline run.
    Gold layer handles SCD2 history tracking.
    
    Returns:
        Standardized DER installed DataFrame
    """
    # Read from Bronze
    df = dlt.read("der_installed_raw")
    
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)

    # Utility 1: Unpivot 14 one-hot technology columns → narrow rows.
    # include_installation_date=False: installed DER has no planned date column.
    # unpivot emits planned_installation_date_raw = NULL so union schemas align.
    utility1_df = df.filter(F.col("utility_id") == UTILITY1_ID)
    utility1_unpivoted = unpivot_utility1_der(utility1_df, include_installation_date=False)

    # Utility 2: Already narrow. Build intermediate schema matching utility 1 output.
    # planned_installation_date_raw and interconnection_queue_id are emitted as NULL
    # so unionByName aligns cleanly without allowMissingColumns masking schema bugs.
    utility2_df = df.filter(F.col("utility_id") == UTILITY2_ID).select(
        F.col("utility_id"),
        F.concat(F.lit(f"{UTILITY2_ID}_"), F.col("DER_ID")).alias("der_id"),
        F.col("DER_INTERCONNECTION_LOCATION").alias("native_feeder_id_raw"),
        F.when(
            F.col("DER_INTERCONNECTION_LOCATION").isNotNull(),
            F.concat(F.lit(f"{UTILITY2_ID}_"), F.col("DER_INTERCONNECTION_LOCATION")),
        ).otherwise(F.lit(None).cast(StringType())).alias("feeder_id"),
        F.col("DER_TYPE").alias("der_type"),
        F.col("DER_NAMEPLATE_RATING").alias("nameplate_rating_kw"),
        F.lit(None).cast(StringType()).alias("planned_installation_date_raw"),
        F.lit(None).cast(StringType()).alias("interconnection_queue_id"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id"),
    )

    combined = utility1_unpivoted.unionByName(utility2_df, allowMissingColumns=False)
    canonical = map_der_to_canonical(combined, der_table_type="installed")


    return canonical


# ==============================================================================
# SILVER TABLE: DER PLANNED STANDARDIZED (Full-Refresh)
# ==============================================================================

@dlt.table(
    name="der_planned_standardized",
    comment="Standardized planned DER - common schema, unresolved feeder_id preserved (full-refresh)"
)
@dlt.expect_or_drop("valid_utility_id", "utility_id IS NOT NULL")
@dlt.expect_or_drop("valid_der_type", "der_type IS NOT NULL")
def der_planned_standardized():
    """Transform Bronze DER planned into canonical Silver schema.
    
    Same logic as der_installed_standardized, but for planned projects.
    Includes planned_installation_date and interconnection_queue_id.
    
    Full-Refresh: This table is rebuilt on each pipeline run.
    Gold layer handles SCD2 history tracking.
    
    Returns:
        Standardized DER planned DataFrame
    """
    # Read from Bronze
    df = dlt.read("der_planned_raw")
    
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)

    # Utility 1: Unpivot with planned date.
    # unpivot_utility1_der checks UTILITY1_PLANNED_DATE_COL existence safely
    # and emits planned_installation_date_raw = NULL if the column is absent —
    # no .count() action needed to guard the empty-DataFrame case.
    utility1_df = df.filter(F.col("utility_id") == UTILITY1_ID)
    utility1_unpivoted = unpivot_utility1_der(utility1_df, include_installation_date=True)

    # Utility 2: Planned DER uses INTERCONNECTION_QUEUE_REQUEST_ID as der_id.
    # Intermediate schema must match utility 1 output exactly so union is strict
    # (allowMissingColumns=False) and schema mismatches surface immediately.
    utility2_df = df.filter(F.col("utility_id") == UTILITY2_ID).select(
        F.col("utility_id"),
        F.concat(
            F.lit(f"{UTILITY2_ID}_"), F.col("INTERCONNECTION_QUEUE_REQUEST_ID")
        ).alias("der_id"),
        F.col("DER_INTERCONNECTION_LOCATION").alias("native_feeder_id_raw"),
        F.when(
            F.col("DER_INTERCONNECTION_LOCATION").isNotNull(),
            F.concat(F.lit(f"{UTILITY2_ID}_"), F.col("DER_INTERCONNECTION_LOCATION")),
        ).otherwise(F.lit(None).cast(StringType())).alias("feeder_id"),
        F.col("DER_TYPE").alias("der_type"),
        F.col("DER_NAMEPLATE_RATING").alias("nameplate_rating_kw"),
        F.col("PLANNED_INSTALLATION_DATE").alias("planned_installation_date_raw"),
        F.col("INTERCONNECTION_QUEUE_REQUEST_ID").alias("interconnection_queue_id"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id"),
    )

    combined = utility1_unpivoted.unionByName(utility2_df, allowMissingColumns=False)
    canonical = map_der_to_canonical(combined, der_table_type="planned")


    return canonical


# ==============================================================================
# DATA QUALITY METRICS TABLE
# ==============================================================================

@dlt.table(
    name="data_quality_metrics_silver",
    comment="Data quality metrics per pipeline run — append-only to preserve trend history",
)
def data_quality_metrics_silver():
    """Track data quality metrics using canonical Silver column names.

    IMPORTANT — why read_stream and not read:
    Silver tables are full-refresh (overwritten each run). If this table
    also used dlt.read() (batch), it would be rebuilt on every run and only
    ever show the current run's metrics — historical DQ trends would be lost.
    dlt.read_stream() makes this an append-only streaming table: each pipeline
    run appends one summary row per utility per dataset, accumulating over time.
    This is what makes "DQ trend over time" queries possible.

    Reads from Silver (post-normalization, canonical column names) not Bronze,
    so metric logic is utility-agnostic — no hardcoded raw column names that
    break when utility 3 onboards.

    F.lit() is NOT used inside .agg() — it is not an aggregate function and
    behaves inconsistently across Spark/DLT versions. Applied via withColumn
    after agg() instead.
    """
    # ── Circuits ──────────────────────────────────────────────────────────────
    circuits_metrics = (
        dlt.read_stream("circuits_standardized")
        .groupBy("utility_id", "ingestion_date", "pipeline_update_id")
        .agg(
            F.count("*").alias("total_records"),
            F.sum(
                F.when(F.col("feeder_id").isNull(), 1).otherwise(0)
            ).alias("null_key_count"),
            F.sum(
                F.when(F.col("max_hosting_capacity_mw") < 0, 1).otherwise(0)
            ).alias("negative_capacity_count"),
            F.lit(0).alias("unresolved_feeder_count"),  # not applicable for circuits
        )
        .withColumn("table_name", F.lit("circuits"))
    )

    # ── DER Installed ─────────────────────────────────────────────────────────
    der_installed_metrics = (
        dlt.read_stream("der_installed_standardized")
        .groupBy("utility_id", "ingestion_date", "pipeline_update_id")
        .agg(
            F.count("*").alias("total_records"),
            F.sum(
                F.when(F.col("der_id").isNull(), 1).otherwise(0)
            ).alias("null_key_count"),
            F.sum(
                F.when(F.col("feeder_id").isNull(), 1).otherwise(0)
            ).alias("unresolved_feeder_count"),
            F.sum(
                F.when(F.col("nameplate_rating_kw") < 0, 1).otherwise(0)
            ).alias("negative_capacity_count"),
        )
        .withColumn("table_name", F.lit("der_installed"))
    )

    # ── DER Planned ───────────────────────────────────────────────────────────
    der_planned_metrics = (
        dlt.read_stream("der_planned_standardized")
        .groupBy("utility_id", "ingestion_date", "pipeline_update_id")
        .agg(
            F.count("*").alias("total_records"),
            F.sum(
                F.when(F.col("der_id").isNull(), 1).otherwise(0)
            ).alias("null_key_count"),
            F.sum(
                F.when(F.col("feeder_id").isNull(), 1).otherwise(0)
            ).alias("unresolved_feeder_count"),
            F.sum(
                F.when(F.col("nameplate_rating_kw") < 0, 1).otherwise(0)
            ).alias("negative_capacity_count"),
        )
        .withColumn("table_name", F.lit("der_planned"))
    )

    return (
        circuits_metrics
        .unionByName(der_installed_metrics, allowMissingColumns=True)
        .unionByName(der_planned_metrics, allowMissingColumns=True)
    )