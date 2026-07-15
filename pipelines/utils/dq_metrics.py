"""Data Quality metrics utilities for NY IEDR data platform.

Provides reusable functions to track data quality issues:
- Unmapped der_type values from utility2
- Schema drift detection
- Null sentinel tracking

Usage:
    from pipelines.utils.dq_metrics import track_unmapped_der_types
    
    @dlt.table(name="dev_iedr.dq.unmapped_der_types")
    def unmapped_der_types_metric():
        return track_unmapped_der_types(dlt.read("silver.der_installed_standardized"))
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List

try:
    from pipelines.utils.schema_normalization import UTILITY1_DER_TECH_COLUMNS, UTILITY2_ID
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.schema_normalization import UTILITY1_DER_TECH_COLUMNS, UTILITY2_ID


def track_unmapped_der_types(df: DataFrame) -> DataFrame:
    """Track utility2 DER_TYPE values that don't map to canonical names.
    
    Identifies utility2 rows where der_type is NOT in the canonical technology list.
    This catches:
    - Typos in UTILITY2_DER_TYPE_MAP
    - New technology types not yet mapped
    - Casing/spacing variations not handled
    
    Returns one row per unmapped (utility_id, der_type) with:
    - unmapped_count: Number of DER projects with this unmapped value
    - unmapped_capacity_kw: Total nameplate capacity
    - first_seen: Earliest ingestion_date for this value
    - last_seen: Most recent ingestion_date for this value
    
    Args:
        df: DataFrame from silver.der_installed_standardized or der_planned_standardized
            Must have columns: utility_id, der_type, nameplate_rating_kw, ingestion_date
    
    Returns:
        DataFrame with unmapped der_type metrics (empty if all values mapped correctly)
    
    Example:
        >>> from pipelines.utils.dq_metrics import track_unmapped_der_types
        >>> installed = dlt.read("dev_iedr.silver.der_installed_standardized")
        >>> planned = dlt.read("dev_iedr.silver.der_planned_standardized")
        >>> all_der = installed.unionByName(planned)
        >>> unmapped_metrics = track_unmapped_der_types(all_der)
    """
    canonical_types = UTILITY1_DER_TECH_COLUMNS
    
    return df.filter(
        # Only utility2 rows (utility1 der_type is already canonical from unpivot)
        (F.col("utility_id") == UTILITY2_ID) &
        # Where canonical der_type is NOT in the known list
        (~F.col("der_type").isin(canonical_types))
    ).groupBy("utility_id", "der_type").agg(
        F.count("*").alias("unmapped_count"),
        F.sum("nameplate_rating_kw").alias("unmapped_capacity_kw"),
        F.min("ingestion_date").alias("first_seen"),
        F.max("ingestion_date").alias("last_seen")
    ).select(
        "utility_id",
        "der_type",
        "unmapped_count",
        "unmapped_capacity_kw",
        "first_seen",
        "last_seen",
        F.concat(
            F.lit("⚠️ Unmapped der_type '"),
            F.col("der_type"),
            F.lit("' found in utility2 data ("),
            F.col("unmapped_count"),
            F.lit(" projects, "),
            F.round(F.col("unmapped_capacity_kw"), 2),
            F.lit(" kW). Add to UTILITY2_DER_TYPE_MAP.")
        ).alias("alert_message")
    )


def validate_canonical_der_types(df: DataFrame) -> DataFrame:
    """Validate that all der_type values match the canonical list.
    
    Checks ALL rows (both utilities) to ensure der_type is in the canonical list.
    Use this to catch:
    - Mapping dictionary typos
    - Unexpected pass-through values
    - Schema drift
    
    Args:
        df: DataFrame with der_type column
    
    Returns:
        DataFrame with invalid der_type rows (empty if all valid)
    """
    canonical_types = UTILITY1_DER_TECH_COLUMNS
    
    return df.filter(
        ~F.col("der_type").isin(canonical_types)
    ).select(
        "utility_id",
        "der_id",
        "der_type",
        "nameplate_rating_kw",
        "ingestion_date"
    )


def track_null_sentinels(df: DataFrame, columns_to_check: List[str]) -> DataFrame:
    """Track rows with NULL sentinel values that weren't normalized.
    
    Catches:
    - "NULL", "null", "Null" strings
    - Empty strings
    - Whitespace-only strings
    
    Args:
        df: DataFrame to check
        columns_to_check: List of STRING column names to inspect
    
    Returns:
        DataFrame with one row per (column, sentinel_value) combination
    """
    # Cache columns to avoid multiple Analyze RPCs in loop
    available_columns = set(df.columns)
    metrics = []
    
    for col in columns_to_check:
        if col in available_columns:
            sentinel_df = df.filter(
                F.col(col).isNotNull() &
                (
                    (F.trim(F.col(col)) == "") |
                    (F.upper(F.trim(F.col(col))) == "NULL")
                )
            ).select(
                F.lit(col).alias("column_name"),
                F.col(col).alias("sentinel_value"),
                F.col("utility_id")
            )
            metrics.append(sentinel_df)
    
    if not metrics:
        # Return empty DataFrame with schema using SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(
            [],
            "column_name STRING, sentinel_value STRING, utility_id STRING, count BIGINT"
        )
    
    combined = metrics[0]
    for metric_df in metrics[1:]:
        combined = combined.unionByName(metric_df)
    
    return combined.groupBy("column_name", "sentinel_value", "utility_id").agg(
        F.count("*").alias("count")
    )
