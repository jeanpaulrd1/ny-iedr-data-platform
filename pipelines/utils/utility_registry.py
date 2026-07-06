"""Utility Registry - Configuration-driven multi-utility support.

This module defines how each utility's data is transformed from Bronze to Silver.
To add a new utility, register its transformation functions here - no changes
needed to the main pipeline files.

Architecture:
- Each utility has unique source schemas requiring custom transformation logic
- Registry pattern allows N utilities without hardcoding utility IDs in pipeline logic
- Transformation functions are utility-specific but follow a common interface
"""

from typing import Callable, Dict, NamedTuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import from relative modules (DLT Python files don't have __file__ defined)
try:
    from pipelines.utils.helpers import normalize_null_sentinels
    from pipelines.utils.schema_normalization import (
        aggregate_utility1_segments,
        unpivot_utility1_der
    )
except ImportError:
    from .helpers import normalize_null_sentinels
    from .schema_normalization import (
        aggregate_utility1_segments,
        unpivot_utility1_der
    )


class UtilityConfig(NamedTuple):
    """Configuration for a single utility's data transformations.
    
    Attributes:
        utility_id: Unique identifier (must match landing directory name)
        circuits_transformer: Function that transforms Bronze circuits → intermediate schema
        der_installed_transformer: Function that transforms Bronze DER installed → intermediate schema
        der_planned_transformer: Function that transforms Bronze DER planned → intermediate schema
    """
    utility_id: str
    circuits_transformer: Callable[[DataFrame], DataFrame]
    der_installed_transformer: Callable[[DataFrame], DataFrame]
    der_planned_transformer: Callable[[DataFrame], DataFrame]


# ==============================================================================
# UTILITY 1 TRANSFORMERS
# ==============================================================================

def transform_utility1_circuits(df: DataFrame) -> DataFrame:
    """Transform utility1 segment-level circuits to feeder-level intermediate schema.
    
    Utility 1 reports multiple segment rows per feeder. This function:
    1. Normalizes NULL sentinels
    2. Aggregates segments to feeder level (max capacity, sum length)
    3. Returns intermediate schema with prefixed feeder_id
    
    Args:
        df: Raw utility1 circuits from Bronze (already filtered to utility_id == "utility1")
        
    Returns:
        DataFrame with columns: feeder_id, utility_id, native_feeder_id, 
        voltage_kv_raw, max_hosting_capacity_raw, min_hosting_capacity_raw,
        hca_refresh_date_raw, color_code_raw, shape_length_raw, 
        ingestion_timestamp, ingestion_date, pipeline_update_id
    """
    # Normalize null sentinels
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)
    
    # Aggregate segments to feeder level
    return aggregate_utility1_segments(df)


def transform_utility1_der_installed(df: DataFrame) -> DataFrame:
    """Transform utility1 installed DER from wide format to intermediate schema.
    
    Utility 1 stores DER capacity in 14 technology columns (wide format).
    This function:
    1. Normalizes NULL sentinels
    2. Unpivots to narrow format (one row per DER)
    3. Returns intermediate schema (unpivot already includes prefixed feeder_id)
    
    Args:
        df: Raw utility1 DER installed from Bronze (already filtered)
        
    Returns:
        DataFrame with columns: der_id, feeder_id, utility_id, native_feeder_id_raw,
        der_type, nameplate_rating_kw, planned_installation_date_raw,
        interconnection_queue_id (NULL), ingestion_timestamp, ingestion_date, pipeline_update_id
    """
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)
    
    # Unpivot 14 technology columns (already includes feeder_id with utility prefix)
    unpivoted = unpivot_utility1_der(df, include_installation_date=False)
    
    # Add interconnection_queue_id as NULL (utility1 doesn't have this column)
    return unpivoted.withColumn("interconnection_queue_id", F.lit(None).cast(StringType()))


def transform_utility1_der_planned(df: DataFrame) -> DataFrame:
    """Transform utility1 planned DER from wide format to intermediate schema.
    
    Same logic as installed, but planned DER includes installation_date.
    
    Args:
        df: Raw utility1 DER planned from Bronze (already filtered)
        
    Returns:
        DataFrame with columns: der_id, feeder_id, utility_id, native_feeder_id_raw,
        der_type, nameplate_rating_kw, planned_installation_date_raw,
        interconnection_queue_id (NULL), ingestion_timestamp, ingestion_date, pipeline_update_id
    """
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)
    
    # Unpivot 14 technology columns (include installation_date for planned)
    # Already includes feeder_id with utility prefix
    unpivoted = unpivot_utility1_der(df, include_installation_date=True)
    
    # Add interconnection_queue_id as NULL (utility1 doesn't have this column)
    return unpivoted.withColumn("interconnection_queue_id", F.lit(None).cast(StringType()))


# ==============================================================================
# UTILITY 2 TRANSFORMERS
# ==============================================================================

def transform_utility2_circuits(df: DataFrame) -> DataFrame:
    """Transform utility2 feeder-level circuits to intermediate schema.
    
    Utility 2 data is already feeder-level (no aggregation needed).
    This function just renames columns to match the intermediate schema.
    
    Args:
        df: Raw utility2 circuits from Bronze (already filtered to utility_id == "utility2")
        
    Returns:
        DataFrame with same intermediate schema as utility1 transformer
    """
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)
    
    return df.select(
        F.col("utility_id"),
        F.concat(F.lit("utility2_"), F.col("Master_CDF")).alias("feeder_id"),
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


def transform_utility2_der_installed(df: DataFrame) -> DataFrame:
    """Transform utility2 installed DER to intermediate schema.
    
    Utility 2 DER data is already in narrow format (one row per DER).
    This function:
    1. Normalizes NULL sentinels
    2. Renames columns to match intermediate schema
    3. Prefixes feeder_id with utility_id
    
    Args:
        df: Raw utility2 DER installed from Bronze (already filtered)
        
    Returns:
        DataFrame with intermediate schema (interconnection_queue_id is NULL for installed)
    """
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)
    
    return df.select(
        F.col("utility_id"),
        F.col("DER_ID").alias("der_id"),
        F.concat(F.lit("utility2_"), F.col("DER_INTERCONNECTION_LOCATION")).alias("feeder_id"),
        F.col("DER_INTERCONNECTION_LOCATION").alias("native_feeder_id_raw"),
        F.col("DER_TYPE").alias("der_type"),
        F.col("DER_NAMEPLATE_RATING").cast("double").alias("nameplate_rating_kw"),
        F.lit(None).cast(StringType()).alias("planned_installation_date_raw"),
        F.lit(None).cast(StringType()).alias("interconnection_queue_id"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id")
    )


def transform_utility2_der_planned(df: DataFrame) -> DataFrame:
    """Transform utility2 planned DER to intermediate schema.
    
    Utility 2 planned DER uses INTERCONNECTION_QUEUE_REQUEST_ID as DER_ID.
    PLANNED_INSTALLATION_DATE is optional (may be NULL).
    
    Args:
        df: Raw utility2 DER planned from Bronze (already filtered)
        
    Returns:
        DataFrame with intermediate schema (includes interconnection_queue_id)
    """
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    df = normalize_null_sentinels(df, string_cols)
    
    return df.select(
        F.col("utility_id"),
        # Use INTERCONNECTION_QUEUE_REQUEST_ID as der_id (planned DER doesn't have DER_ID)
        F.col("INTERCONNECTION_QUEUE_REQUEST_ID").alias("der_id"),
        F.concat(F.lit("utility2_"), F.col("DER_INTERCONNECTION_LOCATION")).alias("feeder_id"),
        F.col("DER_INTERCONNECTION_LOCATION").alias("native_feeder_id_raw"),
        F.col("DER_TYPE").alias("der_type"),
        F.col("DER_NAMEPLATE_RATING").cast("double").alias("nameplate_rating_kw"),
        F.col("PLANNED_INSTALLATION_DATE").alias("planned_installation_date_raw"),
        F.col("INTERCONNECTION_QUEUE_REQUEST_ID").alias("interconnection_queue_id"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id")
    )


# ==============================================================================
# UTILITY REGISTRY
# ==============================================================================

# Register all supported utilities here.
# To add utility3:
# 1. Write its 3 transformer functions (circuits, der_installed, der_planned)
# 2. Add UtilityConfig("utility3", ...) to this list
# 3. Ensure landing zone has /Volumes/.../landing/utility3/
# No other pipeline changes needed!

UTILITY_REGISTRY: Dict[str, UtilityConfig] = {
    "utility1": UtilityConfig(
        utility_id="utility1",
        circuits_transformer=transform_utility1_circuits,
        der_installed_transformer=transform_utility1_der_installed,
        der_planned_transformer=transform_utility1_der_planned,
    ),
    "utility2": UtilityConfig(
        utility_id="utility2",
        circuits_transformer=transform_utility2_circuits,
        der_installed_transformer=transform_utility2_der_installed,
        der_planned_transformer=transform_utility2_der_planned,
    ),
}


def get_registered_utilities() -> Dict[str, UtilityConfig]:
    """Get all registered utilities.
    
    Returns:
        Dictionary mapping utility_id → UtilityConfig
    """
    return UTILITY_REGISTRY


def is_utility_registered(utility_id: str) -> bool:
    """Check if a utility is registered.
    
    Args:
        utility_id: Utility identifier to check
        
    Returns:
        True if registered, False otherwise
    """
    return utility_id in UTILITY_REGISTRY
