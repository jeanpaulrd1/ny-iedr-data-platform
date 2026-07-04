"""Schema normalization utilities for NY IEDR data platform.

Handles utility-specific transformations:
- Utility 1: Segment aggregation + DER unpivot
- Utility 2: Direct pass-through (already feeder-level, narrow format)
- Utility-agnostic: Null normalization, canonical field mapping

Performance: Single-pass transformations using CASE WHEN, not filter-union.

UTILITY ID NAMING CONTRACT:
utility_id values are extracted from the landing zone directory name by
extract_utility_id_from_path() in helpers.py. The directory structure is:
  /Volumes/.../landing/{utility_id}/circuits/

This means the landing directories MUST be named without underscores:
  utility1/   →  utility_id = "utility1"
  utility2/   →  utility_id = "utility2"

All filter expressions here use "utility1" / "utility2" (no underscore).
If landing directories are renamed, update UTILITY_IDS below and all
filter expressions referencing them. Never hardcode the string in multiple
places — use the constants defined here.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List

# Canonical utility ID values — must match landing zone directory names exactly.
# Change here and nowhere else if utilities are renamed.
UTILITY1_ID = "utility1"
UTILITY2_ID = "utility2"

# Confirmed column name for planned installation date in utility 1's planned DER file.
# Verify against actual utility1_planned_der.csv header before running in production.
# If the column doesn't exist, planned_installation_date will be NULL for utility 1.
UTILITY1_PLANNED_DATE_COL = "InServiceDate"


# ==============================================================================
# UTILITY 1: SEGMENT AGGREGATION (Circuits)
# ==============================================================================

def aggregate_utility1_segments(df: DataFrame) -> DataFrame:
    """Aggregate utility 1 segment-level circuits to feeder-level.
    
    Utility 1 reports multiple segment rows per feeder. Aggregate to one feeder row:
    - MAX(FMAXHC) → max_hosting_capacity_mw (NOT SUM - capacity repeats across segments)
    - MIN(FMINHC) → min_hosting_capacity_mw
    - SUM(Shape_Length) → total circuit length for the feeder
    - MAX(FHCADATE) → most recent HCA refresh date
    - First non-NULL color code (color repeats, any value is representative)
    
    Args:
        df: Utility 1 circuits DataFrame with segment-level rows
        
    Returns:
        DataFrame aggregated to feeder level with prefixed feeder_id (utility1_*)
    """
    return df.groupBy("Circuits_Phase3_CIRCUIT").agg(
        # Composite key: prefix circuit ID with utility_id
        F.concat(F.lit("utility1_"), F.col("Circuits_Phase3_CIRCUIT")).alias("feeder_id"),
        
        # Utility ID (unchanged)
        F.first("utility_id").alias("utility_id"),
        
        # Native feeder ID (raw circuit ID before prefixing)
        F.first("Circuits_Phase3_CIRCUIT").alias("native_feeder_id"),
        
        # Hosting capacity fields (use MAX - capacity repeats across segments)
        F.max("NYHCPV_csv_FMAXHC").alias("max_hosting_capacity_raw"),
        F.max("NYHCPV_csv_FMINHC").alias("min_hosting_capacity_raw"),
        
        # Voltage (repeats across segments, any value is representative)
        F.first("NYHCPV_csv_FVOLTAGE").alias("voltage_kv_raw"),
        
        # HCA refresh date (take most recent date)
        F.max("NYHCPV_csv_FHCADATE").alias("hca_refresh_date_raw"),
        
        # Color code (repeats across segments, any non-NULL value is representative)
        F.first("NYHCPV_csv_NMAPCOLOR", ignorenulls=True).alias("color_code_raw"),
        
        # Shape_Length: SUM to get total circuit length for the feeder
        F.sum("Shape_Length").alias("shape_length_raw"),
        
        # Lineage columns (take first, all identical within circuit)
        F.first("ingestion_timestamp").alias("ingestion_timestamp"),
        F.first("ingestion_date").alias("ingestion_date"),
        F.first("pipeline_update_id").alias("pipeline_update_id")
    )


# ==============================================================================
# UTILITY 1: DER UNPIVOT (Wide → Narrow)
# ==============================================================================

def unpivot_utility1_der(df: DataFrame, include_installation_date: bool = False) -> DataFrame:
    """Unpivot utility 1 DER from wide (14 technology columns) to narrow format.
    
    Utility 1 reports one row per project with capacity columns for each technology:
    SolarPV, EnergyStorageSystem, Wind, MicroTurbine, SynchronousGenerator,
    InductionGenerator, FarmWaste, FuelCell, CombinedHeatandPower, GasTurbine,
    Hydro, InternalCombustionEngine, SteamTurbine, Other.
    
    Transform to one row per (project, technology) pair, filtering out zero-capacity
    technologies. For hybrid projects (multiple technologies), create separate rows
    with composite der_id: utility1_{ProjectID}_{Technology}.
    
    Args:
        df: Utility 1 DER DataFrame with wide technology columns
        include_installation_date: If True, include InServiceDate column as
            planned_installation_date_raw (for planned DER). If False, set to NULL
            (for installed DER).
            
    Returns:
        DataFrame in narrow format with one row per (project, technology) pair
    """
    # Technology columns to unpivot
    tech_cols = [
        "SolarPV", "EnergyStorageSystem", "Wind", "MicroTurbine",
        "SynchronousGenerator", "InductionGenerator", "FarmWaste", "FuelCell",
        "CombinedHeatandPower", "GasTurbine", "Hydro", "InternalCombustionEngine",
        "SteamTurbine", "Other"
    ]
    
    # Build stack expression: stack(14, 'SolarPV', SolarPV, 'Wind', Wind, ...)
    # Each pair is (technology_name_literal, capacity_column_reference)
    stack_expr = f"stack({len(tech_cols)}, " + ", ".join(
        f"'{tech}', `{tech}`" for tech in tech_cols
    ) + ") as (der_type, nameplate_rating_kw)"
    
    # Unpivot using stack() - creates one row per technology per project
    unpivoted = df.select(
        "ProjectID",
        "ProjectCircuitID",
        "utility_id",
        F.expr(stack_expr),
        # Installation date column (if requested)
        F.col(UTILITY1_PLANNED_DATE_COL).alias("planned_installation_date_raw") 
            if include_installation_date and UTILITY1_PLANNED_DATE_COL in df.columns
            else F.lit(None).cast("string").alias("planned_installation_date_raw"),
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
    )
    
    # Filter out zero-capacity technologies and build composite identifiers.
    # CRITICAL FIX: Cast to DOUBLE immediately after unpivot to prevent type errors
    # in downstream filters and aggregations. The stack() function produces STRING
    # output, causing "Cannot cast 'X.X' to BigIntegral" errors.
    return unpivoted.filter(
        (F.col("nameplate_rating_kw").cast("double") > 0) &
        (F.col("nameplate_rating_kw") != "0")
    ).select(
        # Composite der_id: utility1_{ProjectID}_{Technology} (unique per DER in hybrid projects)
        F.concat(
            F.lit("utility1_"),
            F.col("ProjectID"),
            F.lit("_"),
            F.col("der_type")
        ).alias("der_id"),
        
        # Composite feeder_id: utility1_{ProjectCircuitID} (matches circuits table)
        F.concat(
            F.lit("utility1_"),
            F.col("ProjectCircuitID")
        ).alias("feeder_id"),
        
        "utility_id",
        
        # Native feeder ID (raw circuit ID before prefixing)
        F.col("ProjectCircuitID").alias("native_feeder_id_raw"),
        
        "der_type",
        
        # Cast nameplate_rating_kw to DOUBLE (already filtered, safe to cast)
        F.col("nameplate_rating_kw").cast("double").alias("nameplate_rating_kw"),
        
        # Installation date (NULL for installed, populated for planned)
        "planned_installation_date_raw",
        
        # Lineage columns
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
    )


# ==============================================================================
# CANONICAL FIELD MAPPING
# ==============================================================================

def map_circuits_to_canonical(df: DataFrame) -> DataFrame:
    """Map utility-specific circuit columns to canonical schema.
    
    Single-pass transformation using CASE WHEN (not filter-union).
    Handles both aggregated utility 1 and native utility 2 formats.
    
    IMPORTANT: Expects intermediate schema from aggregate_utility1_segments and utility2 select.
    Both utilities should have columns: *_raw, native_feeder_id, feeder_id
    
    Canonical schema:
    - feeder_id (prefixed: utility1_*, utility2_*)
    - native_feeder_id (raw utility ID)
    - voltage_kv (DOUBLE)
    - max_hosting_capacity_mw (DOUBLE)
    - min_hosting_capacity_mw (DOUBLE)
    - hca_refresh_date (TIMESTAMP, parsed from string)
    - color_code (STRING)
    - shape_length (DOUBLE)
    
    Args:
        df: Union of utility 1 + utility 2 circuits with *_raw columns
        
    Returns:
        DataFrame with canonical schema, ready for SCD2 tracking
    """
    return df.select(
        "feeder_id",
        "utility_id",
        "native_feeder_id",
        
        # Type cast numeric fields
        F.col("voltage_kv_raw").cast("double").alias("voltage_kv"),
        F.col("max_hosting_capacity_raw").cast("double").alias("max_hosting_capacity_mw"),
        F.col("min_hosting_capacity_raw").cast("double").alias("min_hosting_capacity_mw"),
        
        # Parse timestamp (utility-specific formats handled by to_timestamp)
        F.to_timestamp(F.col("hca_refresh_date_raw")).alias("hca_refresh_date"),
        
        # Pass-through string fields
        F.col("color_code_raw").alias("color_code"),
        
        # Shape length (already aggregated for utility1, native for utility2) - cast to double
        F.col("shape_length_raw").cast("double").alias("shape_length"),
        
        # Lineage columns
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
    )


def map_der_to_canonical(df: DataFrame) -> DataFrame:
    """Map utility-specific DER columns to canonical schema.
    
    Single-pass transformation using CASE WHEN (not filter-union).
    Handles both unpivoted utility 1 and native utility 2 formats.
    
    IMPORTANT: Expects intermediate schema from unpivot_utility1_der and utility2 select.
    Both utilities should have columns: *_raw, der_id, feeder_id, der_type, nameplate_rating_kw
    
    Canonical schema:
    - der_id (composite for utility1, native ID for utility2)
    - feeder_id (prefixed: utility1_*, utility2_*)
    - native_feeder_id (raw feeder ID before prefixing)
    - der_type (standardized technology name)
    - nameplate_rating_kw (DOUBLE)
    - planned_installation_date (DATE, parsed from string)
    
    Args:
        df: Union of utility 1 + utility 2 DER with *_raw columns
        
    Returns:
        DataFrame with canonical schema, ready for SCD2 tracking
    """
    return df.select(
        "der_id",
        "feeder_id",
        "utility_id",
        "native_feeder_id_raw",
        "der_type",
        
        # Ensure nameplate_rating_kw is DOUBLE (already cast in unpivot for utility1)
        F.col("nameplate_rating_kw").cast("double").alias("nameplate_rating_kw"),
        
        # Parse planned installation date (may be NULL)
        F.to_date(F.col("planned_installation_date_raw")).alias("planned_installation_date"),
        
        # Lineage columns
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
    )
