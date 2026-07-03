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
    - MAX(FHCADATE) → hca_refresh_date (most recent)
    - SUM(Shape_Length) → shape_length (additive, NULL-safe)
    
    Args:
        df: Raw utility 1 circuits from Bronze (filtered to utility_id = 'utility_1')
        
    Returns:
        Aggregated feeder-level DataFrame with intermediate column names
    """
    # df is already filtered to utility1 rows by the caller.
    # groupBy retains utility_id so downstream CASE WHEN expressions can match on it.
    return df.groupBy("Circuits_Phase3_CIRCUIT", "utility_id").agg(
        # Hosting capacity: MAX not SUM (repeats across segments)
        F.max("NYHCPV_csv_FMAXHC").alias("max_hosting_capacity_raw"),
        F.min("NYHCPV_csv_FMINHC").alias("min_hosting_capacity_raw"),
        
        # Voltage (should be same across segments)
        F.first("NYHCPV_csv_FVOLTAGE").alias("voltage_kv_raw"),
        
        # Most recent HCA refresh date
        F.max("NYHCPV_csv_FHCADATE").alias("hca_refresh_date_raw"),
        
        # Color code (should be same)
        F.first("NYHCPV_csv_NMAPCOLOR").alias("color_code_raw"),
        
        # Shape length: SUM (additive across segments)
        # NULL-safe: if all segments are NULL, result is 0 instead of NULL
        F.sum(F.coalesce(F.col("Shape_Length"), F.lit(0))).alias("shape_length_raw"),
        
        # Feeder ID
        F.first("NYHCPV_csv_FFEEDER").alias("native_feeder_id"),
        
        # Lineage columns
        F.max("ingestion_timestamp").alias("ingestion_timestamp"),
        F.max("ingestion_date").alias("ingestion_date"),
        F.first("pipeline_update_id").alias("pipeline_update_id")
    ).withColumn(
        "feeder_id",
        F.concat(F.lit("utility1_"), F.col("Circuits_Phase3_CIRCUIT"))
    )


# ==============================================================================
# UTILITY 1: DER UNPIVOT
# ==============================================================================

def unpivot_utility1_der(df: DataFrame, include_installation_date: bool = False) -> DataFrame:
    """Unpivot utility 1 wide DER format to narrow (der_id, der_type, capacity_kw).
    
    Utility 1 has 14 one-hot technology columns where value = nameplate capacity.
    Unpivot to rows: one row per non-zero technology.
    Hybrid projects correctly produce multiple rows.
    
    Technology columns:
    - SolarPV, EnergyStorageSystem, Wind, MicroTurbine, SynchronousGenerator,
      InductionGenerator, FarmWaste, FuelCell, CombinedHeatandPower,
      GasTurbine, Hydro, InternalCombustionEngine, SteamTurbine, Other
    
    Args:
        df: Raw utility 1 DER from Bronze (filtered to utility_id = 'utility_1')
        include_installation_date: If True, include InServiceDate column (for planned DER)
        
    Returns:
        Narrow DataFrame with intermediate column names matching utility 2 pattern
    """
    # Technology columns to unpivot
    tech_columns = [
        "SolarPV", "EnergyStorageSystem", "Wind", "MicroTurbine",
        "SynchronousGenerator", "InductionGenerator", "FarmWaste",
        "FuelCell", "CombinedHeatandPower", "GasTurbine",
        "Hydro", "InternalCombustionEngine", "SteamTurbine", "Other"
    ]
    
    # Stack all technology columns into (der_type, nameplate_rating_kw) pairs.
    # selectExpr with explicit base columns ensures the stack result joins cleanly
    # and no spurious wide columns bleed into the narrow output.
    stack_expr = f"stack({len(tech_columns)}, " + ", ".join(
        [f"'{tech}', `{tech}`" for tech in tech_columns]
    ) + ") as (der_type, nameplate_rating_kw)"

    unpivoted = df.selectExpr(
        "ProjectID",
        "ProjectCircuitID",
        "utility_id",
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id",
        stack_expr,
    )

    # Filter out NULL and zero capacity rows. Empty input → empty output (no .count() needed).
    unpivoted = unpivoted.filter(
        F.col("nameplate_rating_kw").isNotNull() & (F.col("nameplate_rating_kw") > 0)
    )

    # Composite der_id: ProjectID alone is not unique after unpivot.
    # A hybrid project produces one row per technology; key must include technology.
    unpivoted = unpivoted.withColumn(
        "der_id",
        F.concat(F.lit(f"{UTILITY1_ID}_"), F.col("ProjectID"), F.lit("_"), F.col("der_type")),
    )

    # FK resolution: NULL ProjectCircuitID → NULL feeder_id (preserved, not dropped).
    unpivoted = (
        unpivoted
        .withColumn("native_feeder_id_raw", F.col("ProjectCircuitID"))
        .withColumn(
            "feeder_id",
            F.when(
                F.col("ProjectCircuitID").isNotNull(),
                F.concat(F.lit(f"{UTILITY1_ID}_"), F.col("ProjectCircuitID")),
            ).otherwise(F.lit(None).cast(StringType())),
        )
    )

    # Always add planned_installation_date_raw so the schema is consistent whether
    # this is called for installed or planned DER. For installed, it's always NULL.
    # For planned, populate from the source column if it exists in the file.
    # Avoids the fragile "check df.columns at plan-construction time" pattern.
    if include_installation_date and UTILITY1_PLANNED_DATE_COL in df.columns:
        unpivoted = unpivoted.withColumn(
            "planned_installation_date_raw", F.col(UTILITY1_PLANNED_DATE_COL)
        )
    else:
        # Column absent or not requested: emit NULL so unionByName aligns cleanly
        unpivoted = unpivoted.withColumn(
            "planned_installation_date_raw", F.lit(None).cast(StringType())
        )

    return unpivoted


# ==============================================================================
# CANONICAL FIELD MAPPING (Single-Pass CASE WHEN)
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
    - hca_refresh_date (TIMESTAMP)
    - color_code (STRING)
    - shape_length (DOUBLE)
    """
    return df.select(
        F.col("utility_id"),
        F.col("feeder_id"),
        F.col("native_feeder_id"),
        
        # Cast to final types (both utilities use *_raw intermediate names)
        F.col("voltage_kv_raw").cast("double").alias("voltage_kv"),
        F.col("max_hosting_capacity_raw").cast("double").alias("max_hosting_capacity_mw"),
        F.col("min_hosting_capacity_raw").cast("double").alias("min_hosting_capacity_mw"),
        
        # hca_refresh_date: each utility uses a different timestamp format.
        # Utility 1: standard ISO-like format, no explicit pattern needed.
        # Utility 2: "yyyy/MM/dd HH:mm:ssXXX" handles timezone offset (e.g. +00:00).
        # "XXX" is the correct Java SimpleDateFormat pattern for ±HH:MM offset.
        # "+SS" is NOT a valid pattern and produces NULL for every row — do not use.
        F.when(
            F.col("utility_id") == UTILITY1_ID,
            F.to_timestamp(F.col("hca_refresh_date_raw"))
        ).when(
            F.col("utility_id") == UTILITY2_ID,
            F.to_timestamp(F.col("hca_refresh_date_raw"), "yyyy/MM/dd HH:mm:ssXXX")
        ).alias("hca_refresh_date"),
        
        F.col("color_code_raw").alias("color_code"),
        F.col("shape_length_raw").cast("double").alias("shape_length"),
        
        # Lineage
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id")
    )


def map_der_to_canonical(df: DataFrame, der_table_type: str) -> DataFrame:
    """Map utility-specific DER columns to canonical schema.
    
    Single-pass transformation.
    
    IMPORTANT: Expects intermediate schema where both utilities have:
    - der_id, feeder_id, native_feeder_id_raw, der_type, nameplate_rating_kw
    - (planned only): planned_installation_date_raw, interconnection_queue_id
    
    Args:
        df: DER DataFrame with intermediate schema
        der_table_type: 'installed' or 'planned'
    
    Canonical schema:
    - der_id (prefixed: utility1_ProjectID_TechType, utility2_DER_ID)
    - feeder_id (prefixed, NULL if unresolved)
    - native_feeder_id_raw (raw before normalization)
    - der_type (canonical names)
    - nameplate_rating_kw (DOUBLE)
    - der_status ('installed' or 'planned')
    - planned_installation_date (DATE, planned only)
    - interconnection_queue_id (STRING, utility 2 only)
    """
    # Always include planned columns in the select — even for installed DER they will
    # be NULL. This avoids the "column not found" AnalysisException that occurs when
    # withColumn tries to reference a column that was excluded from a prior select().
    # The caller (silver pipeline) emits these columns as NULL for installed DER;
    # Gold filters them out when building the installed-only view.
    canonical = df.select(
        F.col("utility_id"),
        F.col("der_id"),
        F.col("feeder_id"),
        F.col("native_feeder_id_raw"),
        F.col("der_type"),
        F.col("nameplate_rating_kw").cast("double").alias("nameplate_rating_kw"),
        F.lit(der_table_type).alias("der_status"),
        # Planned-specific columns: always selected, NULL for installed rows.
        # "planned_installation_date_raw" is emitted by unpivot_utility1_der (always)
        # and by the utility 2 silver select (always). Safe to reference here.
        F.col("planned_installation_date_raw"),
        # interconnection_queue_id: utility 2 only; utility 1 rows have NULL.
        F.col("interconnection_queue_id"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_date"),
        F.col("pipeline_update_id"),
    )

    if der_table_type == "planned":
        canonical = canonical.withColumn(
            "planned_installation_date",
            # Utility 1: standard date parse from whatever format InServiceDate uses.
            # Utility 2: "M/d/yyyy" e.g. "6/15/2025".
            F.when(
                (F.col("utility_id") == UTILITY1_ID)
                & F.col("planned_installation_date_raw").isNotNull(),
                F.to_date(F.col("planned_installation_date_raw")),
            ).when(
                F.col("utility_id") == UTILITY2_ID,
                F.to_date(F.col("planned_installation_date_raw"), "M/d/yyyy"),
            ).otherwise(F.lit(None))
        ).drop("planned_installation_date_raw")
    else:
        # installed: drop the raw column entirely, not relevant
        canonical = canonical.drop("planned_installation_date_raw")

    return canonical


def get_canonical_columns(dataset_type: str) -> List[str]:
    """Get list of canonical column names for a dataset type."""
    schemas = {
        "circuits": [
            "feeder_id", "utility_id", "native_feeder_id", "voltage_kv",
            "max_hosting_capacity_mw", "min_hosting_capacity_mw",
            "hca_refresh_date", "color_code", "shape_length",
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"
        ],
        "der_installed": [
            "der_id", "feeder_id", "utility_id", "native_feeder_id_raw",
            "der_type", "nameplate_rating_kw", "der_status",
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"
        ],
        "der_planned": [
            "der_id", "feeder_id", "utility_id", "native_feeder_id_raw",
            "der_type", "nameplate_rating_kw", "der_status",
            "planned_installation_date", "interconnection_queue_id",
            "ingestion_timestamp", "ingestion_date", "pipeline_update_id"
        ]
    }
    return schemas.get(dataset_type, [])
