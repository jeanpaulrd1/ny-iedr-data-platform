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
    - color_code (STRING, raw utility-provided color code)
    - color_name (STRING, normalized uppercase color name extracted from color_code)
    - color_hex (STRING, standardized hex code for map rendering, e.g. #13AFED)
    - shape_length (DOUBLE)
    - grid_state_code (INTEGER, parsed from native_feeder_id, NULL for utility1)
    - grid_area_code (INTEGER, parsed from native_feeder_id, NULL for utility1)
    - grid_circuit_id (STRING, parsed from native_feeder_id, NULL for utility1)
    - geom_wkt (STRING, WKT geometry when provided by utility, NULL otherwise)
    
    Args:
        df: Union of utility 1 + utility 2 circuits with *_raw columns
        
    Returns:
        DataFrame with canonical schema, ready for SCD2 tracking
    """
    # Mapping for utility2 simple color names to standard hex codes
    utility2_color_map = F.create_map([
        F.lit("blue"), F.lit("#0070C0"),
        F.lit("brown"), F.lit("#953736"),
        F.lit("dark blue"), F.lit("#003366"),
        F.lit("green"), F.lit("#13B157"),
        F.lit("light blue"), F.lit("#13AFED"),
        F.lit("red"), F.lit("#F6130F"),
        F.lit("turquoise"), F.lit("#40E0D0"),
        F.lit("yellow"), F.lit("#FFF525"),
    ])
    
    return df.select(
        "feeder_id",
        "utility_id",
        "native_feeder_id",
        
        # Type cast numeric fields
        F.col("voltage_kv_raw").cast("double").alias("voltage_kv"),
        F.col("max_hosting_capacity_raw").cast("double").alias("max_hosting_capacity_mw"),
        F.col("min_hosting_capacity_raw").cast("double").alias("min_hosting_capacity_mw"),
        
        # Parse timestamp - utility-specific formats
        # utility1: "2022-06-30 00:00:00+00:00" (ISO with full timezone)
        # utility2: "2022/10/01 00:00:00+00" (slashes, short timezone)
        F.when(
            F.col("utility_id") == UTILITY1_ID,
            F.to_timestamp(F.col("hca_refresh_date_raw"), "yyyy-MM-dd HH:mm:ssXXX")
        ).when(
            F.col("utility_id") == UTILITY2_ID,
            F.to_timestamp(F.col("hca_refresh_date_raw"), "yyyy/MM/dd HH:mm:ssX")
        ).otherwise(None).alias("hca_refresh_date"),
        
        # Pass-through raw color code
        F.col("color_code_raw").alias("color_code"),
        
        # COLOR STANDARDIZATION: Extract hex code and color name
        # utility1 format: "0.00 TO 0.29 BROWN-953736" → hex: #953736, name: BROWN
        # utility2 format: "blue" → hex: #0070C0 (mapped), name: BLUE
        F.when(
            # utility1: Has dash and hex pattern
            F.col("color_code_raw").contains("-"),
            F.concat(F.lit("#"), F.regexp_extract(F.col("color_code_raw"), r"-([0-9A-Fa-f]{6})$", 1))
        ).otherwise(
            # utility2: Lookup in map, default to gray if not found
            F.coalesce(
                utility2_color_map[F.lower(F.trim(F.col("color_code_raw")))],
                F.lit("#808080")  # Gray fallback for unknown colors
            )
        ).alias("color_hex"),
        
        F.when(
            # utility1: Extract color name (word before the dash)
            F.col("color_code_raw").contains("-"),
            F.upper(F.regexp_extract(F.col("color_code_raw"), r"\s([A-Za-z\s]+)-[0-9A-Fa-f]{6}$", 1))
        ).otherwise(
            # utility2: Use the color_code as-is, uppercase
            F.upper(F.trim(F.col("color_code_raw")))
        ).alias("color_name"),
        
        # Shape length (already aggregated for utility1, native for utility2) - cast to double
        F.col("shape_length_raw").cast("double").alias("shape_length"),
        
        # Geospatial: Grid hierarchy parsing (for district-level map grouping)
        # Format: "36_13_81756" → state=36, area=13, circuit=81756
        # utility1 has no underscores ("1501601") → NULLs (graceful degradation)
        # utility2 has 3-part format → enables district clustering
        F.element_at(F.split(F.col("native_feeder_id"), "_"), 1).cast("integer").alias("grid_state_code"),
        F.element_at(F.split(F.col("native_feeder_id"), "_"), 2).cast("integer").alias("grid_area_code"),
        F.element_at(F.split(F.col("native_feeder_id"), "_"), 3).alias("grid_circuit_id"),
        
        # Geospatial: Geometry passthrough (NULL until utility provides Shape column)
        # Expected format: WKT LineString or MultiLineString
        # Map renderer uses this for precise feeder line drawing when available
        F.lit(None).cast("string").alias("geom_wkt"),
        
        # Lineage columns
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
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
    - grid_state_code (INTEGER, parsed from interconnection location, NULL for utility1)
    - grid_area_code (INTEGER, parsed from interconnection location, NULL for utility1)
    - grid_circuit_id (STRING, parsed from interconnection location, NULL for utility1)
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
            ),
        ).drop("planned_installation_date_raw")
    else:
        # installed: drop the raw column entirely, not relevant
        canonical = canonical.drop("planned_installation_date_raw")
 
    return canonical


def normalize_null_sentinels(df: DataFrame, string_columns: List[str]) -> DataFrame:
    """Normalize NULL sentinels ("NULL", "null", "", etc.) to SQL NULL.
    
    Args:
        df: DataFrame with potential NULL sentinels
        string_columns: List of STRING column names to normalize
        
    Returns:
        DataFrame with normalized NULLs
    """
    for col in string_columns:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(
                    (F.col(col).isNull()) |
                    (F.trim(F.col(col)) == "") |
                    (F.upper(F.trim(F.col(col))) == "NULL"),
                    None
                ).otherwise(F.col(col))
            )
    return df
