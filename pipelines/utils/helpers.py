"""Helper utilities for NY IEDR data platform.

Contains reusable functions for:
- Lineage column generation (ingestion timestamps, run IDs)
- Data normalization (UTF-8 BOM, NULL sentinels)
- Data quality expectation definitions

These utilities are designed to work across Bronze, Silver, and Gold layers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional, List
from datetime import datetime
import uuid


def add_lineage_columns(
    df: DataFrame,
    source_file_col: Optional[str] = None,
    include_file_signature: bool = False
) -> DataFrame:
    """Add standard lineage columns to a DataFrame.
    
    Injects metadata columns required for tracking data lineage across
    Bronze, Silver, and Gold layers. Uses Serverless-compatible approach
    for run ID generation.
    
    Args:
        df: Input DataFrame
        source_file_col: Column containing source file path (for Bronze layer only).
                        Defaults to None (Silver/Gold usage).
        include_file_signature: Whether to add file_signature column (Bronze layer only)
        
    Returns:
        DataFrame with added columns:
            - ingestion_timestamp: Record-level timestamp
            - ingestion_date: Date partition key
            - pipeline_update_id: Run identifier (format: run_YYYYMMDD_HHmmss_<hash>)
            - source_file: Source file path (if source_file_col provided)
            - utility_id: Extracted from file path (if source_file_col provided)
            - file_signature: Metadata-based signature (if include_file_signature=True, Bronze only)
            
    Example:
        >>> # Bronze layer usage
        >>> df_bronze = add_lineage_columns(
        ...     df, 
        ...     source_file_col="_metadata.file_path",
        ...     include_file_signature=True
        ... )
        >>> 
        >>> # Silver/Gold layer usage
        >>> df_silver = add_lineage_columns(df)
    """
    # Generate Serverless-compatible run ID ONCE for entire batch
    # Format: run_20240115_143022_a1b2c3d4
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hash_suffix = str(uuid.uuid4())[:8]
    run_id = f"run_{timestamp}_{hash_suffix}"
    
    # Build column dictionary
    columns = {
        "ingestion_timestamp": F.current_timestamp(),
        "ingestion_date": F.current_date(),
        "pipeline_update_id": F.lit(run_id)
    }
    
    # Add Bronze-specific columns if source file path is provided
    if source_file_col:
        columns["source_file"] = F.col(source_file_col)
        
        # Extract utility_id using native Spark functions (not UDF)
        # Path: /Volumes/.../landing/{utility_id}/{dataset_type}/*.csv
        # Split by '/', find 'landing', take next element
        path_parts = F.split(F.col(source_file_col), "/")
        landing_index = F.array_position(path_parts, F.lit("landing"))
        columns["utility_id"] = F.when(
            landing_index > 0,
            # Cast to int to match element_at signature (it expects INT not BIGINT)
            F.element_at(path_parts, (landing_index + 1).cast("int"))
        ).otherwise(F.lit("unknown"))
        
        # Add file signature for Bronze layer idempotency
        # Note: This is NOT a content hash - it's a metadata-based signature
        # combining file path, size, and modification time
        if include_file_signature:
            columns["file_signature"] = F.sha2(F.concat(
                F.col(source_file_col),
                F.coalesce(F.col("_metadata.file_size").cast("string"), F.lit("0")),
                F.coalesce(F.col("_metadata.file_modification_time").cast("string"), F.lit(""))
            ), 256)
    
    return df.withColumns(columns)


def normalize_null_sentinels(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
    """Normalize string null sentinels to true SQL NULL.
    
    Converts common null-representing strings to true NULL values.
    Handles: 'NULL', 'null', 'N/A', 'n/a', 'NA', empty string, and whitespace-only strings.
    Trims all whitespace (spaces, tabs, newlines, etc.) before checking.
    
    Args:
        df: Input DataFrame
        columns: List of column names to normalize. If None, auto-detects all string columns.
        
    Returns:
        DataFrame with normalized NULL values
        
    Example:
        >>> # Explicit columns
        >>> df = normalize_null_sentinels(df, ['feeder_id', 'project_circuit_id'])
        >>> 
        >>> # Auto-detect string columns (recommended for DLT)
        >>> df = normalize_null_sentinels(df)
        
    Note:
        Auto-detection triggers one schema analysis per call, which is acceptable
        since it's done once per DLT function invocation (not in a loop).
    """
    # Auto-detect string columns if not provided
    if columns is None:
        columns = [col_name for col_name, dtype in df.dtypes if dtype == "string"]
    
    # Return early if no columns to process
    if not columns:
        return df
    
    null_sentinels = ['NULL', 'null', 'N/A', 'n/a', 'NA', '']
    
    # Build all column transformations at once for performance
    transformations = {}
    for col_name in columns:
        # Trim ALL whitespace (spaces, tabs, newlines, etc.), then check if it's a null sentinel
        # Use regexp_replace instead of trim() because trim() only removes ASCII spaces
        trimmed_col = F.regexp_replace(F.col(col_name), r"^\s+|\s+$", "")
        transformations[col_name] = F.when(
            trimmed_col.isin(null_sentinels), None
        ).otherwise(trimmed_col)  # Return trimmed value, not original
    
    df = df.withColumns(transformations)
    
    return df


def strip_utf8_bom(df: DataFrame) -> DataFrame:
    """Strip UTF-8 BOM (\\ufeff) from all column names.
    
    Auto Loader with UTF-8 BOM-encoded files can produce column names like:
        \ufeffCircuits_Phase3_CIRCUIT
    
    This function removes the BOM character from all column names.
    
    Args:
        df: Input DataFrame with potentially BOM-prefixed column names
        
    Returns:
        DataFrame with cleaned column names
        
    Example:
        >>> # Auto Loader output might have BOM in first column
        >>> df = spark.read.format("csv").option("header", "true").load(path)
        >>> df = strip_utf8_bom(df)
    """
    # Check if any column name starts with BOM
    bom_char = "\ufeff"
    renamed_columns = {}
    
    for col_name in df.columns:
        if col_name.startswith(bom_char):
            # Remove BOM prefix
            clean_name = col_name.replace(bom_char, "")
            renamed_columns[col_name] = clean_name
    
    # Apply renames if needed
    if renamed_columns:
        for old_name, new_name in renamed_columns.items():
            df = df.withColumnRenamed(old_name, new_name)
    
    return df

def clear_index_artifact_from_rescued_data(df: DataFrame) -> DataFrame:
    """Clear _rescued_data if it contains ONLY structural artifacts, with case-variant column extraction.
    
    TWO-STEP APPROACH:
    
    STEP 1: Extract Business Columns with Case Variations
    Some utilities export the same business column with different casing 
    (e.g., utility1: 'Shape_Length', utility2: 'shape_length'). Auto Loader's 
    case-sensitive schema matching sends the lowercase variant to _rescued_data.
    
    This step extracts known case-variant columns from _rescued_data and populates
    the canonical (target) column, then removes them from _rescued_data.
    
    STEP 2: Clear Pure Artifacts
    After extraction, if _rescued_data contains ONLY known artifacts 
    (_c0, _file_path), clear it to NULL. This prevents false positive alerts
    on structural artifacts while preserving real schema drift.
    
    Known artifacts (cleared in Step 2):
    - _c0: Index column from empty-named first column (numeric sequential values)
    - _file_path: Duplicate file path metadata (already in source_file column)
    
    Known case variants (extracted in Step 1):
    - shape_length (lowercase) → Shape_Length (Title Case)
    
    Pattern examples:
    
    Before: {"shape_length":"1.27","_file_path":"..."}
    Step 1: Extract shape_length → Shape_Length column, _rescued_data = {"_file_path":"..."}
    Step 2: Only _file_path remains (pure artifact) → _rescued_data = NULL
    
    Before: {"_c0":"123","_file_path":"..."}
    Step 1: No business columns to extract, _rescued_data unchanged
    Step 2: Only artifacts present → _rescued_data = NULL
    
    Before: {"new_column":"value","_file_path":"..."}
    Step 1: new_column is not a known variant, _rescued_data unchanged
    Step 2: Has unknown business column → _rescued_data preserved (real drift)
    
    Args:
        df: DataFrame with _rescued_data column
        
    Returns:
        DataFrame with:
        - Case-variant columns extracted and populated in target columns
        - _rescued_data cleared if only artifacts remain
        - _index_col_dropped flag for tracking
        
    Example:
        >>> # utility2: lowercase shape_length
        >>> # Before: Shape_Length=NULL, _rescued_data='{"shape_length":"1.27","_file_path":"..."}'
        >>> df = clear_index_artifact_from_rescued_data(df)
        >>> # After: Shape_Length="1.27", _rescued_data=NULL
    """
    # Check if _rescued_data column exists
    if "_rescued_data" not in df.columns:
        return df.withColumn("_index_col_dropped", F.lit(False))
    
    # =========================================================================
    # STEP 1: Extract Business Columns with Case Variations
    # =========================================================================
    
    # Define case-variant mappings: (rescued_key, target_column)
    # Add more mappings here as new case variations are discovered
    case_variant_mappings = [
        ("shape_length", "Shape_Length"),  # utility2 lowercase → utility1 Title Case
    ]
    
    # Extract each known case variant from _rescued_data
    for rescued_key, target_column in case_variant_mappings:
        # Check if target column exists in schema
        if target_column not in df.columns:
            continue
        
        # Extract value from _rescued_data JSON using get_json_object
        # Example: get_json_object('{"shape_length":"1.27"}', '$.shape_length') = "1.27"
        extracted_value = F.get_json_object(F.col("_rescued_data"), f"$.{rescued_key}")
        
        # Populate target column if it's NULL and extracted_value exists
        # This handles: utility1 has Shape_Length populated, utility2 has it in _rescued_data
        df = df.withColumn(
            target_column,
            F.coalesce(F.col(target_column), extracted_value)
        )
        
        # Remove the extracted key from _rescued_data JSON
        # Strategy: Parse JSON, check if key exists, rebuild without it
        # Use regexp_replace to remove the key-value pair from JSON
        # Pattern: Remove '"key":"value",' or ',"key":"value"' or '"key":"value"'
        
        # Build regex patterns to match the key in different positions
        # Pattern 1: "key":"value", (key at start or middle, has comma after)
        pattern1 = F.concat(F.lit('"'), F.lit(rescued_key), F.lit(r'":"[^"]*",'))
        # Pattern 2: ,"key":"value" (key at end or middle, has comma before)
        pattern2 = F.concat(F.lit(r',"'), F.lit(rescued_key), F.lit(r'":"[^"]*"'))
        # Pattern 3: "key":"value" (only key in object)
        pattern3 = F.concat(F.lit('"'), F.lit(rescued_key), F.lit(r'":"[^"]*"'))
        
        # Apply replacements in sequence
        df = df.withColumn(
            "_rescued_data",
            F.when(
                F.col("_rescued_data").contains(f'"{rescued_key}"'),
                # Try pattern 1 first (key with trailing comma)
                F.regexp_replace(
                    # Then try pattern 2 (key with leading comma)  
                    F.regexp_replace(
                        # Finally pattern 3 (only key - will leave empty {})
                        F.regexp_replace(
                            F.col("_rescued_data"),
                            pattern3,
                            ""
                        ),
                        pattern2,
                        ""
                    ),
                    pattern1,
                    ""
                )
            ).otherwise(F.col("_rescued_data"))
        )
    
    # Clean up empty JSON objects {} that may result from removing all keys
    df = df.withColumn(
        "_rescued_data",
        F.when(
            F.col("_rescued_data").rlike(r'^\s*\{\s*\}\s*$'),
            F.lit(None).cast("string")
        ).otherwise(F.col("_rescued_data"))
    )
    
    # =========================================================================
    # STEP 2: Clear Pure Artifacts
    # =========================================================================
    
    # After Step 1, _rescued_data should only contain:
    # - Known artifacts (_c0, _file_path) → Clear these
    # - Unknown columns (real drift) → Preserve these
    
    has_c0 = F.col("_rescued_data").contains('"_c0"')
    has_file_path = F.col("_rescued_data").contains('"_file_path"')
    is_small = F.length(F.col("_rescued_data")) < 200
    
    # Check for any other keys besides _c0 and _file_path
    # Simple heuristic: if it contains other quoted keys, it has business columns
    # We already extracted known variants in Step 1, so anything else is truly new/unknown
    
    # Count JSON keys (rough heuristic using comma count)
    # This is imperfect but good enough for artifact detection
    # More sophisticated: use from_json, but that requires schema
    
    # Simpler: Check if after removing _c0 and _file_path patterns, anything remains
    # Use regex to remove both known artifacts and check if substantive content remains
    
    temp_cleaned = F.regexp_replace(
        F.regexp_replace(
            F.coalesce(F.col("_rescued_data"), F.lit("")),
            r'"_c0":"[^"]*"', ""
        ),
        r'"_file_path":"[^"]*"', ""
    )
    # Remove leftover JSON punctuation
    temp_cleaned = F.regexp_replace(temp_cleaned, r'[\{\}:,\s]', '')
    
    # If temp_cleaned is empty (or just quotes), then only artifacts were present
    has_other_content = F.length(temp_cleaned) > 2
    
    # Pure artifact if: has known artifacts AND small AND no other content
    is_pure_artifact = (
        (has_c0 | has_file_path) &
        is_small &
        ~has_other_content
    )
    
    # Set tracking flag
    df = df.withColumn("_index_col_dropped", is_pure_artifact)
    
    # Clear _rescued_data for pure artifacts
    df = df.withColumn(
        "_rescued_data",
        F.when(is_pure_artifact, F.lit(None).cast("string"))
         .otherwise(F.col("_rescued_data"))
    )
    
    return df
