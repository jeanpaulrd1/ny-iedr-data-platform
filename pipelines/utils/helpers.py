"""Helper utilities for NY IEDR data pipeline.

Provides lineage tracking, file hashing, and metadata injection functions
for use across Bronze, Silver, and Gold layers.
"""

import uuid
from datetime import datetime
from typing import Optional, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_lineage_columns(
    df: DataFrame,
    source_file_col: str = "_metadata.file_path",
    include_file_signature: bool = False
) -> DataFrame:
    """Add standard lineage columns to a DataFrame.
    
    Injects metadata columns required for tracking data lineage across
    Bronze, Silver, and Gold layers. Uses Serverless-compatible approach
    for run ID generation.
    
    Args:
        df: Input DataFrame
        source_file_col: Column containing source file path (for Bronze layer only)
        include_file_signature: Whether to add file_signature column (Bronze layer only)
        
    Returns:
        DataFrame with added columns:
            - ingestion_timestamp: Record-level timestamp
            - ingestion_date: Date partition key (Bronze only)
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
            F.element_at(path_parts, landing_index + 1)
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
    Handles: 'NULL', 'null', 'N/A', 'NA', empty string.
    
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
    
    null_sentinels = ['NULL', 'null', 'N/A', 'NA', '']
    
    # Build all column transformations at once for performance
    transformations = {}
    for col_name in columns:
        transformations[col_name] = F.when(
            F.col(col_name).isin(null_sentinels), None
        ).otherwise(F.col(col_name))
    
    df = df.withColumns(transformations)
    
    return df


def strip_utf8_bom(df: DataFrame) -> DataFrame:
    """Strip UTF-8 BOM (\\ufeff) from column names.
    
    Utility 2's DER files have UTF-8 BOM on the first header column,
    breaking schema matching. This removes it.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with cleaned column names
        
    Example:
        >>> df = strip_utf8_bom(df)
    """
    # Check if any column has BOM
    bom_cols = [c for c in df.columns if c.startswith('\ufeff')]
    
    if not bom_cols:
        return df
    
    # Rename columns to remove BOM
    for old_name in bom_cols:
        new_name = old_name.replace('\ufeff', '')
        df = df.withColumnRenamed(old_name, new_name)
    
    return df


def extract_utility_id_from_path(path: str) -> str:
    """Extract utility ID from file path.
    
    Path format: /Volumes/.../landing/{utility_id}/{dataset_type}/*.csv
    
    Args:
        path: Full file path
        
    Returns:
        Extracted utility_id or 'unknown'
        
    Example:
        >>> extract_utility_id_from_path("/Volumes/main/data/landing/utility1/circuits/file.csv")
        'utility1'
    """
    parts = path.split('/')
    try:
        landing_idx = parts.index('landing')
        return parts[landing_idx + 1]
    except (ValueError, IndexError):
        return 'unknown'
