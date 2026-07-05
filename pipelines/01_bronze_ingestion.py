"""Bronze Layer DLT Pipeline for NY IEDR Platform.

Ingests raw CSV files from utility landing zones using Auto Loader.
Creates 3 shared tables for all utilities (not per-utility tables):
- circuits_raw
- der_installed_raw
- der_planned_raw
- file_tracking (audit trail)

Key Features:
- All columns stored as STRING for schema fidelity
- Schema evolution enabled (addNewColumns mode)
- _rescued_data captures unexpected columns
- utility_id injected from directory path structure
- Lineage tracking via pipeline_update_id
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Import helper functions (adjust path if running locally)
try:
    from pipelines.utils.helpers import strip_utf8_bom, add_lineage_columns
except ImportError:
    # Fallback for DLT runtime (adds parent to path)
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.helpers import strip_utf8_bom, add_lineage_columns


# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Landing zone base path
LANDING_BASE = "/Volumes/dev_iedr/bronze/landing"

# Schema location - stored OUTSIDE landing directory to avoid conflicts
SCHEMA_BASE = "/Volumes/dev_iedr/bronze/metadata/schemas"

# Base Auto Loader options (schema location will be added per table)
BASE_AUTO_LOADER_OPTIONS = {
    "cloudFiles.format": "csv",
    "cloudFiles.inferColumnTypes": "false",  # Keep all as STRING
    "cloudFiles.schemaEvolutionMode": "addNewColumns",  # Append new columns automatically
    "rescuedDataColumn": "_rescued_data",  # Capture unexpected data
    "header": "true",
    "multiLine": "false",
    "escape": '"',
    "quote": '"'
}


def get_auto_loader_options(dataset_type: str) -> dict:
    """Get Auto Loader options with dataset-specific schema location.
    
    Each dataset needs its own schema location to avoid conflicts.
    """
    options = BASE_AUTO_LOADER_OPTIONS.copy()
    options["cloudFiles.schemaLocation"] = f"{SCHEMA_BASE}/{dataset_type}"
    return options


# ==============================================================================
# BRONZE TABLES: CIRCUITS
# ==============================================================================

@dlt.table(
    name="dev_iedr.bronze.circuits_raw",
    comment="Raw circuit/feeder data from all utilities (shared table)",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect("schema_drift_detected", "_rescued_data IS NULL OR _rescued_data = ''")
def circuits_raw():
    """Ingest raw circuit CSV files from all utilities.
    
    Path structure: /Volumes/.../landing/{utility_id}/circuits/*.csv
    utility_id is extracted from the directory path.
    """
    # Read from all utility subdirectories under landing/*/circuits/
    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**get_auto_loader_options("circuits"))
        .load(f"{LANDING_BASE}/*/circuits")
    )
    
    # Strip UTF-8 BOM from column names (utility 2 issue)
    df = strip_utf8_bom(df)
    
    # Add lineage columns (Bronze-specific: includes source_file, utility_id, file_signature)
    df = add_lineage_columns(
        df, 
        source_file_col="_metadata.file_path",
        include_file_signature=True
    )
    
    return df


# ==============================================================================
# BRONZE TABLES: DER INSTALLED
# ==============================================================================

@dlt.table(
    name="dev_iedr.bronze.der_installed_raw",
    comment="Raw installed DER data from all utilities (shared table)",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect("schema_drift_detected", "_rescued_data IS NULL OR _rescued_data = ''")
def der_installed_raw():
    """Ingest raw installed DER CSV files from all utilities.
    
    Path structure: /Volumes/.../landing/{utility_id}/der_installed/*.csv
    utility_id is extracted from the directory path.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**get_auto_loader_options("der_installed"))
        .load(f"{LANDING_BASE}/*/der_installed")
    )
    
    # Strip UTF-8 BOM from column names
    df = strip_utf8_bom(df)
    
    # Add lineage columns
    df = add_lineage_columns(
        df,
        source_file_col="_metadata.file_path",
        include_file_signature=True
    )
    
    return df


# ==============================================================================
# BRONZE TABLES: DER PLANNED
# ==============================================================================

@dlt.table(
    name="dev_iedr.bronze.der_planned_raw",
    comment="Raw planned DER data from all utilities (shared table)",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect("schema_drift_detected", "_rescued_data IS NULL OR _rescued_data = ''")
def der_planned_raw():
    """Ingest raw planned DER CSV files from all utilities.
    
    Path structure: /Volumes/.../landing/{utility_id}/der_planned/*.csv
    utility_id is extracted from the directory path.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**get_auto_loader_options("der_planned"))
        .load(f"{LANDING_BASE}/*/der_planned")
    )
    
    # Strip UTF-8 BOM from column names
    df = strip_utf8_bom(df)
    
    # Add lineage columns
    df = add_lineage_columns(
        df,
        source_file_col="_metadata.file_path",
        include_file_signature=True
    )
    
    return df


# ==============================================================================
# FILE TRACKING TABLE
# ==============================================================================

@dlt.table(
    name="dev_iedr.bronze.file_tracking",
    comment="Audit trail for all ingested files with idempotency tracking",
    table_properties={
        "quality": "bronze"
    }
)
def file_tracking():
    """Track all files processed across all bronze tables.
    
    Provides idempotency and audit trail - ONE ROW PER FILE (not per record):
    - file_name: Source file name
    - file_path: Full path
    - file_signature: Metadata-based signature (path + size + mtime)
    - utility_id: Extracted from path
    - dataset_type: circuits, der_installed, or der_planned
    - ingestion_timestamp: When processed
    - pipeline_update_id: DLT run identifier (format: run_YYYYMMDD_HHmmss_<hash>)
    - status: SUCCEEDED
    
    Note: Deduplicated by file_signature to ensure one row per unique file.
    Auto Loader prevents re-processing the same file in subsequent runs.
    """
    # Union file metadata from all three bronze tables
    circuits = dlt.read_stream("circuits_raw").select(
        F.col("source_file").alias("file_path"),
        F.element_at(F.split(F.col("source_file"), "/"), -1).alias("file_name"),
        F.col("file_signature"),
        F.col("utility_id"),
        F.lit("circuits").alias("dataset_type"),
        F.col("ingestion_timestamp"),
        F.col("pipeline_update_id"),
        F.lit("SUCCEEDED").alias("status")
    )
    
    der_installed = dlt.read_stream("der_installed_raw").select(
        F.col("source_file").alias("file_path"),
        F.element_at(F.split(F.col("source_file"), "/"), -1).alias("file_name"),
        F.col("file_signature"),
        F.col("utility_id"),
        F.lit("der_installed").alias("dataset_type"),
        F.col("ingestion_timestamp"),
        F.col("pipeline_update_id"),
        F.lit("SUCCEEDED").alias("status")
    )
    
    der_planned = dlt.read_stream("der_planned_raw").select(
        F.col("source_file").alias("file_path"),
        F.element_at(F.split(F.col("source_file"), "/"), -1).alias("file_name"),
        F.col("file_signature"),
        F.col("utility_id"),
        F.lit("der_planned").alias("dataset_type"),
        F.col("ingestion_timestamp"),
        F.col("pipeline_update_id"),
        F.lit("SUCCEEDED").alias("status")
    )
    
    # Union all streams and deduplicate by file_signature (one row per file)
    return (
        circuits.union(der_installed).union(der_planned)
        .dropDuplicates(["file_signature"])
    )
