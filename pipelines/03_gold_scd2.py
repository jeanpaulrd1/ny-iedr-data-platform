"""Gold Layer DLT Pipeline - SCD Type 2 Historical Tables.

Implements SCD Type 2 history tracking for circuits and DER using DLT's APPLY CHANGES INTO.
Reads full-refresh data from Silver layer and maintains historical records in Gold.

Architecture:
- Silver = Current snapshot (full-refresh on each run)
- Gold = Historical tracking (SCD Type 2 via APPLY CHANGES INTO)

Key Features:
- Tracks all changes to feeder capacity and DER installations over time
- Maintains __START_AT, __END_AT, __IS_CURRENT columns automatically
- Liquid clustering optimized for temporal and lookup queries
- No partitioning (history grows linearly, clustering handles efficiently)
- Excludes lineage columns from change detection (ingestion_timestamp, etc.)

Tables:
1. circuits_current: Feeder capacity history
2. der_installed_current: Installed DER state tracking
3. der_planned_current: Planned DER state tracking
"""

import dlt
from pyspark.sql import functions as F

# ==============================================================================
# GOLD TABLE: CIRCUITS CURRENT (SCD Type 2)
# ==============================================================================

dlt.create_target_table(
    name="circuits_current",
    comment="SCD Type 2 history of feeder capacity changes - tracks max/min hosting capacity over time",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["utility_id", "feeder_id"]
)

dlt.apply_changes(
    target="circuits_current",
    source="circuits_standardized",
    keys=["feeder_id"],
    sequence_by="hca_refresh_date",
    stored_as_scd_type=2,
    except_column_list=[
        "ingestion_timestamp",
        "ingestion_date", 
        "pipeline_update_id"
    ],
    track_history_column_list=[
        "utility_id",
        "native_feeder_id",
        "voltage_kv",
        "max_hosting_capacity_mw",
        "min_hosting_capacity_mw",
        "color_code",
        "shape_length"
    ]
)


# ==============================================================================
# GOLD TABLE: DER INSTALLED CURRENT (SCD Type 2)
# ==============================================================================

dlt.create_target_table(
    name="der_installed_current",
    comment="SCD Type 2 history of installed DER projects - tracks installations and capacity changes",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["utility_id", "der_id"]
)

dlt.apply_changes(
    target="der_installed_current",
    source="der_installed_standardized",
    keys=["der_id", "der_type"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2,
    except_column_list=[
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
    ],
    track_history_column_list=[
        "utility_id",
        "feeder_id",
        "native_feeder_id_raw",
        "nameplate_rating_kw",
        "der_status"
    ]
)


# ==============================================================================
# GOLD TABLE: DER PLANNED CURRENT (SCD Type 2)
# ==============================================================================

dlt.create_target_table(
    name="der_planned_current",
    comment="SCD Type 2 history of planned DER projects - tracks queue status and planned installation dates",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["utility_id", "der_id"]
)

dlt.apply_changes(
    target="der_planned_current",
    source="der_planned_standardized",
    keys=["der_id", "der_type"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2,
    except_column_list=[
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_update_id"
    ],
    track_history_column_list=[
        "utility_id",
        "feeder_id",
        "native_feeder_id_raw",
        "nameplate_rating_kw",
        "der_status",
        "planned_installation_date",
        "interconnection_queue_id"
    ]
)
