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
- Excludes lineage columns from change detection (ingestion_timestamp, pipeline_update_id)

Sequence Strategy:
- Circuits: sequence_by=hca_refresh_date (business timestamp from source data)
- DER: sequence_by=ingestion_date (day-level granularity, prevents false versions from same-day reruns)
  * ingestion_timestamp (hour/minute) is excluded via except_column_list
  * ingestion_date is used for sequencing (multiple runs same day = no new version)

SCD2 Configuration Notes:
- except_column_list: Columns excluded from change detection (changes don't create new versions)
- track_history_column_list: Columns explicitly tracked for changes (changes DO create new versions)
- Any column not in either list: Changes are still detected (default behavior)
- except_column_list takes precedence: If a column is in both lists, it's excluded

Same-Day Limitation:
- DER tables use ingestion_date (day-level) as sequence_by
- Multiple pipeline runs on the same day with different data: Only the last run persists
- Intra-day corrections are silently overwritten (no version history)
- For sub-daily versioning, use ingestion_timestamp but accept false versions on reruns

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
    name="dev_iedr.gold.circuits_current",
    comment="SCD Type 2 history of feeder capacity changes - tracks max/min hosting capacity over time",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["utility_id", "feeder_id"]
)

dlt.apply_changes(
    target="dev_iedr.gold.circuits_current",
    source="dev_iedr.silver.circuits_standardized",
    keys=["utility_id", "feeder_id"],
    sequence_by="hca_refresh_date",
    stored_as_scd_type=2,
    except_column_list=[
        "ingestion_timestamp",
        "ingestion_date", 
        "pipeline_update_id"
    ],
    track_history_column_list=[
        "native_feeder_id",
        "voltage_kv",
        "max_hosting_capacity_mw",
        "min_hosting_capacity_mw",
        "color_code",
        "shape_length"
        # Note: utility_id is in keys, not track_history (keys are always tracked)
    ]
)


# ==============================================================================
# GOLD TABLE: DER INSTALLED CURRENT (SCD Type 2)
# ==============================================================================

dlt.create_target_table(
    name="dev_iedr.gold.der_installed_current",
    comment="SCD Type 2 history of installed DER projects - tracks installations and capacity changes",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["utility_id", "feeder_id"]
)

dlt.apply_changes(
    target="dev_iedr.gold.der_installed_current",
    source="dev_iedr.silver.der_installed_standardized",
    keys=["utility_id", "der_id", "der_type"],
    # utility_id: explicit multi-tenant key to prevent DER ID collisions across utilities
    # der_id: project identifier
    # der_type: required because utility 1 unpivot produces one row per technology per project
    #           Without der_type in keys, APPLY CHANGES keeps only one row per project and
    #           silently drops other technologies on every update
    sequence_by="ingestion_date",
    stored_as_scd_type=2,
    except_column_list=[
        "ingestion_timestamp",
        "pipeline_update_id"
        # Note: ingestion_date is NOT in except_column_list (it drives sequencing)
    ],
    track_history_column_list=[
        "feeder_id",
        "native_feeder_id_raw",
        "nameplate_rating_kw"
        # Note: utility_id is in keys, implicitly tracked (consistent with circuits_current)
    ]
)


# ==============================================================================
# GOLD TABLE: DER PLANNED CURRENT (SCD Type 2)
# ==============================================================================

dlt.create_target_table(
    name="dev_iedr.gold.der_planned_current",
    comment="SCD Type 2 history of planned DER projects - tracks queue status and planned installation dates",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["utility_id", "feeder_id"]
)

dlt.apply_changes(
    target="dev_iedr.gold.der_planned_current",
    source="dev_iedr.silver.der_planned_standardized",
    keys=["utility_id", "der_id", "der_type"],
    sequence_by="ingestion_date",
    stored_as_scd_type=2,
    except_column_list=[
        "ingestion_timestamp",
        "pipeline_update_id"
        # Note: ingestion_date is NOT in except_column_list (it drives sequencing)
    ],
    track_history_column_list=[
        "feeder_id",
        "native_feeder_id_raw",
        "nameplate_rating_kw",
        "planned_installation_date",
        "interconnection_queue_id"
        # Note: utility_id is in keys, implicitly tracked (consistent with circuits_current)
    ]
)
