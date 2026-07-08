# NY IEDR Data Platform - Architecture

## 🏗️ Medallion Architecture Overview

### Bronze Layer (dev_iedr.bronze)
**Purpose**: Ingest raw CSV files with minimal transformation

**Tables:**
* `dev_iedr.bronze.circuits_raw` - Circuit/feeder infrastructure data
* `dev_iedr.bronze.der_installed_raw` - Installed DER projects  
* `dev_iedr.bronze.der_planned_raw` - Planned DER projects
* `dev_iedr.bronze.file_tracking` - File ingestion audit trail with idempotency

**Key Features:**
* Auto Loader (cloudFiles) for incremental ingestion
* All columns stored as **STRING** for schema fidelity and evolution
* **No partitioning or clustering** - Raw storage optimized for append-only streaming
  - Computed columns (`utility_id`) cannot be used for clustering at ingestion
  - Optimization deferred to Silver/Gold layers where columns are materialized
* Schema evolution enabled (`addNewColumns` mode)
* **Artifact Handling & Case Sensitivity Resolution:**
  - **Two-step extraction and clearing** via `clear_index_artifact_from_rescued_data()`
  - **Step 1**: Extract business columns with case variations from `_rescued_data`
    - Example: utility2's `shape_length` (lowercase) → `Shape_Length` (Title Case)
    - Auto Loader's case-sensitive matching routes lowercase variants to `_rescued_data`
    - Function extracts known case variants, populates canonical columns, removes from `_rescued_data`
  - **Step 2**: Clear pure structural artifacts
    - `_c0`: Index column from empty-named CSV first column (sequential numeric values)
    - `_file_path`: Duplicate metadata (already in `source_file` column)
    - Cleared to NULL if only artifacts remain after extraction
  - **Tracking**: `_index_col_dropped` flag indicates artifact processing occurred
  - **Result**: All 66,448 records pass `schema_drift_detected` expectation
    - utility1: 64,539 records (pure artifacts cleared)
    - utility2: 1,909 records (shape_length extracted, artifacts cleared)
* File tracking with `file_signature` and `pipeline_update_id` for idempotency
  - **file_signature**: MD5 hash of (file_path + file_size + modification_timestamp)
  - Purpose: Prevent duplicate ingestion of same file across pipeline runs
  - Computed in Bronze layer, stored in `file_tracking` table for idempotency checks
* Change data feed enabled for downstream CDC processing

**Data Volumes (as of 2026-07-05):**
* **utility1**: 64,539 circuit/segment records
* **utility2**: 1,909 circuit/feeder records
* **Total DER Installed**: 39,657 projects (utility1: 14,120, utility2: 25,537)
* **Total DER Planned**: 32,689 projects (utility1: 1,733, utility2: 30,956)

---

### Silver Layer (dev_iedr.silver)
**Purpose**: Standardize schemas, enforce quality, create common data model
**N-Utility Registry Pattern:**
* **Utility Registry** (`pipelines/utils/utility_registry.py`): Configuration-driven utility onboarding
  - Each utility defines 3 transformer functions (circuits, der_installed, der_planned)
  - Register new utilities via `UTILITY_REGISTRY` dict without changing pipeline code
  - `get_registered_utilities()` returns all active utilities dynamically
  - **To onboard utility3**: Write 3 transformer functions, add to registry, done
  - **Each transformer function** uses single-pass CASE WHEN logic to map utility-specific fields to canonical schema
* **Dynamic Processing**: Silver pipeline loops over registered utilities automatically
  - No hardcoded utility IDs in pipeline code
  - Scalable to N utilities without architectural changes

**Tables:**
* `dev_iedr.silver.circuits_standardized` - Feeder-level circuits (full-refresh snapshots)
* `dev_iedr.silver.der_installed_standardized` - Normalized DER installations (full-refresh)
* `dev_iedr.silver.der_planned_standardized` - Normalized DER planning queue (full-refresh)
* `dev_iedr.silver.data_quality_metrics_silver` - Data quality metrics with freshness monitoring
  - **Metrics tracked**: Record counts, null keys, negative capacities, unresolved feeders
  - **Freshness monitoring** (circuits only): `last_refresh_date`, `days_since_refresh`
  - **Streaming pattern**: Appends new metrics on each run (skipChangeCommits for full-refresh sources)
  - **Volume baseline**: Enables historical trend analysis and anomaly detection

**Transformations:**
* **Utility 1**: Aggregate segment-level → feeder-level circuits (MODE capacity, not SUM)
* **Utility 1**: Unpivot wide DER format (14 one-hot tech columns) → narrow (der_id, der_type, capacity)
* **Utility 1**: Add `interconnection_queue_id` column (as NULL) to align with utility2 schema
* **All Utilities**: Normalize null sentinels ("NULL", "null", "" → SQL NULL)
* **All Utilities**: Map inconsistent field names to unified schema via two-step transformation:
  1. **Transformer functions** return intermediate schema with `_raw` suffixes
  2. **Canonical mapping layer** (`map_circuits_to_canonical`, `map_der_to_canonical`) produces final schema
* **All Utilities**: Standardize DER technology types to canonical names
* **All Utilities**: Standardize color codes to unified hex/name format (color_hex, color_name)

**Key Features:**
* **Full-Refresh**: Tables rebuilt on each pipeline run from current Bronze data
* **No partitioning**: Pure liquid clustering (partitioning removed to avoid conflicts)
* **No clustering at Silver**: Deferred to Gold layer for query-specific optimization
* Data quality expectations enforced (`@dlt.expect_or_drop` on circuits, DER unresolved feeders pass through)
* Unresolved DER (feeder_id IS NULL) preserved for data_quality_metrics tracking
* **Composite DER Keys**: `der_id` includes technology type (e.g., `utility1_proj1_SolarPV`) for hybrid projects
* **Two-layer transformation**: Intermediate → Canonical mapping preserves separation of concerns
  - Transformers focus on utility-specific extraction logic
  - Canonical mappers handle common type casting, date parsing, color standardization
  - `allowMissingColumns=True` during union (intermediate schemas may vary)
* **Code simplification (2026-07-08)**: Reduced silver transformations from ~450 to ~306 lines
  - Removed verbose docstrings (moved details to ARCHITECTURE.md)
  - Simplified import patterns (removed try/except fallbacks)
  - Cleaner function signatures and variable names

**Data Quality Issues Tracked:**
* **DER Installed**: 8 unresolved feeders (utility1), 202 unresolved feeders (utility2)
* **DER Planned**: 345 unresolved feeders (utility1), 574 unresolved feeders (utility2)
* **Circuits**: 269 feeders with capacity data (utility1), no data quality issues

---

### Gold Layer (dev_iedr.gold) ✅ COMPLETE
**Purpose**: API-optimized aggregates and SCD Type 2 history

**Tables:**
* `dev_iedr.gold.circuits_current` - SCD Type 2 for feeder capacity history
* `dev_iedr.gold.der_installed_current` - SCD Type 2 for DER state tracking
* `dev_iedr.gold.der_planned_current` - SCD Type 2 for DER planning queue history
* `dev_iedr.gold.feeders_with_capacity` - Pre-aggregated: feeders with available capacity (current view)
* `dev_iedr.gold.feeder_der_summary` - Pre-aggregated: all DER per feeder (current view)

**Key Features:**
* **SCD Type 2**: Track capacity changes over time via DLT's Auto CDC (`dp.create_auto_cdc_flow`)
  - `circuits_current`: KEY = `(feeder_id, utility_id)`, SEQUENCE BY `hca_refresh_date`
  - `der_installed_current`: KEY = `(der_id, utility_id)`, SEQUENCE BY `ingestion_date`
  - `der_planned_current`: KEY = `(der_id, utility_id)`, SEQUENCE BY `ingestion_date`
  - **Current records identified by `__END_AT IS NULL`** (NOT `__IS_CURRENT` column)
  - Temporal columns: `__START_AT`, `__END_AT` (NULL = current)
  - **Color tracking**: `color_code`, `color_hex`, `color_name` in `track_history_column_list`
    (Standardized in Silver, tracked for capacity-color correlation analysis)
* **API-Optimized Current Views**: No SCD2 columns, simple queries for API
* **Liquid Clustering** optimized for query patterns:
  - `circuits_current`: `CLUSTER BY [utility_id, feeder_id]`
  - `der_installed_current`: `CLUSTER BY [utility_id, feeder_id]`
  - `der_planned_current`: `CLUSTER BY [utility_id, feeder_id]`
  - `feeders_with_capacity`: `CLUSTER BY [utility_id, feeder_id]`
  - `feeder_der_summary`: `CLUSTER BY [feeder_id, utility_id]`
* **Multi-tenant safe**: All SCD2 keys include `utility_id` to prevent cross-utility collisions
* **Runtime-resilient helpers**: `filter_current()` handles case-insensitive column matching

---


### 8. Artifact Handling & Case Sensitivity

**Problem**: Multi-utility CSV exports contain structural artifacts and case variations:
* **Index columns**: utility1's exports have unnamed first column (auto-numbered by pandas)
  - Auto Loader names it `_c0`, routes to `_rescued_data` (no schema match)
* **Duplicate metadata**: `_file_path` in `_rescued_data` duplicates `source_file` column
* **Case sensitivity**: Same business column with different casing across utilities
  - utility1: `Shape_Length` (Title Case)
  - utility2: `shape_length` (lowercase)
  - Auto Loader's case-sensitive matching treats these as different columns

**Challenge**: Auto Loader processes schema before transformation code runs:
1. Auto Loader reads CSV → Matches columns to cached schema (case-sensitive)
2. Unmatched columns → routed to `_rescued_data`
3. **Then** transformation functions run (too late to intercept)

**Solution**: Two-step extraction and clearing in `clear_index_artifact_from_rescued_data()`:

**Step 1 - Extract Business Columns:**
* Parse `_rescued_data` JSON for known case-variant columns
* Extract values using `get_json_object()`
* Populate canonical column (e.g., `shape_length` → `Shape_Length`)
* Remove extracted keys from `_rescued_data` using `regexp_replace()`

**Step 2 - Clear Pure Artifacts:**
* After extraction, check what remains in `_rescued_data`
* If only `_c0` and/or `_file_path` → Clear to NULL (pure artifacts)
* If other unknown keys remain → Preserve (real schema drift)
* Set `_index_col_dropped` flag for operational tracking

**Case Variant Mappings** (maintained in helper function):
```python
case_variant_mappings = [
    ("shape_length", "Shape_Length"),  # utility2 lowercase → utility1 Title Case
    # Add more as discovered
]
```

**Example Flow:**

Before:
```
utility1: Shape_Length = "0.00078"
          _rescued_data = {"_c0":"61488","_file_path":"..."}

utility2: Shape_Length = NULL
          _rescued_data = {"shape_length":"1.277","_file_path":"..."}
```

After Step 1 (extraction):
```
utility1: Shape_Length = "0.00078"
          _rescued_data = {"_c0":"61488","_file_path":"..."}  (unchanged)

utility2: Shape_Length = "1.277"  (extracted!)
          _rescued_data = {"_file_path":"..."}  (shape_length removed)
```

After Step 2 (clearing):
```
utility1: Shape_Length = "0.00078"
          _rescued_data = NULL  (only artifacts remained)
          _index_col_dropped = True

utility2: Shape_Length = "1.277"
          _rescued_data = NULL  (only _file_path remained)
          _index_col_dropped = True
```

**Result:**
* All 66,448 records pass `schema_drift_detected` expectation
* False positives eliminated (artifacts cleared)
* Real schema drift still detected (unknown columns preserved)
* Scalable: Add new case variants by extending `case_variant_mappings` list

**Why not normalize column names before Auto Loader?**
* Auto Loader reads files and applies schema cache before transformation code runs
* By the time `normalize_column_names()` would execute, routing already happened
* Extraction approach works with Auto Loader's existing schema cache
* Non-destructive: No schema cache clearing or full re-ingestion required

---
## 🔑 Key Design Decisions

### 1. No Partitioning or Clustering at Bronze
**Decision**: Bronze tables have no partitioning or clustering

**Rationale:**
* **Computed columns limitation**: `utility_id` is extracted from file path, not in source CSV schema
* Delta Liquid Clustering requires columns to have statistics in the source schema
* **Append-only streaming**: Bronze is write-optimized, not query-optimized
* **Optimization deferred**: Silver and Gold layers apply clustering where columns are materialized
* **Simplicity**: Avoid partition/cluster conflicts during schema evolution

**Result**: Bronze is pure raw storage; optimization happens downstream

---

### 2. Silver = Full-Refresh, Gold = History
* **Silver**: Rebuilt on each run from current Bronze data (monthly utility snapshots)
  - Simpler logic, no SCD2 complexity
  - Matches source pattern (utilities deliver full monthly snapshots, not CDC)
* **Gold**: SCD Type 2 tracks changes over time
  - Reads Silver's current data
  - Maintains historical records for trend analysis
  - API views query Gold current records using `__END_AT IS NULL`

### 3. N-Utility Registry Pattern
**Decision**: Configuration-driven utility onboarding via registry pattern

**Rationale:**
* **Scalability**: Adding utility3 requires zero changes to pipeline code
* **Separation of concerns**: Utility-specific logic lives in registry, not pipeline
* **Maintainability**: Each utility's transformations are self-contained functions
* **Testing**: Transformer functions can be unit tested in isolation

**Implementation:**
```python
# pipelines/utils/utility_registry.py
UTILITY_REGISTRY = {
    1: UtilityConfig(
        circuits_transformer=transform_utility1_circuits,
        der_installed_transformer=transform_utility1_der_installed,
        der_planned_transformer=transform_utility1_der_planned
    ),
    2: UtilityConfig(...)  # Add more utilities here
}
```

**How Transformations Work:**
Each transformer function in the registry contains single-pass CASE WHEN logic to map utility-specific column names to the canonical schema:
```python
def transform_utility1_circuits(df):
    return df.select(
        F.col("NYHCPV_csv_NFEEDER").alias("native_feeder_id"),
        F.when(F.col("NYHCPV_csv_FMAXHC").isNotNull(), 
               F.col("NYHCPV_csv_FMAXHC"))
         .otherwise(F.col("feeder_max_hc")).alias("max_hosting_capacity_mw"),
        # ... more CASE WHEN mappings
    )
```

### 4. Color Code Standardization
**Decision**: Standardize color codes in Silver layer for uniform map rendering

**Problem**: Utilities use different color code formats:
* **utility1**: Compound format with embedded hex and name (e.g., "0.00 TO 0.29 BROWN-953736")
* **utility2**: Simple color names only (e.g., "blue", "red", "dark blue")

**Solution**: Silver layer generates standardized columns via `standardize_colors()`:
* **color_hex**: Standard 6-digit hex code (e.g., #F96A0D, #13AFED)
* **color_name**: Uppercase canonical name (e.g., ORANGE, SKYBLUE)

**Implementation**:
```python
# pipelines/utils/schema_normalization.py
def map_circuits_to_canonical(df, utility_id):
    # For utility1: Extract hex from "BROWN-953736" format
    # For utility2: Map names to standard hex codes
    df = df.withColumn("color_hex", 
        when(col("color_code").contains("-"), ...)  # Extract
        .otherwise(...))  # Map from dict
    df = df.withColumn("color_name",
        when(col("color_code").contains("-"), ...)  # Extract
        .otherwise(upper(col("color_code"))))  # Normalize
    return df
```

**Rationale**:
* **Single source of truth**: Color logic in one place (Silver layer)
* **Automatic propagation**: All downstream Gold views inherit standardized colors
* **Maintainability**: Future color mappings only need Silver layer updates
* **Map rendering ready**: Frontend receives consistent hex codes without per-utility logic

**Result**:
* Both utilities now use same hex codes (e.g., BROWN → #953736, BLUE → #0070C0)
* Gold SCD2 tracks color changes over time (color_hex, color_name in track_history_column_list)
* API views expose standardized colors for map rendering and dashboards

---

### 5. SCD2 Key Design
**Multi-tenant safety**: All SCD2 keys include `utility_id`
* Prevents collisions when different utilities use the same native IDs
* Example: `utility1_1000` vs `utility2_1000` are distinct DER projects
* Keys: `(feeder_id, utility_id)` for circuits, `(der_id, utility_id)` for DER

**Why composite keys?**
* Native IDs (e.g., `ProjectID`) are not globally unique across utilities
* `utility_id` prefix in `feeder_id`/`der_id` provides uniqueness
* SCD2 tracking requires utility_id in KEYS to prevent state corruption

### 6. Lineage & Run Tracking
Every table includes:
* `pipeline_update_id` - **Primary run identifier** (format: `run_YYYYMMDD_HHmmss_<hash>`)
* `ingestion_timestamp` - Record-level timestamp
* `ingestion_date` - Date column for time-based filtering
* `source_file` - Source file path (Bronze only)
* `file_signature` - File-level deduplication (Bronze only)
  - MD5 hash of (file_path + file_size + modification_timestamp)
  - Prevents duplicate ingestion of same file across pipeline runs
  - Stored in `file_tracking` table for idempotency audit trail

**Why `pipeline_update_id`?**
* Available via `spark.conf.get("pipelines.update_id")`
* Links records → runs → DLT event logs → source files
* Enables lineage, debugging, rollback, and idempotency

### 7. Schema Evolution Strategy
* **Bronze**: All STRING columns, preserve raw source fidelity
* Auto Loader schema inference with evolution enabled (`addNewColumns` mode)
* **Silver**: Mapping layer handles heterogeneous schemas across utilities
* **Strict schema alignment**: `unionByName(allowMissingColumns=False)` catches drift

### 8. Heterogeneous Source Handling
**Utility 1** (Wide, Segment-Level):
* Circuits: Segment rows → aggregated to feeder (MODE capacity, not SUM)
* DER: Wide format (14 one-hot tech columns: SolarPV, Wind, etc.) → unpivoted to narrow
* DER: Add `interconnection_queue_id` as NULL to match utility2's 11-column schema

**Utility 2** (Narrow, Feeder-Level):
* Already feeder-level, narrow DER format
* Direct mapping to Silver schema

**Result**: Unified Silver schema across all utilities

---

## 📊 Data Flow

```
Bronze Layer (Raw, STRING columns, no partitioning/clustering)
         ↓
  [N-Utility Registry → Dynamic Transformations per Utility]
         ↓
  [Standardization, Segment→Feeder, Unpivot, Color Standardization, Quality Checks]
         ↓
Silver Layer (Full-Refresh, Feeder-level, Normalized DER types, no clustering)
         ↓ (skipChangeCommits for streaming reads)
  [SCD Type 2, Aggregation, Business Logic, Liquid Clustering]
         ↓
  [SCD Type 2, Aggregation, Business Logic, Liquid Clustering]
         ↓
Gold Layer (Historical + API-ready current views, liquid clustering)
         ↓
    REST API / BI Dashboards
```

---

## 🎯 API Query Patterns & Optimization

### Query 1: Get feeders with available capacity > 5 MW
```sql
SELECT utility_id, feeder_id, available_capacity_mw
FROM gold.feeders_with_capacity
WHERE available_capacity_mw > 5.0
ORDER BY available_capacity_mw DESC;
```
**Optimization**: `CLUSTER BY [utility_id, feeder_id]`

### Query 2: Get DER summary for a specific feeder (aggregated counts)
```sql
SELECT * FROM gold.feeder_der_summary
WHERE feeder_id = 'utility1_1105354';
```
**Optimization**: `CLUSTER BY [feeder_id, utility_id]`
**Use Case**: Initial map view showing DER counts per feeder

### Query 3: Temporal query - capacity history
```sql
SELECT feeder_id, max_hosting_capacity_mw, __START_AT, __END_AT
FROM gold.circuits_current
WHERE feeder_id = 'utility1_1105354'
  AND __END_AT IS NULL  -- Current records only
ORDER BY __START_AT DESC;
```
**Optimization**: `CLUSTER BY [utility_id, feeder_id]`

### Query 4: Point-in-time historical query
```sql
SELECT feeder_id, max_hosting_capacity_mw
FROM gold.circuits_current
WHERE feeder_id = 'utility1_1105354'
  AND __START_AT <= '2024-01-15'
  AND (__END_AT > '2024-01-15' OR __END_AT IS NULL);
```

### Query 5: Get individual installed DER records for map click-through
```sql
SELECT 
  der_id,
  der_type,
  nameplate_capacity_kw,
  in_service_date,
  feeder_id,
  utility_id
FROM gold.der_installed_current
WHERE feeder_id = 'utility1_1105354'
  AND __END_AT IS NULL  -- Current records only
ORDER BY nameplate_capacity_kw DESC;
```
**Optimization**: `CLUSTER BY [utility_id, feeder_id]`
**Use Case**: User clicks feeder on map → Show table of all installed DER on that feeder

### Query 6: Get individual planned DER records for map click-through
```sql
SELECT 
  der_id,
  der_type,
  nameplate_capacity_kw,
  estimated_in_service_date,
  interconnection_queue_id,
  feeder_id,
  utility_id
FROM gold.der_planned_current
WHERE feeder_id = 'utility1_1105354'
  AND __END_AT IS NULL  -- Current records only
ORDER BY estimated_in_service_date ASC;
```
**Optimization**: `CLUSTER BY [utility_id, feeder_id]`
**Use Case**: User clicks feeder on map → Show table of all planned DER projects on that feeder

---

## 🗺️ Map Application Architecture

The IEDR application renders an interactive map with the following data flow:

### Initial Map Load
1. **Query**: `SELECT * FROM gold.feeders_with_capacity WHERE available_capacity_mw > 0`
   - Returns all feeders with DER capacity data
   - Includes `color_hex` for feeder rendering
   - Shows aggregate DER counts via JOIN to `feeder_der_summary`

### User Clicks Feeder Icon
2. **Query 5** (Installed DER): Retrieve all installed DER records for clicked feeder
3. **Query 6** (Planned DER): Retrieve all planned DER projects for clicked feeder
4. **Display**: Two tables showing individual DER details

### Data Quality Indicators
The application highlights missing/incomplete data:
* **Last refresh date**: `hca_refresh_date` from `circuits_current`
* **Missing feeders**: Unresolved DER (feeder_id IS NULL) from `data_quality_metrics_silver`
* **Volume statistics**: Record counts from `file_tracking` table
* **Quality characteristics**: DQ metrics (null keys, negative capacities, unresolved counts)

**Implementation Pattern:**
```sql
-- Dashboard query for data quality summary
SELECT 
  utility_id,
  dataset_type,
  total_records,
  null_key_count,
  unresolved_feeder_count,
  negative_capacity_count,
  pipeline_update_id,
  ingestion_date
FROM gold.data_quality_metrics_silver
WHERE ingestion_date = (SELECT MAX(ingestion_date) FROM gold.data_quality_metrics_silver)
ORDER BY utility_id, dataset_type;
```

---

## 🛠️ Implementation Phases

### Phase 1: Foundation ✅ COMPLETE
* Set up Unity Catalogs (`dev_iedr`)
* Create schemas (`bronze`, `ny_iedr` for silver/gold)
* Configure landing volumes (`dev_iedr.bronze.landing`, `dev_iedr.bronze.metadata`)
* Establish project folder structure

### Phase 2: Bronze Layer ✅ COMPLETE
* Upload CSVs to landing zone (`/Volumes/dev_iedr/bronze/landing/{utility_id}/`)
* Develop `helpers.py` (lineage utilities, artifact handling)
* Build DLT pipeline `01_bronze_ingestion.py`:
  - `circuits_raw`, `der_installed_raw`, `der_planned_raw`, `file_tracking`
  - All with lineage columns (no partitioning/clustering)
* **Implement two-step artifact handling:**
  - Extract case-variant business columns from `_rescued_data`
  - Clear structural artifacts (`_c0`, `_file_path`)
  - Track with `_index_col_dropped` flag
* **Data ingested**: 66,448 circuit records (100% pass expectations), 72,346 DER records

### Phase 3: Silver Layer ✅ COMPLETE
* Develop `schema_normalization.py` (utility-specific transformations)
* **Implement N-utility registry pattern** (`utility_registry.py`)
  - Configuration-driven utility onboarding
  - Dynamic utility processing loop in Silver pipeline
  - Scalable to N utilities without code changes
* Transform and standardize circuit and DER data
* Aggregate segments → feeders (utility1)
* Unpivot wide formats → narrow (utility1 DER)
* Enforce data quality expectations
* Add `interconnection_queue_id` to utility1 for schema alignment
* **Fix skipChangeCommits**: DQ metrics streaming reads handle full-refresh overwrites
* **Unit tests**: `test_schema_normalization.py` (20+ test cases)

### Phase 4: Gold Layer ✅ COMPLETE
* Develop SCD Type 2 history tables via Auto CDC (`dp.create_auto_cdc_flow`)
* Create API-optimized current views (no SCD2 columns)
* Apply liquid clustering for query patterns
* Fix `filter_current()` helper to use `__END_AT IS NULL` (not `__IS_CURRENT`)
* Track data quality metrics (unresolved feeders, null keys)
* **SCD2 multi-tenant keys**: Include `utility_id` in all composite keys

### Phase 5: Observability & Monitoring ✅ COMPLETE (2026-07-08)
* **Freshness Monitoring**: Added `last_refresh_date` and `days_since_refresh` to DQ metrics
  - Circuits: 1,362-1,376 days since last refresh (stale data detected)
  - DER tables: NULL (no refresh dates in source data)
* **Volume Baseline Tracking**: Created SQL query for anomaly detection (`docs/volume_baseline_tracking.sql`)
  - 30-day rolling average and standard deviation
  - ±2σ threshold for ANOMALY_LOW/ANOMALY_HIGH flags
* **Job Alert Setup**: Documentation for email, Slack, and PagerDuty alerts (`docs/JOB_ALERT_SETUP.md`)
* **Data Quality Thresholds**: Defined alert criteria for null keys, stale data, volume anomalies
* **Testing**: Full pipeline validation with observability enhancements
  - Run 1 (full refresh): 2m 47s
  - Run 2 (incremental): 1m 13s (56% faster)
  - All 17 tables across 4 layers validated

### Phase 6: Testing & Production (IN PROGRESS)
* ✅ End-to-end pipeline validation (2 successful runs)
* ✅ Data quality validation (DQ metrics table operational)
* ✅ N-utility registry pattern implemented and tested
* ✅ Observability enhancements (freshness, volume, alerts) validated
* ✅ Code refactoring and simplification (silver layer -144 lines)
* ✅ Import and schema mapping bugs fixed during testing
* 🔲 Deploy to `prod_iedr`
* 🔲 Schedule pipelines (daily/weekly cadence)
* 🔲 Set up monitoring dashboards
* 🔲 Production smoke tests

---

## 📈 Scalability Considerations

* **Current Scale**: 2 utilities (utility1, utility2), 269 feeders, ~72K DER projects
* **Future Scale**: 5 utilities, ~50K feeders, ~5M DER projects
* **Auto Loader**: Handles incremental file arrivals efficiently
* **Liquid Clustering**: Adapts to data skew across utilities automatically
* **SCD Type 2 (Gold)**: Manages historical growth without performance degradation
* **Multi-tenant architecture**: Scales horizontally as utilities onboard

---

## 📊 Data Quality Strategy

* **Bronze**: Minimal expectations (valid file structure, schema drift detection)
* **Silver**: Enforce expectations with `@dlt.expect_or_drop`:
  - `valid_feeder_id`: NOT NULL (circuits only)
  - `valid_utility_id`: NOT NULL (all tables)
  - `valid_hca_refresh_date`: NOT NULL (circuits, required for SCD2)
  - `valid_der_type`: NOT NULL (DER tables)
* **Unresolved DER preserved**: `feeder_id IS NULL` records pass through for tracking
* **Gold**: Completeness metrics tracked in `data_quality_metrics_silver` table
  - Total records, null key counts, unresolved feeder counts, negative capacity counts

**Current DQ Metrics (2026-07-08 run):**
* Circuits: 0 null keys, 0 negative capacities, 0 unresolved feeders
  - **Freshness**: utility1 (1,362 days old), utility2 (1,376 days old) - both STALE
  - Total feeders: 2,178 (269 utility1 + 1,909 utility2)
* DER Installed: 0 null keys, 210 unresolved feeders (8 utility1, 202 utility2)
  - Total assets: 39,657 (14,120 utility1 + 25,537 utility2)
* DER Planned: 0 null keys, 919 unresolved feeders (345 utility1, 574 utility2)
  - Total projects: 32,689 (1,733 utility1 + 30,956 utility2)

---

## 🔍 Monitoring & Observability

### Real-Time Monitoring
* **DLT Event Logs**: Track pipeline runs, errors, execution duration
* **Pipeline Update ID**: Trace every record to originating run
* **File Tracking**: Idempotency and deduplication audit trail (`file_tracking` table)
* **SCD2 History**: Temporal queries via `__START_AT` and `__END_AT`

### Data Quality Tracking
* **DQ Metrics Table**: `dev_iedr.silver.data_quality_metrics_silver`
  - Total record counts per utility/dataset
  - Null key counts (critical business keys)
  - Negative capacity counts (data validation)
  - Unresolved feeder counts (missing linkage data)
  - **NEW: Freshness monitoring** (circuits only)
    - `last_refresh_date`: Most recent `hca_refresh_date` from utility data
    - `days_since_refresh`: Days between pipeline run and last data refresh
    - Alerts trigger when > 30 days (warning) or > 45 days (critical)

### Volume Anomaly Detection
* **Baseline Tracking**: 30-day rolling average and standard deviation
* **Anomaly Detection**: ±2 standard deviations (95% confidence interval)
* **Status Flags**:
  - `ANOMALY_LOW`: Record count significantly below baseline (potential data loss)
  - `ANOMALY_HIGH`: Record count significantly above baseline (duplication/drift)
  - `NORMAL`: Within expected range
  - `INSUFFICIENT_BASELINE`: < 3 runs (baseline not yet established)
* **Query**: `/docs/volume_baseline_tracking.sql`

### Alert Configuration
* **Pipeline Failures**: Email alerts on job failure
* **DQ Threshold Violations**: SQL alerts for null keys, stale data, high unresolved counts
* **Volume Anomalies**: SQL alerts for record count deviations
* **Setup Guide**: `/docs/JOB_ALERT_SETUP.md`

### Alert Priority Matrix

| Condition | Severity | Channel | Action |
|-----------|----------|---------|--------|
| Pipeline failure | 🔴 Critical | Email + Slack | Immediate investigation |
| null_key_count > 0 | 🔴 Critical | Email + Slack | Fix referential integrity |
| days_since_refresh > 45 | 🔴 Critical | Email | Contact utility data team |
| Volume ANOMALY_LOW | 🟠 High | Slack | Investigate data loss |
| Volume ANOMALY_HIGH | 🟡 Medium | Slack | Check for duplicates |
| unresolved_feeder_count > 1000 | 🟡 Medium | Email | Review utility data quality |

### Observability Tools

1. **Freshness Monitoring Query:**
```sql
SELECT utility_id, table_name, last_refresh_date, days_since_refresh,
  CASE 
    WHEN days_since_refresh > 45 THEN '🔴 STALE'
    WHEN days_since_refresh > 30 THEN '⚠️  AGING'
    ELSE '✅ FRESH'
  END as freshness_status
FROM dev_iedr.silver.data_quality_metrics_silver
WHERE table_name = 'circuits'
  AND ingestion_date = CURRENT_DATE;
```

2. **Volume Anomaly Query:** See `/docs/volume_baseline_tracking.sql`

3. **Historical Trend Analysis:**
```sql
SELECT ingestion_date, utility_id, table_name, total_records,
  LAG(total_records) OVER (PARTITION BY utility_id, table_name ORDER BY ingestion_date) as prev_records,
  total_records - LAG(total_records) OVER (PARTITION BY utility_id, table_name ORDER BY ingestion_date) as day_over_day_change
FROM dev_iedr.silver.data_quality_metrics_silver
WHERE ingestion_date >= CURRENT_DATE - 7
ORDER BY utility_id, table_name, ingestion_date DESC;
```

---

## 🚀 Technology Stack

* **Platform**: Databricks (Unity Catalog, Serverless, Photon enabled)
* **Orchestration**: Lakeflow Spark Declarative Pipelines (SDP, formerly DLT)
* **Storage**: Delta Lake with Liquid Clustering
* **Ingestion**: Auto Loader (cloudFiles)
* **Languages**: Python (PySpark)
* **Version Control**: Git (GitHub repository: `ny-iedr-data-platform`)
* **Testing**: pytest + PySpark (unit tests for transformations)
* **Pipeline Mode**: Triggered (not continuous)
* **Monitoring**: Databricks SQL Alerts, DLT Event Logs, custom DQ metrics

---

## 🎓 Lessons Learned

### 1. Clustering Requires Materialized Columns
* **Problem**: Cannot cluster by computed columns (`utility_id`) at Bronze ingestion
* **Solution**: Defer clustering to Silver/Gold where columns are materialized
* **Result**: Bronze is pure raw storage; optimization happens downstream

### 2. Partitioning + Clustering Conflicts
* **Problem**: Delta tables cannot have both partitioning AND clustering
* **Solution**: Choose one strategy per layer based on use case
* **Result**: Bronze has neither, Gold uses liquid clustering only

### 3. SCD2 Metadata Column Names
* **Problem**: Assumed `__IS_CURRENT` column exists; SCD2 only provides `__START_AT`/`__END_AT`
* **Solution**: Use `__END_AT IS NULL` to identify current records
* **Result**: `filter_current()` helper correctly filters for API views

### 4. Multi-tenant Key Design
* **Problem**: Native IDs collide across utilities (both have `ProjectID = 1000`)
* **Solution**: Include `utility_id` in all SCD2 composite keys
* **Result**: No state corruption, safe concurrent updates

### 5. Schema Alignment Strictness
* **Problem**: `allowMissingColumns=True` masks schema drift
* **Solution**: Use `allowMissingColumns=False` and add missing columns explicitly
* **Result**: Schema bugs surface immediately (caught `interconnection_queue_id` mismatch)

### 6. N-Utility Scalability Pattern
* **Problem**: Hardcoded utility IDs in pipeline code → architectural changes for each new utility
* **Solution**: Registry pattern with dynamic utility processing loop
* **Result**: Onboard utilities via configuration, not code changes

### 7. Full-Refresh + Streaming Reads
* **Problem**: Silver full-refresh creates change commits that downstream streaming reads reject
* **Solution**: Use `spark.readStream.option("skipChangeCommits", "true")` for DQ metrics
* **Result**: Incremental pipeline runs succeed without full refresh

### 8. Color Standardization Pattern
* **Problem**: Utilities use incompatible color formats — utility1 embeds hex in compound strings, utility2 uses plain names
* **Initial approach**: Attempted to add standardization in Gold API views
* **Better solution**: Move standardization to Silver layer
  * **Why Silver**: Single source of truth, automatic propagation to all downstream tables
  * **Implementation**: `standardize_colors()` in `schema_normalization.py`
  * **Color extraction**: Parse utility1's "0.00 TO 0.29 BROWN-953736" format with regex
  * **Color mapping**: Dictionary-based lookup for utility2's simple names (blue → #0070C0)
  * **Fallback**: Unknown colors → #808080 (gray)
* **Result**: 
  * Gold SCD2 tracks color changes (added to track_history_column_list)
  * API views simply pass through standardized color_hex and color_name
  * Map rendering works uniformly across all utilities
  * Future utilities only need to add mappings to Silver layer

---

### 9. Geospatial Data Readiness Strategy
* **Problem**: Utilities provide inconsistent location data — some with grid hierarchies, some with geometry, some with neither
* **Solution**: Multi-tier graceful degradation
  * **Tier 1 (Geometry)**: Parse WKT LineString when provided → precise feeder line rendering
  * **Tier 2 (Grid Hierarchy)**: Parse native_feeder_id (e.g., "36_13_81756") → district-level clustering
  * **Tier 3 (Point Fallback)**: Use feeder_id as discrete point markers
* **Implementation**:
  * Silver: Parse `grid_state_code`, `grid_area_code`, `grid_circuit_id` from native_feeder_id (NULL for unparseable)
  * Silver: Add `geom_wkt` passthrough column (NULL until utility provides geometry)
  * Gold: Add `map_render_level` computed field ("geometry"/"district"/"point_fallback")
  * Gold: Create `feeder_map_layer` table (denormalized, Liquid Clustered on utility_id + grid_area_code)
* **Result**: 
  * utility2: 1,909 feeders across 54 districts → district-level map clustering ready NOW
  * utility1: 269 feeders → point fallback (no grid hierarchy in IDs)
  * Frontend queries ONE table with zero joins
  * Geometry upgrade path ready (add WKT column to source, pipeline passthroughs automatically)



### 10. MODE Aggregation for Robust Capacity Calculation
* **Problem**: utility1 circuit data has segment-level records where 5% of feeders (15 out of 291) have outlier capacity values in 1-2 segments
  * Example: Circuit 2304312 has 271 segments @ 2.1 MW and 1 segment @ 10 MW
  * Using MAX: Reports 10 MW (takes the outlier)
  * Using SUM: Would be completely wrong (would multiply capacity by segment count)
* **Initial approach**: Used MAX() aggregation assuming all segments have identical capacity
* **Better solution**: Use MODE() to take the most common value
  * **Why MODE**: Immune to outliers in both directions (high and low)
  * **Implementation**: `mode("NYHCPV_csv_FMAXHC")` in `aggregate_utility1_segments()`
  * **Data pattern**: 99%+ of segments within a feeder share ONE consistent value
  * **Outlier handling**: MODE correctly ignores the 0.5-1% outlier segments
* **Examples**:
  * Circuit 2304312: 271 @ 2.1 MW, 1 @ 10 MW → MODE = 2.1 MW ✓ (ignores high outlier)
  * Circuit 1204003: 218 @ 10 MW, 1 @ 4.2 MW → MODE = 10 MW ✓ (ignores low outlier)
* **Impact**:
  * **Affected feeders**: 15 out of 291 (5.2%) with capacity variation across segments
  * **Unaffected feeders**: 276 (94.8%) where all segments already have identical capacity
  * **Data quality improvement**: More accurate representation of actual feeder capacity
  * **Robustness**: Handles data entry errors and outliers automatically
* **Result**: 
  * Capacity calculations now immune to segment-level data quality issues
  * Better representation of true feeder capacity than MAX approach
  * No performance impact (MODE is native PySpark function)
  * Tested and validated: All 291 feeders processed correctly

---

### 11. Observability First Approach
* **Problem**: Initial implementation focused on data flow without proactive monitoring
* **Initial state**: Basic DQ metrics (null counts, record totals) but no freshness or volume tracking
* **Solution**: Add layered observability enhancements
  * **Freshness Monitoring**: Track `days_since_refresh` to detect stale utility data
  * **Volume Baseline**: Compare current run against 30-day rolling average (±2σ threshold)
  * **Alert Matrix**: Tiered severity levels (Critical → High → Medium) with clear action items
* **Implementation**:
  * Silver DQ metrics table enhanced with freshness columns (circuits only)
  * SQL query for volume anomaly detection (`volume_baseline_tracking.sql`)
  * Alert setup documentation with Slack/email/PagerDuty patterns
* **Result**:
  * Proactive detection of data issues before they impact downstream users
  * Baseline tracking catches both data loss (ANOMALY_LOW) and duplication (ANOMALY_HIGH)
  * Freshness monitoring flags when utility data exceeds 30-day refresh window
  * Clear escalation path: Email → Slack → PagerDuty based on severity

---


### 12. Refactoring with Two-Layer Schema Transformation
* **Problem**: Initial code review suggested eliminating the intermediate mapping layer to reduce complexity
* **Attempted refactoring (2026-07-08)**: Removed canonical mapping functions, expecting transformers to return final schemas
* **Reality**: Transformers were still returning intermediate schemas with `_raw` suffixes
* **Failure mode**: Pipeline failed with column resolution errors
  * `Cannot resolve column name "Circuits_Phase3_CIRCUIT"` (intermediate column leaked through)
  * `cannot import name 'map_der_installed_to_canonical'` (broken imports)
  * Wrong function calls (`map_circuits_to_canonical()` on DER data)
* **Solution**: Restored the two-layer transformation pattern with clarified responsibilities
  1. **Transformer functions** (`utility_registry.py`): Extract and normalize utility-specific data → intermediate schema
  2. **Canonical mappers** (`schema_normalization.py`): Type casting, date parsing, color standardization → final schema
* **Why the pattern works**:
  * **Separation of concerns**: Utility logic vs. common transformations
  * **Schema flexibility**: `allowMissingColumns=True` for intermediate unions (utilities may vary)
  * **Reusability**: Common transformations (colors, dates, nulls) applied once for all utilities
  * **Testability**: Each layer can be unit tested independently
* **Refactoring outcome**: Simplified code structure (-144 lines) while preserving separation of concerns
  * Removed verbose docstrings (details moved to ARCHITECTURE.md)
  * Cleaned up imports (removed try/except fallbacks)
  * Kept functional architecture intact
* **Lesson**: Code simplification ≠ architectural simplification. The two-layer pattern adds ~30 lines vs. direct mapping but provides critical maintainability benefits for N-utility scaling.

---

---

## 📚 References

* [Databricks SDP (DLT) Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
* [Delta Lake Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
* [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
* [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
