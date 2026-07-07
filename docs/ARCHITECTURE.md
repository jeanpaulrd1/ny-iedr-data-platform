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
* File tracking with `file_signature` and `pipeline_update_id` for idempotency
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
* **Dynamic Processing**: Silver pipeline loops over registered utilities automatically
  - No hardcoded utility IDs in pipeline code
  - Scalable to N utilities without architectural changes

**Tables:**
* `dev_iedr.silver.circuits_standardized` - Feeder-level circuits (full-refresh snapshots)
* `dev_iedr.silver.der_installed_standardized` - Normalized DER installations (full-refresh)
* `dev_iedr.silver.der_planned_standardized` - Normalized DER planning queue (full-refresh)
* `dev_iedr.silver.data_quality_metrics_silver` - Data quality metrics from transformations

**Transformations:**
* **Utility 1**: Aggregate segment-level → feeder-level circuits (MAX capacity, not SUM)
* **Utility 1**: Unpivot wide DER format (14 one-hot tech columns) → narrow (der_id, der_type, capacity)
* **Utility 1**: Add `interconnection_queue_id` column (as NULL) to align with utility2 schema
* **All Utilities**: Normalize null sentinels ("NULL", "null", "" → SQL NULL)
* **All Utilities**: Map inconsistent field names to unified schema (single-pass CASE WHEN)
* **All Utilities**: Standardize DER technology types to canonical names

**Key Features:**
* **Full-Refresh**: Tables rebuilt on each pipeline run from current Bronze data
* **No partitioning**: Pure liquid clustering (partitioning removed to avoid conflicts)
* **No clustering at Silver**: Deferred to Gold layer for query-specific optimization
* Data quality expectations enforced (`@dlt.expect_or_drop` on circuits, DER unresolved feeders pass through)
* Unresolved DER (feeder_id IS NULL) preserved for data_quality_metrics tracking
* **Composite DER Keys**: `der_id` includes technology type (e.g., `utility1_proj1_SolarPV`) for hybrid projects
* **Schema Alignment**: Strict union without `allowMissingColumns` to catch schema drift early

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
### 3. SCD2 Key Design
**Multi-tenant safety**: All SCD2 keys include `utility_id`
* Prevents collisions when different utilities use the same native IDs
* Example: `utility1_1000` vs `utility2_1000` are distinct DER projects
* Keys: `(feeder_id, utility_id)` for circuits, `(der_id, utility_id)` for DER

**Why composite keys?**
* Native IDs (e.g., `ProjectID`) are not globally unique across utilities
* `utility_id` prefix in `feeder_id`/`der_id` provides uniqueness
* SCD2 tracking requires utility_id in KEYS to prevent state corruption

### 4. Lineage & Run Tracking
Every table includes:
* `pipeline_update_id` - **Primary run identifier** (format: `run_YYYYMMDD_HHmmss_<hash>`)
* `ingestion_timestamp` - Record-level timestamp
* `ingestion_date` - Date column for time-based filtering
* `source_file` - Source file path (Bronze only)
* `file_signature` - File-level deduplication (Bronze only)

**Why `pipeline_update_id`?**
* Available via `spark.conf.get("pipelines.update_id")`
* Links records → runs → DLT event logs → source files
* Enables lineage, debugging, rollback, and idempotency

### 5. Schema Evolution Strategy
* **Bronze**: All STRING columns, preserve raw source fidelity
* Auto Loader schema inference with evolution enabled (`addNewColumns` mode)
* **Silver**: Mapping layer handles heterogeneous schemas across utilities
* **Strict schema alignment**: `unionByName(allowMissingColumns=False)` catches drift

### 6. Heterogeneous Source Handling
**Utility 1** (Wide, Segment-Level):
* Circuits: Segment rows → aggregated to feeder (MAX capacity, not SUM)
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
  [Standardization, Segment→Feeder, Unpivot, Quality Checks]
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

### Query 2: Get all DER for a specific feeder
```sql
SELECT * FROM gold.feeder_der_summary
WHERE feeder_id = 'utility1_1105354';
```
**Optimization**: `CLUSTER BY [feeder_id, utility_id]`

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

---

## 🛠️ Implementation Phases

### Phase 1: Foundation ✅ COMPLETE
* Set up Unity Catalogs (`dev_iedr`)
* Create schemas (`bronze`, `ny_iedr` for silver/gold)
* Configure landing volumes (`dev_iedr.bronze.landing`, `dev_iedr.bronze.metadata`)
* Establish project folder structure

### Phase 2: Bronze Layer ✅ COMPLETE
* Upload CSVs to landing zone (`/Volumes/dev_iedr/bronze/landing/{utility_id}/`)
* Develop `helpers.py` (lineage utilities)
* Build DLT pipeline `01_bronze_ingestion.py`:
  - `circuits_raw`, `der_installed_raw`, `der_planned_raw`, `file_tracking`
  - All with lineage columns (no partitioning/clustering)
* **Data ingested**: 66,448 circuit records, 72,346 DER records

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

### Phase 5: Testing & Production (IN PROGRESS)
* ✅ End-to-end pipeline validation
* ✅ Data quality validation (DQ metrics table operational)
* ✅ N-utility registry pattern implemented and tested
* 🔲 Integration tests for full Bronze → Gold pipeline
* 🔲 Deploy to `prod_iedr`
* 🔲 Schedule pipelines
* 🔲 Set up monitoring dashboards

---

## 📈 Scalability Considerations

* **Current Scale**: 2 utilities (utility1, utility2), 269 feeders, ~72K DER projects
* **Future Scale**: 8 utilities, ~50K feeders, ~5M DER projects
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

**Current DQ Metrics (2026-07-05 run):**
* Circuits: 0 null keys, 0 negative capacities, 0 unresolved feeders
* DER Installed: 0 null keys, 210 unresolved feeders (8 utility1, 202 utility2)
* DER Planned: 0 null keys, 919 unresolved feeders (345 utility1, 574 utility2)

---

## 🔍 Monitoring & Observability

* **DLT Event Logs**: Track pipeline runs, errors, data quality metrics
* **Data Quality Dashboard**: Query `dev_iedr.gold.data_quality_metrics_silver` for trends
* **Pipeline Update ID**: Trace every record to originating run
* **File Tracking**: Idempotency and deduplication audit trail (`file_tracking` table)
* **SCD2 History**: Temporal queries via `__START_AT` and `__END_AT`

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
* **Edition**: ADVANCED (required for SCD2 and Auto CDC)

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

### 8. Geospatial Data Readiness Strategy
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

---

## 📚 References

* [Databricks SDP (DLT) Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
* [Delta Lake Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
* [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
* [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
