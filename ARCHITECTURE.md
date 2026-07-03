# NY IEDR Data Platform - Architecture

## 🏗️ Medallion Architecture Overview

### Bronze Layer (Raw Ingestion)
**Purpose**: Ingest raw CSV files with minimal transformation

**Tables:**
* `bronze.circuits_raw` - Circuit/feeder infrastructure data
* `bronze.der_installed_raw` - Installed DER projects  
* `bronze.der_planned_raw` - Planned DER projects
* `bronze.file_tracking` - File ingestion audit trail with idempotency

**Key Features:**
* Auto Loader (cloudFiles) for incremental ingestion
* All columns stored as **STRING** for schema fidelity and evolution
* **Partitioning + Clustering**: `PARTITION BY ingestion_date` + `CLUSTER BY utility_id`
  - Partitioning for lifecycle management (simple `DROP PARTITION` for retention)
  - Clustering within partitions for query performance
* File tracking with `file_hash` and `pipeline_update_id` for idempotency

---

### Silver Layer (Standardized, Full-Refresh)
**Purpose**: Standardize schemas, enforce quality, create common data model

**Tables:**
* `silver.circuits_standardized` - Feeder-level circuits (full-refresh snapshots)
* `silver.der_installed_standardized` - Normalized DER installations (full-refresh)
* `silver.der_planned_standardized` - Normalized DER planning queue (full-refresh)
* `silver.data_quality_metrics_silver` - Data quality metrics from transformations

**Transformations:**
* **Utility 1**: Aggregate segment-level → feeder-level circuits (MAX capacity, not SUM)
* **Utility 1**: Unpivot wide DER format (14 one-hot tech columns) → narrow (der_id, der_type, capacity)
* **All Utilities**: Normalize null sentinels ("NULL", "null", "" → SQL NULL)
* **All Utilities**: Map inconsistent field names to unified schema (single-pass CASE WHEN)
* **All Utilities**: Standardize DER technology types to canonical names

**Key Features:**
* **Full-Refresh**: Tables rebuilt on each pipeline run from current Bronze data
* **Liquid Clustering**: `CLUSTER BY utility_id, feeder_id, der_type`
* Data quality expectations enforced (`@dlt.expect_or_drop` on circuits, DER unresolved feeders pass through)
* No partitioning (liquid clustering handles multi-dimensional queries efficiently)
* Unresolved DER (feeder_id IS NULL) preserved for data_quality_metrics tracking
* **Composite DER Keys**: `der_id` includes technology type (e.g., `utility1_proj1_SolarPV`) for hybrid projects

---

### Gold Layer (Business-Ready + Historical Tracking)
**Purpose**: API-optimized aggregates and SCD Type 2 history

**Tables:**
* `gold.circuits_current` - SCD Type 2 for feeder capacity history
* `gold.der_installed_current` - SCD Type 2 for DER state tracking
* `gold.feeders_with_capacity` - Pre-aggregated: feeders with available capacity (current view)
* `gold.feeder_der_summary` - Pre-aggregated: all DER per feeder (current view)
* `gold.data_quality_metrics` - Observability (time-series tracking)

**Key Features:**
* **SCD Type 2**: Track capacity changes over time via DLT's `APPLY CHANGES INTO`
  - `circuits_current`: KEY = `feeder_id`, SEQUENCE BY `hca_refresh_date`
  - `der_installed_current`: KEY = `(der_id, der_type)`, SEQUENCE BY `ingestion_timestamp`
  - Reads current records from Silver (`WHERE __IS_CURRENT = TRUE` conceptually)
* **API-Optimized Current Views**: No SCD2 columns, simple queries for API
* **Liquid Clustering** optimized for query patterns:
  - `feeders_with_capacity`: `CLUSTER BY utility_id, available_capacity_mw DESC`
  - `feeder_der_summary`: `CLUSTER BY feeder_id, utility_id`
  - `circuits_current`: `CLUSTER BY utility_id, feeder_id, __END_AT`
  - `data_quality_metrics`: `CLUSTER BY metric_date, utility_id`

---

## 🔑 Key Design Decisions

### 1. Partitioning + Clustering Strategy by Layer

**Bronze Layer: Partitioning + Clustering**
```
PARTITION BY ingestion_date + CLUSTER BY utility_id
```

**Rationale:**
* **Partitioning** = Lifecycle management
  - Low cardinality (one partition per day/month)
  - Simple retention: `ALTER TABLE DROP PARTITION WHERE ingestion_date < '2023-01-01'`
  - Physical isolation for compliance/regulatory needs
* **Clustering** = Query performance
  - Clustering happens **within each partition** on `utility_id`
  - Optimizes common query pattern: `WHERE ingestion_date = '...' AND utility_id = '...'`
  - Moderate cardinality (~8 utilities) works well with clustering

**Silver/Gold Layers: Pure Liquid Clustering**
```
CLUSTER BY utility_id, feeder_id, der_type  (Silver)
CLUSTER BY <query-pattern-specific>         (Gold)
```

**Rationale:**
* **No partitioning** - Avoid 50K+ partitions on `feeder_id`
* **Flexibility** - Liquid clustering adapts to changing query patterns
* **Multi-dimensional** - Efficiently handles queries filtering on any combination of columns
* **No lifecycle needs** - Silver/Gold keep all history, no regular partition drops

**Why This Hybrid Approach?**
* Each layer uses the strategy that fits its use case:
  - **Bronze** = Lifecycle-focused (partitioning) + performance (clustering)
  - **Silver/Gold** = Pure query optimization (liquid clustering)
* Not mixing strategies **within** a layer, but **across** layers based on requirements

---

### 2. Silver = Full-Refresh, Gold = History
* **Silver**: Rebuilt on each run from current Bronze data (monthly utility snapshots)
  - Simpler logic, no SCD2 complexity
  - Matches source pattern (utilities deliver full monthly snapshots, not CDC)
* **Gold**: SCD Type 2 tracks changes over time
  - Reads Silver's current data
  - Maintains historical records for trend analysis
  - API views query Gold current records (no `__END_AT` filtering needed)

### 3. Lineage & Run Tracking
Every table includes:
* `pipeline_id` - Static pipeline UUID
* `pipeline_update_id` - **Primary run identifier** (DLT's `update_id`)
* `ingestion_timestamp` - Record-level timestamp
* `ingestion_date` - Date column for time-based filtering and partitioning (Bronze)
* `batch_id` - Microbatch identifier
* `source_file` - Source file path
* `file_hash` - File-level deduplication
* `record_hash` - Row-level SCD detection (Gold only)

**Why `pipeline_update_id`?**
* Available via `spark.conf.get("pipelines.update_id")`
* Links records → runs → DLT event logs → source files
* Enables lineage, debugging, rollback, and idempotency

### 4. Schema Evolution Strategy
* **Bronze**: All STRING columns, preserve raw source fidelity
* Auto Loader schema inference with evolution enabled
* **Silver**: Mapping layer handles heterogeneous schemas across utilities

### 5. Heterogeneous Source Handling
**Utility 1** (Wide, Segment-Level):
* Circuits: Segment rows → aggregated to feeder (MAX capacity, not SUM)
* DER: Wide format (SolarPV_kW, Wind_kW) → unpivoted to narrow (der_type, capacity_kw)

**Utility 2** (Narrow, Feeder-Level):
* Already feeder-level, narrow DER format
* Direct mapping to Silver schema

**Result**: Unified Silver schema across all utilities

---

## 📊 Data Flow

```
Source CSV Files (Landing Zone: /Volumes/.../landing/)
         ↓
    [Auto Loader - Incremental]
         ↓
Bronze Layer (Raw, STRING columns, partitioned by date, clustered by utility)
         ↓
  [Standardization, Segment→Feeder, Unpivot, Quality Checks]
         ↓
Silver Layer (Full-Refresh, Feeder-level, Normalized DER types, liquid clustering)
         ↓
  [SCD Type 2, Aggregation, Business Logic]
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
**Optimization**: `CLUSTER BY utility_id, available_capacity_mw DESC`

### Query 2: Get all DER for a specific feeder
```sql
SELECT * FROM gold.feeder_der_summary
WHERE feeder_id = '1105354';
```
**Optimization**: `CLUSTER BY feeder_id, utility_id`

### Query 3: Temporal query - capacity history
```sql
SELECT feeder_id, max_hosting_capacity_mw, __START_AT, __END_AT
FROM gold.circuits_current
WHERE feeder_id = '1105354'
ORDER BY __START_AT DESC;
```
**Optimization**: `CLUSTER BY utility_id, feeder_id, __END_AT`

### Query 4: Retention/Cleanup (Bronze Layer)
```sql
-- Simple partition drop for retention (Bronze only)
ALTER TABLE bronze.circuits_raw 
DROP IF EXISTS PARTITION (ingestion_date < '2023-01-01');
```

---

## 🛠️ Implementation Phases

### Phase 1: Foundation
* Set up Unity Catalogs (dev_iedr, prod_iedr)
* Create schemas (bronze, silver, gold)
* Configure landing volumes
* Establish project folder structure

### Phase 2: Bronze Layer
* Upload sample CSVs to landing zone
* Develop `helpers.py` (lineage utilities: `get_update_id()`, `get_file_hash()`)
* Build DLT pipeline `01_bronze_ingestion.py`:
  - `circuits_raw`, `der_installed_raw`, `der_planned_raw`, `file_tracking`
  - All with lineage columns and partitioning + clustering

### Phase 3: Silver Layer ✅ COMPLETE
* Develop `schema_normalization.py` (utility-specific transformations)
* Transform and standardize circuit and DER data
* Aggregate segments → feeders
* Unpivot wide formats → narrow
* Enforce data quality expectations
* **Unit tests**: `test_schema_normalization.py` (20+ test cases)

### Phase 4: Gold Layer (TO BE IMPLEMENTED)
* Develop SCD Type 2 history tables via `APPLY CHANGES INTO`
* Create API-optimized current views (no SCD2 columns)
* Apply liquid clustering for query patterns
* Track data quality metrics

### Phase 5: Testing & Production
* Integration tests for full Bronze → Gold pipeline
* Data quality validation dashboards
* Deploy to prod_iedr
* Schedule pipelines
* Set up monitoring

---

## 📈 Scalability Considerations

* **Current Scale**: 8 utilities, ~50K feeders, ~5M DER projects
* **Auto Loader**: Handles incremental file arrivals efficiently
* **Bronze Partitioning**: Low cardinality (daily/monthly), simple lifecycle management
* **Liquid Clustering**: Adapts to data skew across utilities automatically
* **SCD Type 2 (Gold)**: Manages historical growth without performance degradation

---

## 📊 Data Quality Strategy

* **Bronze**: Minimal expectations (valid file structure, non-empty)
* **Silver**: Enforce expectations with `@dlt.expect_or_drop`:
  - `valid_feeder_id`: NOT NULL
  - `valid_utility_id`: NOT NULL
  - `valid_der_type`: NOT NULL
* **Gold**: Completeness metrics tracked in `data_quality_metrics` table

---

## 🔍 Monitoring & Observability

* **DLT Event Logs**: Track pipeline runs, errors, data quality metrics
* **Data Quality Dashboard**: Query `gold.data_quality_metrics`
* **Pipeline Update ID**: Trace every record to originating run
* **File Tracking**: Idempotency and deduplication audit trail

---

## 🚀 Technology Stack

* **Platform**: Databricks (Unity Catalog)
* **Orchestration**: Delta Live Tables (DLT)
* **Storage**: Delta Lake with Liquid Clustering
* **Ingestion**: Auto Loader (cloudFiles)
* **Languages**: Python (PySpark), SQL
* **Version Control**: Git (GitHub)
* **Testing**: pytest + PySpark (unit tests for transformations)
