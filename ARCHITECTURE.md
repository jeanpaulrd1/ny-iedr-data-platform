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
* **Partitioning**: `PARTITION BY ingestion_date` (time-based retention)
* **Clustering**: `CLUSTER BY utility_id` (liquid clustering)
* File tracking with `file_hash` and `pipeline_update_id` for idempotency

---

### Silver Layer (Standardized)
**Purpose**: Standardize schemas, handle heterogeneous utility formats, enforce quality

**Tables:**
* `silver.circuits_standardized` - Feeder-level circuits (standardized)
* `silver.der_installed_standardized` - Normalized DER installations
* `silver.der_planned_standardized` - Normalized DER planning queue

**Transformations:**
* **Utility 1**: Aggregate segment-level → feeder-level circuits
* **Utility 1**: Unpivot wide DER format (SolarPV_kW, Wind_kW columns) → narrow format
* **All Utilities**: Standardize DER technology types to canonical names
* **All Utilities**: Map inconsistent field names to unified schema

**Key Features:**
* **Clustering**: `CLUSTER BY utility_id, feeder_id, der_type` (liquid clustering)
* Data quality expectations enforced (`@dlt.expect_all_or_drop`)
* No partitioning (high cardinality on feeder_id ~50K values)

---

### Gold Layer (Business-Ready)
**Purpose**: API-optimized aggregates and historical tracking

**Tables:**
* `gold.circuits_current` - SCD Type 2 for feeder capacity history
* `gold.der_installed_current` - SCD Type 2 for DER state tracking
* `gold.feeders_with_capacity` - Pre-aggregated: feeders with available capacity
* `gold.feeder_der_summary` - Pre-aggregated: all DER per feeder
* `gold.data_quality_metrics` - Observability (partitioned by metric_date)

**Key Features:**
* **SCD Type 2**: Track capacity changes over time via DLT's `APPLY CHANGES INTO`
* **Liquid Clustering** optimized for query patterns:
  - `feeders_with_capacity`: `CLUSTER BY utility_id, available_capacity_mw DESC`
  - `feeder_der_summary`: `CLUSTER BY feeder_id, utility_id`
  - `circuits_current`: `CLUSTER BY utility_id, feeder_id, __END_AT`
* **Partitioning**: Only for `data_quality_metrics` (by metric_date)

---

## 🔑 Key Design Decisions

### 1. Partitioning vs Liquid Clustering Strategy
* **Bronze**: PARTITION BY `ingestion_date` + CLUSTER BY `utility_id`
  - Rationale: Time-based retention (drop old partitions), utility-level filtering
* **Silver/Gold**: Pure liquid clustering (no partitioning)
  - Rationale: Avoid 50K+ partitions on `feeder_id`, enable multi-dimensional queries
  - High cardinality columns handled efficiently by liquid clustering

### 2. Lineage & Run Tracking
Every table includes:
* `pipeline_id` - Static pipeline UUID
* `pipeline_update_id` - **Primary run identifier** (DLT's `update_id`)
* `ingestion_timestamp` - Record-level timestamp
* `ingestion_date` - Date partition key (Bronze only)
* `batch_id` - Microbatch identifier
* `source_file` - Source file path
* `file_hash` - File-level deduplication
* `record_hash` - Row-level SCD detection

**Why `pipeline_update_id`?**
* Available via `spark.conf.get("pipelines.update_id")`
* Links records → runs → DLT event logs → source files
* Enables lineage, debugging, rollback, and idempotency

### 3. Schema Evolution Strategy
* **Bronze**: All STRING columns, preserve raw source fidelity
* Auto Loader schema inference with evolution enabled
* **Silver**: Mapping layer handles heterogeneous schemas across utilities

### 4. Heterogeneous Source Handling
**Utility 1** (Wide, Segment-Level):
* Circuits: Segment rows → aggregated to feeder (SUM capacity)
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
Bronze Layer (Raw, STRING columns, file tracking)
         ↓
  [Standardization, Segment→Feeder, Unpivot, Quality Checks]
         ↓
Silver Layer (Feeder-level, Normalized DER types)
         ↓
  [SCD Type 2, Aggregation, Business Logic]
         ↓
Gold Layer (API-ready, Historical tracking)
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
  - All with lineage columns

### Phase 3: Silver Layer
* Develop `schema_mappings.py` (DER type mappings, field aliases)
* Transform and standardize circuit and DER data
* Aggregate segments → feeders
* Unpivot wide formats → narrow
* Enforce data quality expectations

### Phase 4: Gold Layer
* Develop SCD Type 2 history tables via `APPLY CHANGES INTO`
* Create API-optimized aggregates
* Apply liquid clustering for query patterns
* Track data quality metrics

### Phase 5: Testing & Production
* Unit tests, integration tests, data quality validation
* Deploy to prod_iedr
* Schedule pipelines
* Set up monitoring

---

## 📈 Scalability Considerations

* **Current Scale**: 8 utilities, ~50K feeders, ~5M DER projects
* **Auto Loader**: Handles incremental file arrivals efficiently
* **Liquid Clustering**: Adapts to data skew across utilities automatically
* **SCD Type 2**: Manages historical growth without performance degradation
* **Partitioning Strategy**: Time-based retention in Bronze (drop old partitions)

---

## 📊 Data Quality Strategy

* **Bronze**: Minimal expectations (valid file structure, non-empty)
* **Silver**: Enforce expectations with `@dlt.expect_all_or_drop`:
  - `valid_feeder_id`: NOT NULL
  - `valid_utility_id`: NOT NULL
  - `valid_der_type`: IN (approved list)
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
* **Storage**: Delta Lake
* **Ingestion**: Auto Loader (cloudFiles)
* **Languages**: Python (PySpark), SQL
* **Version Control**: Git (GitHub)
