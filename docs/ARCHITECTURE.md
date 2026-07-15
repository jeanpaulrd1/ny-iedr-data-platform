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
* `dev_iedr.dq.unmapped_der_types` - Tracks unmapped DER technology types from utility2

**Transformations:**
* **Utility 1**: Aggregate segment-level → feeder-level circuits (MODE capacity, not SUM)
* **Utility 1**: Unpivot wide DER format (14 one-hot tech columns) → narrow (der_id, der_type, capacity)
* **Utility 1**: Add `interconnection_queue_id` column (as NULL) to align with utility2 schema
* **All Utilities**: Normalize null sentinels ("NULL", "null", "" → SQL NULL)
* **All Utilities**: Map inconsistent field names to unified schema via two-step transformation:
  1. **Transformer functions** return intermediate schema with `_raw` suffixes
  2. **Canonical mapping layer** (`map_circuits_to_canonical`, `map_der_to_canonical`) produces final schema
* **All Utilities**: Standardize DER technology types to canonical names (see DER Type Standardization below)
* **All Utilities**: Standardize color codes to unified hex/name format (color_hex, color_name)

**Key Features:**
* **Full-Refresh**: Tables rebuilt on each pipeline run from current Bronze data
* **No partitioning**: Pure liquid clustering (partitioning removed to avoid conflicts)
* **No clustering at Silver**: Deferred to Gold layer for query-specific optimization
* Data quality expectations enforced (`@dlt.expect_or_drop` on circuits, `@dlt.expect_or_quarantine` on DER types)
* Unresolved DER (feeder_id IS NULL) preserved for data_quality_metrics tracking
* **Composite DER Keys**: `der_id` includes technology type (e.g., `utility1_proj1_SolarPV`) for hybrid projects
* **Two-layer transformation**: Intermediate → Canonical mapping preserves separation of concerns
  - Transformers focus on utility-specific extraction logic
  - Canonical mappers handle common type casting, date parsing, color/der_type standardization
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

### 5. DER Technology Type Standardization
**Decision**: Centralized DER technology normalization with validation and quarantine

**Heterogeneous Source Pattern**: This demonstrates our approach to handling structural differences across utilities — utility1 uses wide format (14 one-hot columns), utility2 uses narrow format (free-text column). Silver normalizes both to a unified schema via mapping dictionaries and validation.

**Problem**: Utilities use different DER_TYPE naming conventions:
* **utility1**: Wide format with 14 one-hot encoded tech columns (`Solar_PV`, `Wind`, `Energy_Storage`, etc.)
* **utility2**: Narrow format with free-text `DER_TYPE` column (e.g., "Solar - PV", "Energy Storage - Battery", "Wind - Onshore")

**Challenge**: Without normalization:
* Aggregation bugs (same technology counted separately)
* Mapping drift (new utility2 values not caught)
* API inconsistency (different tech names for same resource type)

**Solution**: Three-layer normalization implemented in Silver layer

**1. Canonical Technology List** (`UTILITY1_DER_TECH_COLUMNS`):
```python
# pipelines/utils/schema_normalization.py
UTILITY1_DER_TECH_COLUMNS = [
    "Solar_PV", "Wind", "Energy_Storage", "Fuel_Cell",
    "CHP_Cogen", "Microturbine", "Diesel_Gen", "Natural_Gas_Gen",
    "Biogas_Gen", "Other_Gen", "Microgrid", "Inverter",
    "Synchronous_Gen", "Combined_System"
]
```
* **Single source of truth**: All canonical DER types in one list
* **utility1 DER unpivot** uses this list to generate narrow format
* **utility2 DER mapping** validates against this list

**2. utility2 Mapping Dictionary** (`UTILITY2_DER_TYPE_MAP`):
```python
UTILITY2_DER_TYPE_MAP = {
    "Solar - PV": "Solar_PV",
    "Energy Storage - Battery": "Energy_Storage",
    "Wind - Onshore": "Wind",
    "Wind - Offshore": "Wind",
    "CHP - Cogeneration": "CHP_Cogen",
    "Fuel Cell - Hydrogen": "Fuel_Cell",
    # ... more mappings
}
```
* Maps utility2's free-text values → canonical names
* Case-insensitive matching with `.strip()` normalization
* Handles spacing/punctuation variations

**3. Data Quality Validation** (`@dlt.expect_or_quarantine`):
```python
@dlt.expect_or_quarantine(
    "canonical_der_type_only",
    f"der_type IN {tuple(UTILITY1_DER_TECH_COLUMNS)}"
)
def der_installed_standardized():
    # ... transformation logic
```
* **Quarantines** rows with non-canonical `der_type` values
* Quarantine tables: `dev_iedr.silver.__der_installed_standardized_quarantine`
* Passes valid rows through to Silver tables

**4. Unmapped DER_TYPE Tracking** (`dev_iedr.dq.unmapped_der_types`):
```python
@dlt.table(name="dev_iedr.dq.unmapped_der_types")
def unmapped_der_types_metric():
    return track_unmapped_der_types(all_der)
```
* **DQ metric table** tracks utility2 values NOT in canonical list
* Aggregates by `(utility_id, der_type)` with:
  * `unmapped_count`: Number of projects with unmapped type
  * `unmapped_capacity_kw`: Total capacity
  * `first_seen`, `last_seen`: Temporal tracking
  * `alert_message`: Human-readable alert string
* **Empty result = good** (all types mapped correctly)
* **Non-empty result = alert** (new/unmapped types detected)

**Implementation Location**:
* **Canonical list**: `pipelines/utils/schema_normalization.py` (UTILITY1_DER_TECH_COLUMNS)
* **Mapping dict**: `pipelines/utils/schema_normalization.py` (UTILITY2_DER_TYPE_MAP)
* **Validation**: `pipelines/02_silver_transformations.py` (@dlt.expect_or_quarantine decorators)
* **DQ metric**: `pipelines/utils/dq_metrics.py` (track_unmapped_der_types function)

**Benefits**:
* **Schema drift prevention**: New utility2 types immediately flagged
* **Mapping audit**: Validation notebook verifies all sample types covered
* **API consistency**: Gold layer aggregates use unified technology names
* **Operational visibility**: Quarantine tables + DQ metrics provide clear alerts
* **Zero-drift guarantee**: No unmapped values reach Gold layer

**Validation Workflow**:
1. Run `notebooks/validate_utility2_der_types.py` after updating mapping dict
2. Check for unmapped types in Bronze sample data
3. Add new mappings to `UTILITY2_DER_TYPE_MAP` if found
4. Re-deploy pipeline
5. Monitor `dev_iedr.dq.unmapped_der_types` in production

---

### 6. SCD2 Key Design
**Multi-tenant safety**: All SCD2 keys include `utility_id`
* Prevents collisions when different utilities use the same native IDs
* Example: `utility1_1000` vs `utility2_1000` are distinct DER projects
* Keys: `(feeder_id, utility_id)` for circuits, `(der_id, utility_id)` for DER

**Why composite keys?**
* Native IDs (e.g., `ProjectID`) are not globally unique across utilities
* `utility_id` prefix in `feeder_id`/`der_id` provides uniqueness
* SCD2 tracking requires utility_id in KEYS to prevent state corruption

### 7. Lineage & Run Tracking
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

### 8. Schema Evolution Strategy
* **Bronze**: All STRING columns, preserve raw source fidelity
* Auto Loader schema inference with evolution enabled (`addNewColumns` mode)
* **Silver**: Mapping layer handles heterogeneous schemas across utilities
* **Strict schema alignment**: `unionByName(allowMissingColumns=False)` catches drift
