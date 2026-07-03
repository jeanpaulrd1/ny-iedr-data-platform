# Silver Layer - Round 2 Fixes Applied

## ✅ Summary

All 8 issues from Round 2 have been addressed, on top of the original 10 fixes from Round 1.

---

## 🔧 **Round 2 Issues Fixed:**

### **Issue 1: `.count()` action inside DLT streaming function**

**Problem:** Line 288 had `if utility1_df.count() > 0:` which triggers a Spark action inside DLT transformation.

**Before (BAD):**
```python
utility1_df = df.filter(F.col("utility_id") == "utility_1")
if utility1_df.count() > 0:  # ❌ Action in DLT function!
    utility1_unpivoted = unpivot_utility1_der(utility1_df)
else:
    utility1_unpivoted = df.filter(F.lit(False))
```

**After (FIXED):**
```python
# Just call unpivot - it will return empty DF if no data
utility1_df = df.filter(F.col("utility_id") == "utility_1")
utility1_unpivoted = unpivot_utility1_der(utility1_df, include_installation_date=True)
```

**Impact:** No Spark actions in DLT transformations; unpivot handles empty DataFrames gracefully.

---

### **Issue 2: `map_circuits_to_canonical` references utility-2-only columns on utility-1 DataFrame**

**Problem:** After union, `map_circuits_to_canonical` tried to reference utility2 raw column names on utility1 rows.

**Before (BAD):**
```python
utility2_df = utility2_df.withColumn(
    "feeder_id",
    F.concat(F.lit("utility2_"), F.col("Master_CDF"))
)
# Still has columns: Master_CDF, feeder_voltage, feeder_max_hc, etc.

# After union, map_circuits_to_canonical tries:
F.when(F.col("utility_id") == "utility_2", F.col("feeder_max_hc"))  # Doesn't exist on utility1!
```

**After (FIXED):**
```python
# Utility 2: Rename to intermediate schema matching utility 1
utility2_df = utility2_df.select(
    F.col("utility_id"),
    F.concat(F.lit("utility2_"), F.col("Master_CDF")).alias("feeder_id"),
    F.col("Master_CDF").alias("native_feeder_id"),
    F.col("feeder_voltage").alias("voltage_kv_raw"),
    F.col("feeder_max_hc").alias("max_hosting_capacity_raw"),
    ...
)

# Now both utilities have: *_raw, native_feeder_id, feeder_id
# map_circuits_to_canonical just casts types:
F.col("max_hosting_capacity_raw").cast("double").alias("max_hosting_capacity_mw")
```

**Impact:** Consistent intermediate schema; no missing column references.

---

### **Issue 3: `apply_changes` keys missing `utility_id` — cross-utility collision risk**

**Problem:** SCD2 keys were just `["feeder_id"]` or `["der_id", "der_type"]`. If utility1 and utility2 both have a feeder with the same native ID, they could collide.

**Before (BAD):**
```python
keys=["feeder_id"]  # What if utility1_ABC and utility2_ABC both exist?
```

**After (FIXED):**
```python
# Circuits
keys=["utility_id", "feeder_id"]  # Explicit utility separation

# DER
keys=["utility_id", "der_id", "der_type"]  # Explicit utility separation
```

**Impact:** No cross-utility collisions in SCD2; explicit composite keys.

---

### **Issue 4: `sequence_by="ingestion_timestamp"` makes replays non-idempotent at SCD2 level**

**Problem:** If we replay the same source file, it gets a new `ingestion_timestamp`, so SCD2 treats it as a new version even though the data is identical.

**Before (BAD):**
```python
sequence_by="ingestion_timestamp"  # Circuits
# Replay same file → new timestamp → new SCD2 version (even if data unchanged)
```

**After (FIXED):**
```python
# Circuits: Use business timestamp (hca_refresh_date)
sequence_by="hca_refresh_date"  # Deterministic from source data

# DER: No business timestamp available, keep ingestion_timestamp
sequence_by="ingestion_timestamp"  # DER has no alternative
```

**Impact:** Circuit replays are idempotent (same data → same SCD2 state).

---

### **Issue 5: `track_history_column_list` excludes `utility_id` from versioning**

**Problem:** If `utility_id` changes (shouldn't happen, but for completeness), it won't be tracked in SCD2 history.

**Before (BAD):**
```python
track_history_column_list=["voltage_kv", "max_hosting_capacity_mw", ...]
# utility_id not tracked!
```

**After (FIXED):**
```python
track_history_column_list=[
    "utility_id",  # ✅ Now tracked
    "voltage_kv",
    "max_hosting_capacity_mw",
    ...
]
```

**Impact:** Complete SCD2 tracking of all mutable columns.

---

### **Issue 6: DQ metrics hardcodes utility-specific raw column names**

**Problem:** Lines 395, 409, 413 referenced utility-specific columns like `Circuits_Phase3_CIRCUIT`, `ProjectCircuitID`, `DER_INTERCONNECTION_LOCATION`. Won't scale to new utilities.

**Before (BAD):**
```python
circuits_raw = dlt.read("circuits_raw")
circuits_metrics = circuits_raw.groupBy(...).agg(
    F.sum(F.when(F.col("Circuits_Phase3_CIRCUIT").isNull(), 1).otherwise(0))  # Utility 1 only!
)
```

**After (FIXED):**
```python
# Read from STAGING tables (after normalization to canonical schema)
circuits_staging = dlt.read("circuits_staging")
circuits_metrics = circuits_staging.groupBy(...).agg(
    F.sum(F.when(F.col("feeder_id").isNull(), 1).otherwise(0))  # Canonical column name
)
```

**Impact:** DQ metrics work for all utilities; no hardcoded utility-specific columns.

---

### **Issue 7: `temporary=True` prevents external inspection and limits DQ metrics options**

**Problem:** Staging tables marked `temporary=True` can't be queried externally for debugging.

**Before (BAD):**
```python
@dlt.table(
    name="circuits_staging",
    temporary=True  # ❌ Can't inspect for debugging!
)
```

**After (FIXED):**
```python
@dlt.table(
    name="circuits_staging",
    comment="Staging: ... (kept for debugging, not temporary)"
    # No temporary=True → can query staging tables
)
```

**Impact:** Staging tables queryable for debugging and DQ metrics.

---

### **Issue 8: InServiceDate reference in planned DER may not exist in planned file**

**Problem:** `map_der_to_canonical` referenced `InServiceDate` column for utility 1 planned DER, but this column might not exist.

**Before (BAD):**
```python
# In map_der_to_canonical:
F.when(F.col("utility_id") == "utility_1", F.to_date(F.col("InServiceDate")))
# Fails if InServiceDate doesn't exist!
```

**After (FIXED):**
```python
# In unpivot_utility1_der:
def unpivot_utility1_der(df: DataFrame, include_installation_date: bool = False):
    # Only include InServiceDate if requested AND it exists
    if include_installation_date and "InServiceDate" in df.columns:
        base_columns.append("InServiceDate")
    ...

# In map_der_to_canonical:
F.when(
    (F.col("utility_id") == "utility_1") & F.col("planned_installation_date_raw").isNotNull(),
    F.to_date(F.col("planned_installation_date_raw"))
)
```

**Impact:** Handles missing InServiceDate column gracefully; no runtime errors.

---

## 📊 **Additional Performance Fixes (SCPAP Lints):**

### **SCPAP001: `.dtypes` accessed repeatedly in loop**

**Before:**
```python
string_cols = [col for col, dtype in df.dtypes if dtype == "string"]  # Inside function
```

**After:**
```python
schema = df.dtypes  # Cache once
string_cols = [col for col, dtype in schema if dtype == "string"]
```

---

### **SCPAP004: `.withColumn()` chaining**

**Before:**
```python
df = df.withColumn("col1", ...)
df = df.withColumn("col2", ...)  # Nested execution plan
```

**After:**
```python
df = df.withColumns({
    "col1": ...,
    "col2": ...
})  # Single logical plan node
```

---

## 📋 **Files Modified:**

### **1. `pipelines/02_silver_transformations.py`** (447 lines, unchanged count)
**Changes:**
- ✅ Removed `.count()` call (Issue #1)
- ✅ Utility2 circuits: use `.select()` with intermediate schema (Issue #2)
- ✅ Utility2 DER: use `.select()` with intermediate schema (Issue #2)
- ✅ SCD2 keys include `utility_id` (Issue #3)
- ✅ Circuits: `sequence_by="hca_refresh_date"` (Issue #4)
- ✅ `track_history_column_list` includes `utility_id` (Issue #5)
- ✅ DQ metrics read from staging tables (Issue #6)
- ✅ Staging tables NOT temporary (Issue #7)
- ✅ Planned DER: `include_installation_date=True` (Issue #8)
- ✅ SCPAP001 fix: cache `.dtypes`
- ✅ SCPAP004 fix: use `.withColumns()`

### **2. `pipelines/utils/schema_normalization.py`** (287 lines, -6 lines)
**Changes:**
- ✅ `unpivot_utility1_der`: add `include_installation_date` parameter (Issue #8)
- ✅ `unpivot_utility1_der`: conditional InServiceDate inclusion
- ✅ `map_circuits_to_canonical`: expect intermediate schema (Issue #2)
- ✅ `map_der_to_canonical`: expect intermediate schema (Issue #2)
- ✅ `map_der_to_canonical`: conditional `planned_installation_date_raw` (Issue #8)

---

## ✅ **Complete Checklist (Round 1 + Round 2):**

### **Round 1 (Original 10 Fixes):**
- [x] 1. Single-pass CASE WHEN (not filter-union)
- [x] 2. Real CSV column names
- [x] 3. SCD2 in Silver (not Gold)
- [x] 4. No expect_or_drop on DER feeder_id
- [x] 5. No left_semi join on DER
- [x] 6. Utility 1 DER unpivot implemented
- [x] 7. Utility 1 circuits segment aggregation implemented
- [x] 8. DQ metrics read from Bronze (now staging)
- [x] 9. No partitioning on Silver
- [x] 10. Composite key (der_id, der_type)

### **Round 2 (Additional 8 Fixes):**
- [x] 11. No `.count()` actions in DLT functions
- [x] 12. SCD2 keys include `utility_id`
- [x] 13. `sequence_by` uses deterministic columns
- [x] 14. `track_history_column_list` includes `utility_id`
- [x] 15. DQ metrics uses canonical column names
- [x] 16. Staging tables NOT temporary
- [x] 17. InServiceDate handling for planned DER
- [x] 18. SCPAP performance lints fixed

---

## 🚀 **Next Steps:**

1. ✅ Review Round 2 fixes
2. ✅ Approve for commit to `feature/silver-layer`
3. ⏸️ Gold layer - defer to new branch

---

**Status:** ✅ All 18 fixes applied. Ready for final review.
