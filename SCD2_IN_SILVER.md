# SCD2 in Silver - Architecture Update

## ✅ Summary

Silver layer now maintains SCD Type 2 history as the **system of record**.
Gold layer simplified to consumption views reading from Silver current records.

---

## 🏗️ **Architecture Change:**

### **Before (SCD2 in Gold):**
```
Bronze (raw) → Silver (full-refresh, latest only) → Gold (SCD2 history)
                                                            ↑
                                                    System of Record
```

### **After (SCD2 in Silver):**
```
Bronze (raw) → Silver (SCD2 history) → Gold (current views + aggregates)
                        ↑
                System of Record
```

---

## 🔑 **Key Changes:**

### **Silver Layer:**

**Tables Created:**
- `circuits_standardized` (SCD2, KEY: `feeder_id`)
- `der_installed_standardized` (SCD2, KEY: `der_id, der_type`)
- `der_planned_standardized` (SCD2, KEY: `der_id, der_type`)
- `data_quality_metrics_silver` (tracking violations from Bronze)

**Pattern:**
```python
# Staging table: transformations only
@dlt.table(name="circuits_staging", temporary=True)
def circuits_staging():
    # All transformations (aggregate, unpivot, normalize, map)
    return canonical_df

# Target table: SCD2 history
dlt.create_target_table(name="circuits_standardized")

dlt.apply_changes(
    target="circuits_standardized",
    source="circuits_staging",
    keys=["feeder_id"],  # Natural key
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2,  # SCD2 tracking
    track_history_column_list=[...]  # Columns to track changes on
)
```

**Result:**
- Silver tables have `__START_AT`, `__END_AT`, `__IS_CURRENT` columns
- Full history maintained in Silver
- Composite key `(der_id, der_type)` preserves unpivoted DER technology rows

---

### **Gold Layer (Not Implemented Yet - Next Branch):**

**Future Pattern:**
```python
@dlt.table(name="feeders_with_capacity")
def feeders_with_capacity():
    # Read CURRENT records from Silver
    circuits = dlt.read("circuits_standardized").filter(F.col("__IS_CURRENT") == True)
    der = dlt.read("der_installed_standardized").filter(F.col("__IS_CURRENT") == True)
    
    # Business logic: calculate available capacity
    return circuits.join(der, ...).groupBy(...).agg(...)
```

**Gold Tables (future):**
- `feeders_with_capacity` - Current feeders with capacity calculations
- `feeder_der_summary` - Current DER aggregated by feeder
- `data_quality_metrics` - Observability dashboard

**Key Points:**
- Gold reads `WHERE __IS_CURRENT = TRUE` from Silver
- No SCD2 storage in Gold
- Gold can be rebuilt from Silver anytime
- Historical queries read directly from Silver

---

## ✅ **Benefits of SCD2 in Silver:**

1. **Silver = Authoritative System of Record**
   - Full historical truth maintained at standardized layer
   - Gold corruption? Rebuild from Silver
   
2. **Simpler Gold Queries**
   - No need for `WHERE __END_AT IS NULL` everywhere
   - Just filter `WHERE __IS_CURRENT = TRUE`
   
3. **Better Disaster Recovery**
   - Silver has full history
   - Gold is just derived views (stateless)
   
4. **Clearer Separation of Concerns**
   - Silver = Data quality + History
   - Gold = Business logic + Aggregation
   
5. **Easier Testing**
   - Validate SCD2 behavior in Silver
   - Gold is just transformations on current records

---

## 🔧 **All 10 Fixes Still Applied:**

✅ 1. Single-pass CASE WHEN (not filter-union)
✅ 2. Real CSV column names
✅ 3. ~~SCD1~~ → **SCD2 in Silver** (history preserved)
✅ 4. No expect_or_drop on DER feeder_id
✅ 5. No left_semi join on DER
✅ 6. Utility 1 DER unpivot implemented
✅ 7. Utility 1 circuits segment aggregation implemented
✅ 8. DQ metrics read from Bronze
✅ 9. No partitioning on Silver
✅ 10. Composite key (der_id, der_type) for DER

---

## 📊 **SCD2 Keys Rationale:**

### **Circuits: `feeder_id` (single key)**
```
One feeder per utility, versioned on capacity changes
```

### **DER: `(der_id, der_type)` (composite key)**
```
After unpivot, ProjectID 20751 becomes:
- utility1_20751_SolarPV (100 kW)
- utility1_20751_EnergyStorageSystem (50 kW)

If KEY was only `der_id`:
  → APPLY CHANGES keeps 1 row per project
  → Other technology rows silently discarded (WRONG!)

With KEY = (der_id, der_type):
  → Each technology tracked independently (CORRECT!)
```

---

## 📂 **Files Updated:**

### **1. `pipelines/02_silver_transformations.py`** (440 lines)
- Staging tables for transformations
- Target tables with `dlt.create_target_table` + `dlt.apply_changes`
- Keys: `feeder_id` for circuits, `(der_id, der_type)` for DER
- SEQUENCE BY: `ingestion_timestamp`
- All 10 fixes preserved

### **2. `pipelines/utils/schema_normalization.py`** (293 lines, unchanged)
- Helper functions still valid
- No changes needed

### **3. `ARCHITECTURE.md`** (updated)
- Silver section: Added SCD2 details
- Gold section: Changed to consumption layer
- Data flow diagram: Updated to show SCD2 in Silver
- Query patterns: Historical queries from Silver, not Gold

---

## 🚀 **Next Steps:**

1. ✅ **Review updated files** (current task)
2. ✅ **Approve for commit** to `feature/silver-layer`
3. ⏸️ **Gold layer implementation** - new branch `feature/gold-layer` later
   - Read from Silver `WHERE __IS_CURRENT = TRUE`
   - Implement capacity calculations
   - Create feeder-DER summary
   - Build data quality metrics dashboard

---

## 📖 **References:**

- **02_silver_transformations.py** - SCD2 implementation
- **schema_normalization.py** - Transformation helpers (unchanged)
- **ARCHITECTURE.md** - Updated design doc
- **SILVER_FIXES.md** - Detailed explanation of all 10 fixes

---

**Status:** ✅ Ready for review before commit to `feature/silver-layer`

**Gold Layer:** ⏸️ Defer to new branch after Silver is validated
