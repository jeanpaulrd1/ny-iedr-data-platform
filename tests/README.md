# NY IEDR Data Platform - Unit Tests

This directory contains comprehensive unit tests for the data transformation pipeline.

## 📂 Test Structure

```
tests/
├── conftest.py                      # Shared pytest fixtures (SparkSession)
├── test_helpers.py                  # Tests for helper utilities ✅
├── test_bronze_ingestion.py         # Tests for Bronze layer ingestion ✅ NEW
├── test_schema_normalization.py     # Tests for Silver transformations ✅
├── test_gold_scd2.py                # Tests for Gold SCD Type 2 logic ✅
├── test_gold_api_views.py           # Tests for Gold API views ✅ NEW
└── README.md                        # This file
```

## 🧪 Test Coverage

### `test_helpers.py` (19 tests)
Tests for `pipelines/utils/helpers.py`:
* ✅ `strip_utf8_bom()` - UTF-8 BOM removal from column names
* ✅ `normalize_null_sentinels()` - NULL string normalization
* ✅ `compute_record_hash()` - SHA-256 record hashing for SCD2
* ✅ `add_lineage_columns()` - Lineage metadata injection

### `test_bronze_ingestion.py` ✅ NEW (18 tests)
Tests for `pipelines/01_bronze_ingestion.py` patterns:

**TestAutoLoaderPatterns** (4 tests):
* ✅ Extracts utility ID from file path
* ✅ Extracts ingestion date from filename
* ✅ CSV schema inference with headers
* ✅ UTF-8 BOM handling in column names

**TestLineageColumns** (4 tests):
* ✅ Adds `ingestion_timestamp`
* ✅ Adds `ingestion_date` (derived from timestamp)
* ✅ Adds `pipeline_update_id`
* ✅ Preserves original columns

**TestNullSentinelHandling** (3 tests):
* ✅ Normalizes common NULL strings ("NULL", "N/A", "")
* ✅ Handles whitespace variants (" NULL ", "  ")
* ✅ Preserves valid data

**TestDataQualityExpectations** (3 tests):
* ✅ Detects NULL utility_id violations
* ✅ Detects invalid capacity values (negative)
* ✅ Tracks DQ violation counts

**TestMultiFileIngestion** (2 tests):
* ✅ Handles multiple files from same utility
* ✅ Handles files from multiple utilities

### `test_schema_normalization.py` (17 tests)
Tests for `pipelines/utils/schema_normalization.py`:

**TestAggregateUtility1Segments** (4 tests):
* ✅ Segment → feeder aggregation (3 segments → 1 feeder)
* ✅ MAX hosting capacity (not SUM - capacity repeats across segments)
* ✅ Most recent HCA refresh date selection
* ✅ NULL value handling in optional fields

**TestUnpivotUtility1Der** (6 tests):
* ✅ Single technology unpivot (solar only)
* ✅ Hybrid project unpivot (solar + storage → 2 rows)
* ✅ Composite `der_id` includes technology type
* ✅ Zero-capacity technologies filtered out
* ✅ NULL `feeder_id` preserved (unresolved DER)
* ✅ `planned_installation_date_raw` handling (installed vs planned)

**TestMapCircuitsToCanonical** (3 tests):
* ✅ Utility 1 field mapping and type casting
* ✅ Utility 2 timestamp parsing with timezone offset
* ✅ Lineage column preservation

**TestMapDerToCanonical** (4 tests):
* ✅ Installed DER mapping (`der_status = "installed"`)
* ✅ Planned DER mapping with installation date
* ✅ Unresolved feeder handling (NULL `feeder_id`)
* ✅ Composite `der_id` preservation for hybrid projects

### `test_gold_scd2.py` (17 tests)
Tests for `pipelines/03_gold_scd2.py` SCD Type 2 logic:

**TestCircuitsSCD2Logic** (4 tests):
* ✅ Detects capacity changes (triggers new SCD2 version)
* ✅ Ignores lineage column changes (no false versions)
* ✅ Sequence by `hca_refresh_date` (determines version order)
* ✅ Composite key validation (feeder_id uniqueness)

**TestDerSCD2Logic** (5 tests):
* ✅ Composite key (der_id, der_type) for hybrid projects
* ✅ Detects DER capacity changes
* ✅ Detects feeder_id changes (DER moving between feeders)
* ✅ Sequence by `ingestion_timestamp`
* ✅ Tracks planned installation date changes

**TestSCD2Configuration** (5 tests):
* ✅ Circuits `except_column_list` defined correctly
* ✅ Circuits `track_history_column_list` complete
* ✅ DER tracks `feeder_id` (projects can move)
* ✅ Planned DER tracks installation date and queue ID
* ✅ Composite keys defined correctly

**TestSCD2EdgeCases** (3 tests):
* ✅ Handles NULL `feeder_id` (unresolved DER)
* ✅ Multiple changes in same run
* ✅ Same sequence date with different values

### `test_gold_api_views.py` ✅ NEW (14 tests)
Tests for `pipelines/04_gold_api_views.py` API-optimized views:

**TestFeedersWithCapacity** (5 tests):
* ✅ Calculates available capacity correctly (max - installed)
* ✅ Handles feeders with no DER (full capacity available)
* ✅ Handles negative available capacity (overcapacity)
* ✅ Filters SCD2 current records only (`__IS_CURRENT = true`)
* ✅ kW to MW conversion precision

**TestFeederDerSummary** (6 tests):
* ✅ Aggregates installed and planned DER separately
* ✅ Technology breakdown by type (solar, storage, wind)
* ✅ Detects hybrid projects correctly (multiple types)
* ✅ Counts unique projects vs total DER rows
* ✅ Excludes NULL feeder_id (unresolved DER)
* ✅ Union of installed and planned sources

**TestApiViewsEdgeCases** (3 tests):
* ✅ Handles zero-capacity DER
* ✅ Multiple feeders in single query
* ✅ Validates clustering columns defined

---

## 📊 Total Test Coverage

| Layer | Test File | Tests | Lines | Status |
|-------|-----------|-------|-------|--------|
| **Helpers** | test_helpers.py | 19 | ~350 | ✅ Complete |
| **Bronze** | test_bronze_ingestion.py | 18 | ~317 | ✅ NEW |
| **Silver** | test_schema_normalization.py | 17 | ~520 | ✅ Complete |
| **Gold SCD2** | test_gold_scd2.py | 17 | ~282 | ✅ Complete |
| **Gold API** | test_gold_api_views.py | 14 | ~339 | ✅ NEW |
| **TOTAL** | **5 test files** | **80 tests** | **~1,808 lines** | **✅ Comprehensive** |

---

## 🚀 Running Tests

### Prerequisites
```bash
# Ensure pytest and PySpark are available
pip install pytest pyspark
```

### Run All Tests
```bash
cd /Workspace/Repos/jeanpaulrd1@gmail.com/ny-iedr-data-platform
pytest tests/ -v
```

### Run Specific Test File
```bash
# Test helper utilities
pytest tests/test_helpers.py -v

# Test Bronze layer
pytest tests/test_bronze_ingestion.py -v

# Test Silver transformations
pytest tests/test_schema_normalization.py -v

# Test Gold SCD2 logic
pytest tests/test_gold_scd2.py -v

# Test Gold API views
pytest tests/test_gold_api_views.py -v
```

### Run Specific Test Class
```bash
# Test only feeders_with_capacity view
pytest tests/test_gold_api_views.py::TestFeedersWithCapacity -v

# Test only Auto Loader patterns
pytest tests/test_bronze_ingestion.py::TestAutoLoaderPatterns -v

# Test only circuits SCD2 logic
pytest tests/test_gold_scd2.py::TestCircuitsSCD2Logic -v
```

### Run Single Test
```bash
pytest tests/test_gold_api_views.py::TestFeedersWithCapacity::test_calculates_available_capacity_correctly -v
```

---

## 📊 Test Output Example

```
tests/test_helpers.py::TestStripUtf8Bom::test_removes_bom_from_single_column PASSED
tests/test_bronze_ingestion.py::TestAutoLoaderPatterns::test_extracts_utility_from_file_path PASSED
tests/test_schema_normalization.py::TestAggregateUtility1Segments::test_aggregates_segments_to_feeder PASSED
tests/test_gold_scd2.py::TestCircuitsSCD2Logic::test_detects_capacity_change PASSED
tests/test_gold_api_views.py::TestFeedersWithCapacity::test_calculates_available_capacity_correctly PASSED

==================== 80 passed in 24.32s ====================
```

---

## 🧩 What's Tested vs. What's Missing

### ✅ Covered (85 tests)
* **Helper utilities** (BOM removal, null normalization, hashing, lineage)
* **Bronze ingestion** (Auto Loader patterns, file path extraction, lineage, DQ)
* **Silver transformations** (segment aggregation, DER unpivot, field mapping)
* **Gold SCD2 logic** (change detection, sequence ordering, composite keys)
* **Gold API views** (capacity calculations, aggregations, SCD2 filtering)
* **Edge cases** (NULL handling, hybrid projects, negative values, overcapacity)

### ❌ Not Yet Covered (Future Work)
* Integration tests: Full Bronze → Silver → Gold pipeline execution
* DLT runtime execution (these are unit tests simulating behavior)
* Data quality metrics aggregation (`05_gold_data_quality.py` - pending)
* Performance tests (large datasets, clustering efficiency)
* Schema evolution scenarios
* Real Auto Loader incremental file ingestion

---

## 🔧 Continuous Integration

To integrate with CI/CD:

```yaml
# .github/workflows/test.yml
name: Unit Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install pytest pyspark
      - run: pytest tests/ -v --junitxml=test-results.xml
      - uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: test-results.xml
```

---

## 📝 Adding New Tests

1. **Create test file**: Follow naming convention `test_*.py`
2. **Import fixtures**: Use `spark` fixture from `conftest.py`
3. **Organize by class**: Group related tests in classes (e.g., `TestMyFunction`)
4. **Document**: Add docstrings explaining what each test validates
5. **Run**: Execute `pytest tests/test_myfile.py -v` to verify

Example:
```python
class TestMyNewFunction:
    """Tests for my_new_function()."""
    
    def test_basic_case(self, spark):
        """Test basic happy-path scenario."""
        df = spark.createDataFrame([...], [...])
        result = my_new_function(df)
        assert result.count() == expected_count
```

---

## 🎯 Testing Philosophy

1. **Unit tests** test individual functions in isolation
2. **Integration tests** test full pipeline flows (Bronze → Gold)
3. **Data quality tests** validate expectations on real data
4. **Each test** should be independent, fast, and deterministic
5. **Mock external dependencies** (file system, APIs) when possible

### Testing Approach by Layer

**Bronze Layer:**
* Test helper logic (file path extraction, lineage generation)
* Validate schema handling and data cleaning
* Simulate Auto Loader patterns without requiring DLT runtime

**Silver Layer:**
* Test transformation functions in isolation
* Validate aggregation and unpivot logic
* Test field mapping and type casting

**Gold SCD2:**
* Simulate expected SCD2 behavior (what triggers versions)
* Validate configuration (keys, sequence columns, tracked columns)
* Test change detection and edge cases

**Gold API Views:**
* Test calculation accuracy (available capacity, aggregations)
* Validate SCD2 filtering and NULL handling
* Test query patterns and edge cases

Integration tests would validate actual DLT execution with real pipelines.

---

## 📚 References

* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Best Practices](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
* [Databricks DLT Testing Guide](https://docs.databricks.com/delta-live-tables/testing.html)
* [DLT APPLY CHANGES Documentation](https://docs.databricks.com/delta-live-tables/cdc.html)
* [Auto Loader Best Practices](https://docs.databricks.com/ingestion/auto-loader/index.html)
