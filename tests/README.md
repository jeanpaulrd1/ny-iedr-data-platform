# NY IEDR Data Platform - Unit Tests

This directory contains comprehensive unit tests for the data transformation pipeline.

## 📂 Test Structure

```
tests/
├── conftest.py                      # Shared pytest fixtures (SparkSession)
├── test_helpers.py                  # Tests for helper utilities
├── test_schema_normalization.py     # Tests for Silver transformations ✅
├── test_gold_scd2.py                # Tests for Gold SCD Type 2 logic ✅ NEW
└── README.md                        # This file
```

## 🧪 Test Coverage

### `test_helpers.py`
Tests for `pipelines/utils/helpers.py`:
* ✅ `strip_utf8_bom()` - UTF-8 BOM removal from column names
* ✅ `normalize_null_sentinels()` - NULL string normalization
* ✅ `compute_record_hash()` - SHA-256 record hashing for SCD2
* ✅ `add_lineage_columns()` - Lineage metadata injection

### `test_schema_normalization.py`
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

### `test_gold_scd2.py` ✅ NEW
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

**Total Gold Tests: 17 tests covering SCD2 behavior**

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
# Test helper utilities only
pytest tests/test_helpers.py -v

# Test Silver transformations only
pytest tests/test_schema_normalization.py -v

# Test Gold SCD2 logic only
pytest tests/test_gold_scd2.py -v
```

### Run Specific Test Class
```bash
# Test only circuits SCD2 logic
pytest tests/test_gold_scd2.py::TestCircuitsSCD2Logic -v

# Test only DER SCD2 logic
pytest tests/test_gold_scd2.py::TestDerSCD2Logic -v

# Test only SCD2 configuration
pytest tests/test_gold_scd2.py::TestSCD2Configuration -v
```

### Run Single Test
```bash
pytest tests/test_gold_scd2.py::TestCircuitsSCD2Logic::test_detects_capacity_change -v
```

---

## 📊 Test Output Example

```
tests/test_helpers.py::TestStripUtf8Bom::test_removes_bom_from_single_column PASSED
tests/test_schema_normalization.py::TestAggregateUtility1Segments::test_aggregates_segments_to_feeder PASSED
tests/test_schema_normalization.py::TestUnpivotUtility1Der::test_unpivots_hybrid_project PASSED
tests/test_gold_scd2.py::TestCircuitsSCD2Logic::test_detects_capacity_change PASSED
tests/test_gold_scd2.py::TestCircuitsSCD2Logic::test_ignores_lineage_column_changes PASSED
tests/test_gold_scd2.py::TestDerSCD2Logic::test_composite_key_der_id_and_type PASSED
tests/test_gold_scd2.py::TestSCD2Configuration::test_circuits_except_columns_defined PASSED

==================== 53 passed in 18.47s ====================
```

---

## 🧩 What's Tested vs. What's Missing

### ✅ Covered
* **Helper utilities** (BOM removal, null normalization, hashing, lineage)
* **Silver transformations** (segment aggregation, DER unpivot, field mapping)
* **Gold SCD2 logic** (change detection, sequence ordering, composite keys)
* **SCD2 configuration** (except_column_list, track_history_column_list)
* **Edge cases** (NULL handling, hybrid projects, unresolved DER)

### ❌ Not Yet Covered (Future Work)
* Integration tests: Full Bronze → Silver → Gold pipeline
* DLT runtime execution (these are unit tests simulating behavior)
* API views (`04_gold_api_views.py` - to be implemented)
* Data quality metrics aggregation (`05_gold_data_quality.py` - to be implemented)
* Auto Loader file ingestion
* Performance tests (large datasets, clustering efficiency)

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

### SCD2 Testing Approach

Since DLT's `apply_changes` is a runtime operation, these tests:
* **Simulate expected behavior** (what SCD2 should do)
* **Validate configuration** (keys, sequence columns, tracked columns)
* **Test change detection logic** (what triggers new versions)
* **Verify edge cases** (NULL handling, composite keys, lineage exclusion)

Integration tests would validate actual DLT execution with real pipelines.

---

## 📚 References

* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Best Practices](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
* [Databricks DLT Testing Guide](https://docs.databricks.com/delta-live-tables/testing.html)
* [DLT APPLY CHANGES Documentation](https://docs.databricks.com/delta-live-tables/cdc.html)
