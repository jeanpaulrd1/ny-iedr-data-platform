# NY IEDR Data Platform - Unit Tests

This directory contains comprehensive unit tests for the data transformation pipeline.

## 📂 Test Structure

```
tests/
├── conftest.py                      # Shared pytest fixtures (SparkSession)
├── test_helpers.py                  # Tests for helper utilities
├── test_schema_normalization.py     # Tests for Silver transformations ✅ NEW
└── README.md                        # This file
```

## 🧪 Test Coverage

### `test_helpers.py`
Tests for `pipelines/utils/helpers.py`:
* ✅ `strip_utf8_bom()` - UTF-8 BOM removal from column names
* ✅ `normalize_null_sentinels()` - NULL string normalization
* ✅ `compute_record_hash()` - SHA-256 record hashing for SCD2
* ✅ `add_lineage_columns()` - Lineage metadata injection

### `test_schema_normalization.py` ✅ NEW
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

**Total: 20+ test cases covering all Silver transformation logic**

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
```

### Run Specific Test Class
```bash
# Test only utility 1 segment aggregation
pytest tests/test_schema_normalization.py::TestAggregateUtility1Segments -v

# Test only DER unpivot
pytest tests/test_schema_normalization.py::TestUnpivotUtility1Der -v
```

### Run Single Test
```bash
pytest tests/test_schema_normalization.py::TestUnpivotUtility1Der::test_unpivots_hybrid_project -v
```

---

## 📊 Test Output Example

```
tests/test_schema_normalization.py::TestAggregateUtility1Segments::test_aggregates_segments_to_feeder PASSED
tests/test_schema_normalization.py::TestAggregateUtility1Segments::test_max_hosting_capacity_not_sum PASSED
tests/test_schema_normalization.py::TestUnpivotUtility1Der::test_unpivots_hybrid_project PASSED
tests/test_schema_normalization.py::TestUnpivotUtility1Der::test_handles_null_feeder_id PASSED
tests/test_schema_normalization.py::TestMapCircuitsToCanonical::test_maps_utility1_fields PASSED
tests/test_schema_normalization.py::TestMapDerToCanonical::test_preserves_composite_der_id PASSED

==================== 20 passed in 15.23s ====================
```

---

## 🧩 What's Tested vs. What's Missing

### ✅ Covered (Silver Layer Transformations)
* Utility 1 segment aggregation logic (MAX vs SUM)
* Utility 1 DER unpivot (14 tech columns → narrow)
* Canonical field mapping (both utilities)
* NULL handling and edge cases
* Composite DER keys for hybrid projects
* Timestamp parsing with timezone offsets
* Helper functions (BOM removal, null normalization, hashing)

### ❌ Not Yet Covered (Future Work)
* Integration tests: Full Bronze → Silver → Gold pipeline
* DLT expectations enforcement (`@dlt.expect_or_drop`)
* Data quality metrics aggregation
* Gold layer SCD Type 2 logic (when implemented)
* Auto Loader file ingestion
* Edge cases: Large files, corrupt data, schema evolution

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

---

## 📚 References

* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Best Practices](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
* [Databricks DLT Testing Guide](https://docs.databricks.com/delta-live-tables/testing.html)
