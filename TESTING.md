# Testing Guide - Sales Data DLT Pipeline

This guide covers how to run pytest for the Databricks DLT (Delta Live Tables) pipeline with PySpark, both locally and in GitHub Actions.

## Overview

The test suite covers:
- **Bronze Layer**: Raw data ingestion with Auto Loader
- **Silver Layer**: Data cleaning, standardization, and CDC (SCD Type 1)
- **Gold Layer**: Fact and dimension tables (SCD Type 2)
- **Data Quality**: Validation checks and constraints

## Local Testing

### Prerequisites

1. **Python**: 3.10 - 3.12
2. **Java**: Required for Spark
3. **uv package manager** (recommended) or pip

### Installation

**Using uv (recommended)**:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv pip install -e ".[dev]"
```

**Using pip**:
```bash
pip install -e ".[dev]"
pip install pyspark pytest-cov
```

### Running Tests Locally

**Run all tests**:
```bash
pytest tests/
```

**Run specific test file**:
```bash
pytest tests/test_dlt_transformations.py -v
```

**Run specific test class**:
```bash
pytest tests/test_dlt_transformations.py::TestBronzeLayer -v
```

**Run specific test**:
```bash
pytest tests/test_dlt_transformations.py::TestBronzeLayer::test_bronze_customers_schema -v
```

**Run with coverage report**:
```bash
pytest tests/ --cov=src/sales_data_etl --cov-report=html --cov-report=term-missing
```

**Run only DLT transformation tests**:
```bash
pytest tests/test_dlt_transformations.py -v -k "dlt or bronze or silver or gold"
```

### Test Markers

Organize tests by category using markers:

```bash
# Run only integration tests
pytest tests/ -m integration

# Run unit tests
pytest tests/ -m unit

# Skip slow tests
pytest tests/ -m "not slow"
```

## GitHub Actions Setup

The pipeline includes automated testing via GitHub Actions. Configuration is in `.github/workflows/pytest.yml`.

### Features

- ✅ Runs on Python 3.10, 3.11, 3.12
- ✅ Automatic dependency installation
- ✅ Coverage reports with Codecov integration
- ✅ Triggers on push to `main`/`develop` and pull requests
- ✅ Local Spark testing (no Databricks credentials needed in CI)
- ✅ Test artifacts for debugging

### Triggering Tests

Tests run automatically on:
- Push to `main` or `develop` branches
- Pull requests targeting `main` or `develop`

To manually trigger a workflow via GitHub API:
```bash
gh workflow run pytest.yml
```

### Viewing Results

1. Go to the repository's **Actions** tab
2. Select the workflow run
3. Click on the job to see detailed logs
4. Download artifacts for test results and coverage

## Testing with Databricks Connect (Optional)

To test against a live Databricks workspace:

1. **Set up Databricks credentials**:
```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
export DATABRICKS_CLUSTER_ID=your-cluster-id
```

2. **Run tests with Databricks**:
```bash
pytest tests/ -v
```

The conftest.py automatically detects Databricks credentials and uses them if available.

## Project Structure

```
tests/
├── conftest.py                      # Pytest fixtures & configuration
├── test_dlt_transformations.py      # Main transformation tests
│   ├── TestBronzeLayer              # Raw data ingestion tests
│   ├── TestSilverLayer              # Data cleaning tests
│   ├── TestGoldLayer                # Fact & dimension table tests
│   └── TestDataQuality              # Data validation tests
├── test_dummy.py                    # Placeholder test
└── sample_taxis_test.py             # Example test

.github/workflows/
└── pytest.yml                        # GitHub Actions workflow

pytest.ini                            # Pytest configuration
pyproject.toml                        # Project dependencies
```

## Key Testing Features

### 1. Mock DLT Module
The `MockDLT` class in `conftest.py` simulates DLT functionality locally:
- Mocks `@dlt.table` and `@dlt.view` decorators
- Provides `dlt.read_stream()` and `dlt.read()` functions
- Simulates `dlt.create_auto_cdc_flow()` for SCD operations

### 2. Spark Fixtures
- **`spark`**: Provides local SparkSession for testing
- **`mock_dlt`**: Provides mock DLT module
- **`load_fixture`**: Loads test data from `fixtures/` directory (JSON/CSV)

### 3. Test Data
Create test data in the `fixtures/` directory:

**fixtures/sample_customers.json**:
```json
[
  {"customer_id": 1, "first_name": "John", "last_name": "Doe", "city": "NYC"},
  {"customer_id": 2, "first_name": "Jane", "last_name": "Smith", "city": "LA"}
]
```

Then use in tests:
```python
def test_with_fixture(load_fixture):
    df = load_fixture("sample_customers.json")
    assert df.count() == 2
```

## CI/CD Integration

### Status Checks
Tests must pass before merging PRs:
- Runs on all Python versions (3.10, 3.11, 3.12)
- Generates coverage reports
- Artifacts preserved for debugging

### Coverage Requirements
Coverage reports are uploaded to Codecov. Set up Codecov badge in README:

```markdown
[![codecov](https://codecov.io/gh/YOUR_ORG/sales_data/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_ORG/sales_data)
```

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'pyspark'"
**Solution**:
```bash
pip install pyspark
```

### Issue: "Java not found"
**Solution** (Ubuntu/Debian):
```bash
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Issue: Tests timeout
**Solution**: Increase timeout in conftest.py or use markers to skip slow tests:
```bash
pytest tests/ -m "not slow"
```

### Issue: "SPARK_LOCAL_IP not set"
**Solution**: The GitHub Actions workflow automatically sets this. For local testing:
```bash
export SPARK_LOCAL_IP=127.0.0.1
pytest tests/
```

## Best Practices

1. **Write testable transformations**: Refactor DLT code into pure functions
2. **Use fixtures**: Centralize test data setup in conftest.py
3. **Parametrize tests**: Use `@pytest.mark.parametrize` for multiple scenarios
4. **Test data quality**: Include validation tests for expected output
5. **Keep tests fast**: Use local Spark for unit tests, Databricks for integration tests

## Example: Adding a New Test

1. Add test data to `fixtures/`:
```json
// fixtures/new_feature.json
[{"id": 1, "value": "test"}]
```

2. Add test class:
```python
# tests/test_dlt_transformations.py

class TestNewFeature:
    def test_something(self, spark, load_fixture):
        df = load_fixture("new_feature.json")
        result = df.filter(col("id") == 1)
        assert result.count() == 1
```

3. Run tests:
```bash
pytest tests/test_dlt_transformations.py::TestNewFeature -v
```

## Additional Resources

- [Databricks DLT Documentation](https://docs.databricks.com/workflows/delta-live-tables/)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/)
- [Pytest Documentation](https://docs.pytest.org/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
