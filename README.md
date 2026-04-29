# 📊 Sales Data ETL Pipeline

A modern, scalable ETL pipeline built on Databricks using the **Medallion Architecture** (Bronze → Silver → Gold layers) for processing sales data. This project demonstrates best practices for data engineering with Spark, automated testing, and CI/CD integration.

## 🏗️ Architecture Overview

### Medallion Architecture Layers
- **Bronze Layer**: Raw data ingestion and schema validation
- **Silver Layer**: Data cleaning, standardization, and enrichment
- **Gold Layer**: Business-ready fact and dimension tables

### Project Structure
```
sales_data/
├── src/
│   ├── sales_data/          # Shared Python code for jobs and pipelines
│   └── sales_data_etl/      # Main ETL transformations
│       ├── transformations/ # Dataset definitions by layer
│       │   ├── Bronze/      # Raw data processing
│       │   ├── Silver/      # Cleaned data (customers, orders, products, regions)
│       │   ├── Gold/        # Fact tables and aggregated data
│       │   
│       ├── utilities/       # Utility functions and modules
│       └── explorations/    # Ad-hoc data exploration notebooks
├── tests/                   # Comprehensive unit test suite
├── resources/               # Databricks configurations (pipelines, jobs)
├── fixtures/               # Test data fixtures
└── .github/workflows/      # CI/CD pipelines
```

## 🚀 Getting Started

### Development Environments

Choose how you want to work on this project:

**Option A: Databricks Workspace**
- Direct development in your Databricks workspace
- [Workspace Development Guide](https://docs.databricks.com/dev-tools/bundles/workspace)

**Option B: Local IDE (Recommended)**
- Use Cursor, VS Code, or your preferred IDE
- [VS Code Extension Guide](https://docs.databricks.com/dev-tools/vscode-ext.html)

**Option C: Command Line**
- Full CLI control with Databricks CLI
- [CLI Documentation](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)

### Installation & Setup

1. **Install UV Package Manager** (modern alternative to pip):
   ```bash
   # Install UV
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Install Project Dependencies**:
   ```bash
   uv sync --dev
   ```

3. **Authenticate to Databricks**:
   ```bash
   databricks configure
   ```

## 🛠️ Development Workflow

### Running Transformations

**Single Transformation File**:
- Use `Run file` to execute and preview individual transformations
- Check syntax at [Python Transformation Reference](https://docs.databricks.com/ldp/developer/python-ref)

**Full Pipeline Execution**:
- Use `Run pipeline` to execute all transformations
- Use `+ Add` in file browser to add new dataset definitions
- Use `Schedule` to set up automated pipeline runs

### Testing

**Run All Tests**:
```bash
uv run pytest
```

**Test Coverage**:
```bash
uv run pytest --cov=src/sales_data_etl --cov-report=html
```

## 📦 Deployment

### Development Deployment
```bash
databricks bundle deploy --target dev
```
- Creates development copy with `[dev username]` prefix
- Pauses job schedules by default
- Deploys to: `sales_data` catalog, user-specific schema

### Production Deployment
```bash
databricks bundle deploy --target prod
```
- Production mode with active schedules
- Deploys to: `sales_data` catalog, `prod` schema
- Includes daily job execution (see `resources/sample_job.job.yml`)

### Running Jobs/Pipelines
```bash
databricks bundle run
```

## 🧪 Testing Strategy

### Test Categories
- **Bronze Layer**: Schema validation and raw data ingestion
- **Silver Layer**: Data cleaning, standardization, and transformations
- **Gold Layer**: Fact/dimension table joins and business logic
- **Data Quality**: Validation checks, null detection, duplicate handling

### Example Test Patterns
- Schema validation and type conversion
- Data quality checks (null values, duplicates, format validation)
- Join operations and relationship validation
- SCD Type 2 dimension tracking
- Aggregation and business logic validation

## 🔧 Configuration

### Pipeline Configuration (`resources/sales_data_etl.pipeline.yml`)
- Serverless execution mode
- Catalog: `sales_data`
- Schema: `sales_tables`
- Includes all transformation libraries
- Editable environment dependencies

### Bundle Configuration (`databricks.yml`)
- Development and production targets
- Workspace host configuration
- Variable definitions for catalog/schema
- Permission management

## 📈 Features & Capabilities

### Data Processing
- ✅ Multi-layer medallion architecture
- ✅ SCD Type 2 dimension tracking
- ✅ Data quality validation
- ✅ Schema evolution handling
- ✅ Timestamp-based processing

### Development Experience
- ✅ Comprehensive test suite
- ✅ Modern tooling (UV, pytest, ruff)
- ✅ CI/CD integration (GitHub Actions)
- ✅ Local development support
- ✅ Databricks bundle deployment

### Operational Excellence
- ✅ Serverless execution
- ✅ Automated scheduling
- ✅ Environment separation (dev/prod)
- ✅ Permission management
- ✅ Monitoring-ready structure

## 🚦 CI/CD Pipeline

### GitHub Actions
- **Test Pipeline**: Automated testing on pull requests
- **Deploy Pipeline**: Automated deployment to environments
- **Quality Checks**: Code formatting and linting

### Quality Gates
- ✅ All tests must pass
- ✅ Code coverage requirements
- ✅ Linting standards (ruff)
- ✅ Dependency security scanning

## 📚 Learning Resources

- [Databricks Documentation](https://docs.databricks.com)
- [Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)
- [Python Transformations](https://docs.databricks.com/ldp/developer/python-ref)
- [Bundle Deployment](https://docs.databricks.com/dev-tools/bundles)

## 🆘 Support & Troubleshooting

### Common Issues
1. **Authentication**: Ensure `databricks configure` is completed
2. **Dependencies**: Run `uv sync --dev` after pulling changes
3. **Permissions**: Check workspace permissions for deployment

### Getting Help
- Check existing test cases for implementation examples
- Review transformation patterns in `tests/test_dlt_transformations.py`
- Consult Databricks documentation links above

---

**Built with ❤️ using Databricks, Spark, and modern data engineering practices**