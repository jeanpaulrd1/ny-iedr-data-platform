# NY IEDR Data Platform

> Medallion architecture data lakehouse for New York Interconnection-eligible Distributed Energy Resources (IEDR) using Databricks + Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)

## Overview

This project implements a data pipeline for the NY IEDR program, processing hosting capacity analysis (HCA) and distributed energy resource (DER) data from NY utilities.

## Architecture

📊 **[View Visual Architecture Diagrams](docs/SOLUTION_ARCHITECTURE_DIAGRAM.md)** - Interactive Mermaid diagrams showing end-to-end data flow

📚 **[Read Detailed Architecture Documentation](docs/ARCHITECTURE.md)** - Comprehensive technical documentation

Medallion layers:
* **Bronze**: Raw ingestion with Auto Loader, schema evolution, artifact clearing
* **Silver**: Standardized feeder-level data, MODE aggregation, normalized DER types
* **Gold**: SCD Type 2 historical tables + API-optimized aggregates

**Key Features:**
* **N-Utility Registry Pattern**: Config-driven onboarding, zero pipeline code changes for new utilities
* **Observability**: Freshness monitoring, volume baseline tracking, data quality alerts
* **Production-Ready**: 17 tables across 4 layers, 2,178 feeders, 72,346 DER projects

**Key Technologies:** Lakeflow Spark Declarative Pipelines (SDP), Liquid Clustering, Unity Catalog, Auto Loader, pytest

## Project Structure

```
ny-iedr-data-platform/
├── pipelines/          # DLT pipeline definitions
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_scd2.py
│   └── utils/          # Helper functions (registry, normalization, lineage)
├── config/             # Environment configurations
├── tests/              # Unit and integration tests
├── notebooks/          # Exploratory and testing notebooks
└── docs/               # Documentation
    ├── ARCHITECTURE.md                  # Detailed technical architecture
    ├── SOLUTION_ARCHITECTURE_DIAGRAM.md # Visual diagrams (6 Mermaid charts)
    ├── JOB_ALERT_SETUP.md              # Alert configuration guide
    └── volume_baseline_tracking.sql     # Anomaly detection query
```

## Setup & Installation

### Prerequisites

* **Python 3.10+** (specified in `.python-version`)
* **Databricks Workspace** with Unity Catalog enabled
* **Databricks CLI** (for deployment)
* **Git** (for version control)

### Local Development Setup

```bash
# 1. Clone the repository
git clone https://github.com/your-org/ny-iedr-data-platform.git
cd ny-iedr-data-platform

# 2. Install dependencies
make install-dev
# Or manually:
pip install -r requirements.txt -r requirements-dev.txt
pre-commit install

# 3. Run tests
make test
# Or:
pytest

# 4. Lint and format code
make lint
make format
```

### Databricks Repos Setup

```bash
# 1. Link repo to Databricks Repos (via UI or CLI)
databricks repos create --url https://github.com/your-org/ny-iedr-data-platform.git   --path /Repos/<your-email>/ny-iedr-data-platform

# 2. Validate bundle configuration
make validate
# Or:
databricks bundle validate --target dev

# 3. Deploy pipeline
make deploy
# Or:
databricks bundle deploy --target dev

# 4. Run pipeline
make run
# Or:
databricks bundle run ny_iedr_pipeline --target dev
```

### Dependencies

**Runtime Dependencies** (see `requirements.txt`):
* PySpark, Delta Lake, Databricks SDK - Included in Databricks Runtime
* `dlt` - Included in DLT pipeline runtime
* `great-expectations` - Data quality validation
* `python-dotenv`, `pyyaml` - Configuration utilities

**Development Dependencies** (see `requirements-dev.txt`):
* `pytest`, `pytest-cov`, `chispa` - Testing framework
* `ruff` - Fast linting & formatting (replaces flake8, black, isort)
* `mypy` - Type checking
* `pre-commit` - Git hooks for code quality

### Configuration Files

* **`pyproject.toml`** - Modern Python project configuration (PEP 518)
* **`requirements.txt`** - Runtime dependencies
* **`requirements-dev.txt`** - Development dependencies
* **`.python-version`** - Python version specification (3.10.12)
* **`Makefile`** - Common development tasks
* **`.pre-commit-config.yaml`** - Pre-commit hooks (ruff, mypy, YAML lint)
* **`databricks.yml`** - Declarative Automation Bundle (DABs) configuration

### Makefile Commands

```bash
# Setup
make install          # Install runtime dependencies
make install-dev      # Install all dependencies (runtime + dev)

# Development
make test             # Run all tests with coverage
make test-unit        # Run unit tests only
make lint             # Run linter (ruff)
make lint-fix         # Auto-fix linting issues
make format           # Auto-format code (ruff)
make type-check       # Run type checker (mypy)
make clean            # Remove build artifacts

# Databricks
make validate         # Validate DABs bundle
make deploy           # Deploy to dev environment
make deploy-prod      # Deploy to production
make run              # Run pipeline in dev environment
make check-all        # Run all checks (lint + format + type + test)
```

## Quick Start

### Unity Catalog Setup

```sql
CREATE CATALOG IF NOT EXISTS dev_iedr;
CREATE SCHEMA IF NOT EXISTS dev_iedr.bronze;
CREATE SCHEMA IF NOT EXISTS dev_iedr.silver;
CREATE SCHEMA IF NOT EXISTS dev_iedr.gold;
CREATE VOLUME IF NOT EXISTS dev_iedr.bronze.landing;
CREATE VOLUME IF NOT EXISTS dev_iedr.bronze.metadata;
```

### Upload Data

```bash
# Upload utility CSVs to landing volumes
/Volumes/dev_iedr/bronze/landing/utility1/circuits.csv
/Volumes/dev_iedr/bronze/landing/utility1/der_installed.csv
/Volumes/dev_iedr/bronze/landing/utility1/der_planned.csv
/Volumes/dev_iedr/bronze/landing/utility2/circuits.csv
/Volumes/dev_iedr/bronze/landing/utility2/der_installed.csv
/Volumes/dev_iedr/bronze/landing/utility2/der_planned.csv
```

### Development Workflow

1. Clone repo to Databricks Repos
2. Create feature branch for your work
3. Develop and test on feature branch
4. Run unit tests: `make test` or `pytest tests/`
5. Lint and format: `make lint && make format`
6. Merge to main when stable

## Current Status (2026-07-08)

✅ **Production-Ready**
* All 17 tables validated across Bronze → Silver → Gold → API layers
* Observability features active (freshness monitoring, volume baseline, alerts)
* 2 successful pipeline runs (full refresh + incremental)
* Code refactored and optimized (Silver: -144 lines)

⚠️ **Data Quality Findings**
* Freshness: Both utilities STALE (Oct 2022 data, 1,362-1,376 days old)
* Unresolved Feeders: 210 installed, 919 planned

🚀 **Next Steps**
1. Deploy to `prod_iedr` catalog
2. Schedule pipeline (daily/weekly cadence)
3. Configure Databricks SQL Alerts
4. Set up monitoring dashboards

## Documentation

* **[Solution Architecture Diagrams](docs/SOLUTION_ARCHITECTURE_DIAGRAM.md)** - Visual architecture with 6 Mermaid diagrams
* **[Technical Architecture](docs/ARCHITECTURE.md)** - Detailed design decisions, lessons learned, query patterns
* **[Alert Setup Guide](docs/JOB_ALERT_SETUP.md)** - Email, Slack, PagerDuty configuration
* **[Volume Baseline Tracking](docs/volume_baseline_tracking.sql)** - Anomaly detection query (30-day rolling avg ± 2σ)

---

**License:** Technical assessment project for Senior Data Engineer position
