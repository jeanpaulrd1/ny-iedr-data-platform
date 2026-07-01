# NY IEDR Data Platform

> Medallion architecture data lakehouse for New York Interconnection-eligible Distributed Energy Resources (IEDR) using Databricks + Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)

## Overview

This project implements a data pipeline for the NY IEDR program, processing hosting capacity analysis (HCA) and distributed energy resource (DER) data from NY utilities.

## Architecture

Medallion layers:
* **Bronze**: Raw ingestion with Auto Loader, schema evolution
* **Silver**: Standardized feeder-level data, normalized DER types
* **Gold**: SCD Type 2 historical tables + API-optimized aggregates

**Key Technologies:** Delta Live Tables, Liquid Clustering, Unity Catalog, Auto Loader

## Project Structure

```
ny-iedr-data-platform/
├── pipelines/          # DLT pipeline definitions
│   └── utils/          # Helper functions
├── config/             # Environment configurations
├── tests/              # Unit and integration tests
├── notebooks/          # Exploratory and testing notebooks
└── docs/               # Documentation
```

## Quick Start

### Unity Catalog Setup

```sql
CREATE CATALOG IF NOT EXISTS dev_iedr;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE VOLUME IF NOT EXISTS bronze.landing;
```

### Development Workflow

1. Clone repo to Databricks Repos
2. Create feature branch for your work
3. Develop and test on feature branch
4. Merge to main when stable

---

**License:** Technical assessment project for Senior Data Engineer position
