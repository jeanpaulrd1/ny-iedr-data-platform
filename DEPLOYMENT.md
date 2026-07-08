# Declarative Automation Bundle Deployment Guide

## 🎯 What This Does

Automates deployment of your NY IEDR Data Platform pipeline across dev/prod environments using Infrastructure as Code.

**Benefits:**
* Deploy pipeline with one command: `databricks bundle deploy --target prod`
* Version control all configuration (catalog names, volumes, clusters)
* Multi-environment support (dev → prod promotion)
* CI/CD ready (GitHub Actions, Azure DevOps)
* No manual UI configuration needed

---

## 📋 Prerequisites

1. **Databricks CLI installed** (v0.205.0+):
   ```bash
   pip install databricks-cli --upgrade
   ```

2. **Authenticated to workspace**:
   ```bash
   databricks auth login --host https://dbc-5e6da03e-1613.cloud.databricks.com
   ```

3. **Git repository** (already have this ✓)

---

## 🚀 Quick Start

### Deploy to Development (First Time)

```bash
cd /Workspace/Repos/jeanpaulrd1@gmail.com/ny-iedr-data-platform

# Validate configuration
databricks bundle validate --target dev

# Deploy pipeline (creates/updates DLT pipeline)
databricks bundle deploy --target dev

# Run the pipeline
databricks bundle run ny_iedr_pipeline --target dev
```

**What happens:**
1. Creates DLT pipeline named `dev_iedr_pipeline`
2. Uploads your pipeline code (01-04.py + utils/)
3. Configures serverless compute, ADVANCED edition, SCD2
4. Sets catalog = `dev_iedr`, landing volume = `/Volumes/dev_iedr/bronze/landing`

---

### Deploy to Production

```bash
# First, create prod_iedr catalog if it doesn't exist
# (Do this once in Databricks UI or via SQL)

# Validate production config
databricks bundle validate --target prod

# Deploy to production
databricks bundle deploy --target prod

# Run production pipeline
databricks bundle run ny_iedr_pipeline --target prod
```

**Production differences:**
* Catalog: `prod_iedr` (instead of `dev_iedr`)
* Landing volume: `/Volumes/prod_iedr/bronze/landing`
* Development mode: `false` (optimized for production)
* Notifications: Sends alerts on failures

---

## 📁 File Structure

```
ny-iedr-data-platform/
├── databricks.yml          ← Main bundle config (you just created this)
├── pipelines/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_scd2.py
│   ├── 04_gold_api_views.py
│   └── utils/
│       ├── helpers.py
│       ├── utility_registry.py
│       └── schema_normalization.py
└── .gitignore
```

---

## 🔧 Common Workflows

### Update Pipeline Code
```bash
# 1. Make changes to pipeline code (e.g., add utility3 transformer)
vim pipelines/utils/utility_registry.py

# 2. Test locally (optional)
pytest tests/

# 3. Commit changes
git add .
git commit -m "feat: Add utility3 transformer"
git push

# 4. Deploy to dev
databricks bundle deploy --target dev

# 5. Run and validate
databricks bundle run ny_iedr_pipeline --target dev

# 6. Promote to prod (after validation)
databricks bundle deploy --target prod
```

### Change Configuration (e.g., Enable Notifications)
```bash
# Edit databricks.yml
vim databricks.yml

# Deploy changes
databricks bundle deploy --target prod
```

### Switch Between Environments
```bash
# Deploy to dev
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod

# Default is dev (no --target flag needed)
databricks bundle deploy
```

---

## 🔍 Validation & Troubleshooting

### Validate Configuration
```bash
# Check for syntax errors
databricks bundle validate --target dev

# See what will be deployed (dry-run)
databricks bundle validate --target prod
```

### Check Deployed Pipeline
```bash
# List bundles in workspace
databricks bundle list

# Get pipeline details
databricks pipelines get --pipeline-name dev_iedr_pipeline
```

### Debugging

**Error: "Pipeline already exists"**
* Solution: Bundle will update existing pipeline. This is expected behavior.

**Error: "Catalog prod_iedr does not exist"**
* Solution: Create catalog first:
  ```sql
  CREATE CATALOG IF NOT EXISTS prod_iedr;
  CREATE SCHEMA IF NOT EXISTS prod_iedr.bronze;
  CREATE SCHEMA IF NOT EXISTS prod_iedr.silver;
  CREATE SCHEMA IF NOT EXISTS prod_iedr.gold;
  ```

**Error: "Serverless not enabled"**
* Solution: Change `serverless: true` to `serverless: false` in databricks.yml
* Add cluster configuration:
  ```yaml
  clusters:
    - label: default
      node_type_id: i3.xlarge
      num_workers: 2
  ```

---

## 🤖 CI/CD Integration (Optional)

### GitHub Actions Example

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy Pipeline

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Databricks CLI
        run: pip install databricks-cli
      
      - name: Deploy to Dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks bundle deploy --target dev
          databricks bundle run ny_iedr_pipeline --target dev
      
      # Uncomment for prod deployment on tag
      # - name: Deploy to Prod
      #   if: startsWith(github.ref, 'refs/tags/v')
      #   run: databricks bundle deploy --target prod
```

**Setup:**
1. Add secrets to GitHub repo: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
2. Push to `main` → auto-deploys to dev
3. Create tag `v1.0.0` → auto-deploys to prod

---

## 📊 Benefits for Your N-Utility Architecture

✅ **Onboarding utility3:**
1. Update `utility_registry.py` with utility3 transformers
2. Run `databricks bundle deploy --target dev`
3. Pipeline automatically includes new utility
4. No manual UI configuration

✅ **MODE implementation changes:**
* Code changes tracked in Git
* Deploy atomically (code + config together)
* Rollback via Git: `git revert <commit>` + redeploy

✅ **Multi-environment testing:**
* Test MODE changes in dev before prod
* Separate dev/prod catalogs prevent data contamination
* Identical pipeline logic across environments

---

## 🎓 Next Steps

1. ✅ **You've created databricks.yml** (done)

2. **Validate and deploy to dev:**
   ```bash
   databricks bundle validate --target dev
   databricks bundle deploy --target dev
   ```

3. **Test the deployed pipeline:**
   * Check Databricks UI → Workflows → Pipelines → `dev_iedr_pipeline`
   * Run pipeline manually or via CLI
   * Validate data in `dev_iedr` catalog

4. **Update README.md:**
   * Add deployment instructions
   * Document dev vs prod differences
   * Link to this guide

5. **Set up CI/CD** (optional):
   * Add GitHub Actions workflow
   * Automate deployment on push

---

## 📚 Resources

* [Databricks Bundles Documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)
* [DLT Pipeline Configuration](https://docs.databricks.com/en/delta-live-tables/index.html)
* [CI/CD with Bundles](https://docs.databricks.com/en/dev-tools/bundles/ci-cd.html)

---

## 🔑 Key Commands Cheatsheet

```bash
# Validate configuration
databricks bundle validate --target <dev|prod>

# Deploy (create/update resources)
databricks bundle deploy --target <dev|prod>

# Run pipeline
databricks bundle run ny_iedr_pipeline --target <dev|prod>

# Destroy resources (DANGEROUS)
databricks bundle destroy --target <dev|prod>

# See what's deployed
databricks bundle list
```

---

**Questions?** Check the [ARCHITECTURE.md](./docs/ARCHITECTURE.md) for pipeline details.
