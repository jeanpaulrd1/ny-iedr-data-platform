# Job Alert Setup Guide

> Comprehensive guide for configuring Databricks job alerts for the NY IEDR Data Platform

## Overview

This guide covers alert configuration for the NY IEDR data pipeline to monitor:
* **Pipeline failures** - Job execution errors
* **Data quality issues** - Null keys, unresolved feeders, negative capacities
* **Freshness problems** - Stale data (> 30 or 45 days)
* **Volume anomalies** - Unexpected record count changes (±2σ from baseline)

## Alert Severity Levels

| Severity | Trigger Conditions | Notification Channels | Response Time |
|----------|-------------------|----------------------|---------------|
| 🔴 **CRITICAL** | • Pipeline failure<br/>• null_key_count > 0<br/>• days_since_refresh > 45 | Email + Slack + PagerDuty | Immediate |
| 🟠 **HIGH** | • Volume ANOMALY_LOW<br/>• days_since_refresh > 30 | Email + Slack | < 4 hours |
| 🟡 **MEDIUM** | • Volume ANOMALY_HIGH<br/>• unresolved_feeders > 1000 | Email | < 24 hours |
| 🟢 **LOW** | • unresolved_feeders > 500 | Email (weekly digest) | < 1 week |

---

## 1. Pipeline Failure Alerts (CRITICAL)

### Job Configuration

Navigate to **Workflows → Jobs → [Your Pipeline Job]** and configure alerts:

#### Settings:
```yaml
Alert Type: Job Failure
Severity: CRITICAL
Condition: Job run fails
Notification Channels:
  - Email: data-team@company.com, on-call@company.com
  - Slack: #data-pipeline-alerts
  - PagerDuty: DataPlatform-OnCall
```

#### Email Template:
```
Subject: [CRITICAL] NY IEDR Pipeline Failed - {{job_name}}

Pipeline: {{job_name}}
Run ID: {{run_id}}
Status: {{status}}
Error: {{error_message}}

Job URL: {{job_url}}

Action Required:
1. Check pipeline logs for errors
2. Review data quality metrics
3. Notify data engineering team
```

#### Slack Webhook Configuration:
```json
{
  "channel": "#data-pipeline-alerts",
  "username": "IEDR Pipeline Monitor",
  "icon_emoji": ":rotating_light:",
  "attachments": [{
    "color": "danger",
    "title": "🔴 CRITICAL: Pipeline Failed",
    "fields": [
      {"title": "Job", "value": "{{job_name}}", "short": true},
      {"title": "Run ID", "value": "{{run_id}}", "short": true},
      {"title": "Error", "value": "{{error_message}}", "short": false}
    ],
    "actions": [{
      "type": "button",
      "text": "View Job Run",
      "url": "{{job_url}}"
    }]
  }]
}
```

---

## 2. Data Quality Alerts (SQL Alerts)

### 2.1 Null Key Detection (CRITICAL)

Create a **Databricks SQL Alert** that queries the data quality metrics table:

#### Query:
```sql
-- Alert: Null Keys Detected
SELECT 
  utility_id,
  table_name,
  null_key_count,
  ingestion_date,
  pipeline_update_id
FROM dev_iedr.silver.data_quality_metrics_silver
WHERE ingestion_date = CURRENT_DATE
  AND null_key_count > 0
ORDER BY null_key_count DESC;
```

#### Alert Configuration:
```yaml
Name: "IEDR - Null Keys Detected"
Schedule: Run after pipeline job completes
Trigger Condition: Row count > 0
Rearm: 1 hour
Destinations:
  - Email: data-team@company.com
  - Slack: #data-quality-alerts
```

#### Notification Template:
```
🔴 CRITICAL: Null keys detected in NY IEDR data

{{result_count}} tables have null keys.

Details:
{{#results}}
  • {{utility_id}}.{{table_name}}: {{null_key_count}} null keys
{{/results}}

Query: {{query_url}}
Date: {{ingestion_date}}

This indicates data corruption. Immediate investigation required.
```

---

### 2.2 Freshness Monitoring (CRITICAL/HIGH)

#### Query:
```sql
-- Alert: Stale Data Detected
SELECT 
  utility_id,
  table_name,
  last_refresh_date,
  days_since_refresh,
  CASE 
    WHEN days_since_refresh > 45 THEN '🔴 CRITICAL'
    WHEN days_since_refresh > 30 THEN '🟠 HIGH'
    ELSE '✅ OK'
  END as severity,
  ingestion_date
FROM dev_iedr.silver.data_quality_metrics_silver
WHERE ingestion_date = CURRENT_DATE
  AND table_name = 'circuits'  -- Only circuits have refresh dates
  AND days_since_refresh > 30
ORDER BY days_since_refresh DESC;
```

#### Alert Configuration:
```yaml
Name: "IEDR - Stale Data Alert"
Schedule: Daily at 9:00 AM (after pipeline)
Trigger Condition: Row count > 0
Rearm: 24 hours
Destinations:
  - Email: data-team@company.com, utility-liaisons@company.com
  - Slack: #iedr-data-quality
```

#### Notification Template:
```
⚠️ Stale data detected in NY IEDR circuits

{{result_count}} utilities have stale hosting capacity data.

Details:
{{#results}}
  {{severity}} {{utility_id}}: Last refresh {{last_refresh_date}} ({{days_since_refresh}} days ago)
{{/results}}

Thresholds:
  • > 45 days: 🔴 CRITICAL (utility contact required)
  • > 30 days: 🟠 HIGH (proactive outreach)

Action: Contact utility data coordinators to request updated HCA files.
```

---

### 2.3 Volume Baseline Anomaly Detection (HIGH/MEDIUM)

Use the `volume_baseline_tracking.sql` query from the docs folder.

#### Alert Query:
```sql
-- Alert: Volume Anomalies Detected
WITH baseline_stats AS (
  -- (Full query from docs/volume_baseline_tracking.sql)
  -- Abbreviated here for brevity
  SELECT 
    utility_id,
    table_name,
    total_records,
    baseline_avg,
    baseline_stddev,
    lower_bound,
    upper_bound,
    CASE 
      WHEN total_records < lower_bound THEN 'ANOMALY_LOW'
      WHEN total_records > upper_bound THEN 'ANOMALY_HIGH'
      ELSE 'NORMAL'
    END as status
  FROM dev_iedr.silver.data_quality_metrics_silver
  WHERE ingestion_date = CURRENT_DATE
)
SELECT *
FROM baseline_stats
WHERE status IN ('ANOMALY_LOW', 'ANOMALY_HIGH')
  AND baseline_run_count >= 3  -- Require sufficient baseline
ORDER BY 
  CASE status 
    WHEN 'ANOMALY_LOW' THEN 1 
    ELSE 2 
  END,
  utility_id;
```

#### Alert Configuration:
```yaml
Name: "IEDR - Volume Anomaly Detected"
Schedule: Daily at 9:30 AM
Trigger Condition: Row count > 0
Rearm: 24 hours
Destinations:
  - Email: data-team@company.com
  - Slack: #data-quality-alerts
```

#### Notification Template:
```
📊 Volume anomaly detected in NY IEDR data

{{result_count}} tables show unexpected record counts.

Details:
{{#results}}
  • {{utility_id}}.{{table_name}}: {{status}}
    Current: {{total_records}} | Baseline: {{baseline_avg}} (±{{baseline_stddev}})
    Expected range: {{lower_bound}} - {{upper_bound}}
{{/results}}

ANOMALY_LOW: Potential data loss or incomplete file upload
ANOMALY_HIGH: Possible duplicates or unexpected data growth

Action: Review source files and pipeline logs.
```

---

### 2.4 Unresolved Feeder Tracking (MEDIUM)

#### Query:
```sql
-- Alert: High Unresolved Feeder Count
SELECT 
  utility_id,
  table_name,
  unresolved_feeder_count,
  total_records,
  ROUND(100.0 * unresolved_feeder_count / total_records, 1) as unresolved_pct,
  ingestion_date
FROM dev_iedr.silver.data_quality_metrics_silver
WHERE ingestion_date = CURRENT_DATE
  AND table_name LIKE '%der%'
  AND unresolved_feeder_count > 1000
ORDER BY unresolved_pct DESC;
```

#### Alert Configuration:
```yaml
Name: "IEDR - High Unresolved Feeder Count"
Schedule: Weekly on Monday at 10:00 AM
Trigger Condition: Row count > 0
Rearm: 7 days
Destinations:
  - Email: data-team@company.com
```

---

## 3. PagerDuty Integration (CRITICAL Only)

### Setup Steps:

1. **Create PagerDuty Service**
   - Service Name: "NY IEDR Data Platform"
   - Escalation Policy: DataPlatform-OnCall
   - Integration Type: Events API v2

2. **Get Integration Key**
   - Navigate to service → Integrations
   - Copy the Integration Key (32-character string)

3. **Configure in Databricks**
   ```bash
   # Set as Databricks secret
   databricks secrets create-scope --scope pagerduty
   databricks secrets put --scope pagerduty --key iedr-integration-key
   # Paste integration key when prompted
   ```

4. **Job Webhook Configuration**
   ```python
   # In job alert settings, add webhook:
   {
     "url": "https://events.pagerduty.com/v2/enqueue",
     "headers": {
       "Content-Type": "application/json"
     },
     "body": {
       "routing_key": "{{secrets/pagerduty/iedr-integration-key}}",
       "event_action": "trigger",
       "payload": {
         "summary": "NY IEDR Pipeline Failed - {{job_name}}",
         "severity": "critical",
         "source": "databricks",
         "custom_details": {
           "job_name": "{{job_name}}",
           "run_id": "{{run_id}}",
           "error": "{{error_message}}"
         }
       }
     }
   }
   ```

---

## 4. Slack Integration

### Setup Steps:

1. **Create Slack App** (if not exists)
   - Go to https://api.slack.com/apps
   - Create New App → From scratch
   - Name: "IEDR Pipeline Monitor"
   - Workspace: Your workspace

2. **Enable Incoming Webhooks**
   - Navigate to Incoming Webhooks
   - Activate Incoming Webhooks
   - Add New Webhook to Workspace
   - Select channel: #data-pipeline-alerts
   - Copy Webhook URL

3. **Store Webhook in Secrets**
   ```bash
   databricks secrets create-scope --scope slack
   databricks secrets put --scope slack --key iedr-webhook-url
   # Paste webhook URL when prompted
   ```

4. **Configure Job Notifications**
   - In job settings → Notifications
   - Add destination → Webhook
   - URL: `{{secrets/slack/iedr-webhook-url}}`
   - Use webhook payload format above

---

## 5. Email Configuration

### Distribution Lists:

```yaml
data-team@company.com:
  - All data quality alerts
  - Pipeline failures
  - Weekly summaries

on-call@company.com:
  - Critical alerts only (24/7)
  
utility-liaisons@company.com:
  - Freshness alerts (stale data)
  - Quarterly data quality reports
```

### Email Template Best Practices:

1. **Subject Line Format**: `[SEVERITY] Category - Brief Description`
2. **Body Structure**:
   - Summary (1-2 sentences)
   - Affected resources (tables, utilities)
   - Metrics/Details
   - Action items
   - Links to dashboards/queries
3. **Formatting**: Use bullet points, tables, and clear sections

---

## 6. Testing Alerts

### Test Procedure:

1. **Pipeline Failure Test**
   ```python
   # Temporarily break pipeline to test alerts
   # In 02_silver_transformations.py
   raise Exception("TEST ALERT - Pipeline failure simulation")
   ```

2. **SQL Alert Test**
   ```sql
   -- Override condition for testing
   SELECT 'TEST' as utility_id, 
          'circuits' as table_name,
          999 as null_key_count;
   ```

3. **Verify Channels**
   - [ ] Email received (< 5 minutes)
   - [ ] Slack notification appears
   - [ ] PagerDuty incident created (critical only)
   - [ ] Links work correctly
   - [ ] Content is clear and actionable

4. **Cleanup**
   - Remove test code
   - Acknowledge/resolve test incidents
   - Document any issues

---

## 7. Alert Maintenance

### Weekly:
- [ ] Review alert history for false positives
- [ ] Check volume baseline thresholds (adjust if needed)
- [ ] Verify on-call rotation is current

### Monthly:
- [ ] Review unresolved feeder trends
- [ ] Update distribution lists
- [ ] Test critical alert path (PagerDuty)

### Quarterly:
- [ ] Review and update alert thresholds
- [ ] Analyze alert response times
- [ ] Train new team members on alert response

---

## 8. Alert Response Playbook

### Pipeline Failure (CRITICAL)

1. **Immediate Actions** (< 15 minutes)
   - Acknowledge incident in PagerDuty
   - Check pipeline logs in Databricks
   - Identify failing step (Bronze/Silver/Gold)
   - Post initial update in #data-pipeline-alerts

2. **Diagnosis** (< 30 minutes)
   - Review error message and stack trace
   - Check source files in landing volumes
   - Verify compute cluster availability
   - Check for data format changes

3. **Resolution**
   - Fix code or data issues
   - Re-run pipeline
   - Verify data quality metrics
   - Document root cause and fix

### Null Keys (CRITICAL)

1. Check which utility and table affected
2. Query Bronze layer to verify source data integrity
3. Check Silver transformations for mapping errors
4. Contact utility if data issue
5. Consider data quarantine if corruption widespread

### Stale Data (HIGH)

1. Identify affected utilities
2. Check last file upload timestamp in landing volumes
3. Contact utility data coordinator
4. Track outstanding data requests
5. Update stakeholders if delays expected

### Volume Anomaly (HIGH/MEDIUM)

1. Compare current vs. baseline record counts
2. Check for duplicate files in landing volumes
3. Review file tracking table for anomalies
4. Investigate if ANOMALY_LOW (potential data loss)
5. Document if expected growth/reduction

---

## 9. Dashboard Integration

Create a **monitoring dashboard** with links from alerts:

### Recommended Tiles:
1. **Pipeline Run History** (Success/Failure over time)
2. **Data Freshness by Utility** (Days since refresh)
3. **Volume Trends** (30-day rolling avg with bounds)
4. **Unresolved Feeders** (By utility and type)
5. **Alert History** (Count by severity)

### Dashboard Query Examples:

```sql
-- Tile 1: Pipeline Success Rate (Last 30 days)
SELECT 
  DATE(start_time) as run_date,
  COUNT(*) as total_runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate
FROM system.lakeflow.pipeline_events
WHERE pipeline_id = '<your_pipeline_id>'
  AND event_type = 'update'
  AND start_time >= CURRENT_DATE - 30
GROUP BY DATE(start_time)
ORDER BY run_date DESC;
```

---

## 10. Contact Information

| Role | Contact | Responsibilities |
|------|---------|------------------|
| Data Engineering Lead | data-lead@company.com | Pipeline architecture, escalations |
| On-Call Engineer | on-call@company.com | 24/7 critical incident response |
| Utility 1 Liaison | utility1@company.com | Data refresh coordination |
| Utility 2 Liaison | utility2@company.com | Data refresh coordination |
| Product Owner | product@company.com | Business impact decisions |

---

## Appendix A: Alert Configuration Checklist

- [ ] Pipeline failure alerts configured (email + Slack + PagerDuty)
- [ ] Null key SQL alert created and tested
- [ ] Freshness monitoring SQL alert created and tested
- [ ] Volume baseline SQL alert created and tested
- [ ] Unresolved feeder tracking alert created
- [ ] Slack webhook configured and verified
- [ ] PagerDuty integration configured and tested
- [ ] Email distribution lists created and verified
- [ ] Alert response playbook documented and shared
- [ ] Monitoring dashboard created with alert links
- [ ] Team trained on alert response procedures
- [ ] Weekly/monthly maintenance scheduled

---

## Appendix B: Example Alert Queries

All SQL queries referenced in this document can be found in:
- `docs/volume_baseline_tracking.sql` - Volume anomaly detection
- `dev_iedr.silver.data_quality_metrics_silver` - Source table for all DQ alerts

To run ad-hoc queries:
```sql
-- Current data quality snapshot
SELECT * 
FROM dev_iedr.silver.data_quality_metrics_silver 
WHERE ingestion_date = CURRENT_DATE
ORDER BY utility_id, table_name;
```

---

**Last Updated:** 2026-07-08  
**Version:** 1.0  
**Owner:** Data Platform Team
