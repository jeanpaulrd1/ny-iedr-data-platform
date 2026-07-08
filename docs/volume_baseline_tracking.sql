-- Volume Baseline Tracking & Anomaly Detection
-- Purpose: Monitor record count deviations from 30-day rolling baseline
-- Alert on: ANOMALY_LOW (potential data loss), ANOMALY_HIGH (duplication/drift)
-- Baseline: Mean ± 2 standard deviations (95% confidence interval)

WITH baseline_stats AS (
  SELECT 
    utility_id,
    table_name,
    -- 30-day rolling baseline (exclude current run)
    AVG(total_records) OVER (
      PARTITION BY utility_id, table_name 
      ORDER BY ingestion_date 
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) as baseline_avg,
    STDDEV(total_records) OVER (
      PARTITION BY utility_id, table_name 
      ORDER BY ingestion_date 
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) as baseline_stddev,
    -- Count of historical runs for baseline validation
    COUNT(*) OVER (
      PARTITION BY utility_id, table_name 
      ORDER BY ingestion_date 
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) as baseline_run_count,
    total_records,
    ingestion_date,
    pipeline_update_id
  FROM dev_iedr.silver.data_quality_metrics_silver
  WHERE ingestion_date >= CURRENT_DATE - 31  -- Include 31 days for baseline calculation
),

current_run AS (
  SELECT 
    utility_id,
    table_name,
    baseline_avg,
    baseline_stddev,
    baseline_run_count,
    total_records as current_records,
    ingestion_date,
    pipeline_update_id,
    -- Calculate deviation from baseline
    CASE 
      WHEN baseline_avg IS NOT NULL AND baseline_stddev IS NOT NULL 
      THEN (total_records - baseline_avg) / NULLIF(baseline_stddev, 0)
      ELSE NULL 
    END as z_score,
    -- Lower and upper bounds (mean ± 2σ)
    CASE 
      WHEN baseline_avg IS NOT NULL AND baseline_stddev IS NOT NULL 
      THEN baseline_avg - (2 * baseline_stddev)
      ELSE NULL 
    END as lower_bound,
    CASE 
      WHEN baseline_avg IS NOT NULL AND baseline_stddev IS NOT NULL 
      THEN baseline_avg + (2 * baseline_stddev)
      ELSE NULL 
    END as upper_bound
  FROM baseline_stats
  WHERE ingestion_date = CURRENT_DATE  -- Only current run
)

SELECT 
  utility_id,
  table_name,
  current_records,
  ROUND(baseline_avg, 0) as baseline_avg,
  ROUND(baseline_stddev, 2) as baseline_stddev,
  ROUND(lower_bound, 0) as lower_bound_2sigma,
  ROUND(upper_bound, 0) as upper_bound_2sigma,
  ROUND(z_score, 2) as z_score,
  baseline_run_count,
  -- Anomaly detection status
  CASE 
    WHEN baseline_run_count < 3 THEN 'INSUFFICIENT_BASELINE'
    WHEN current_records < lower_bound THEN 'ANOMALY_LOW'
    WHEN current_records > upper_bound THEN 'ANOMALY_HIGH'
    ELSE 'NORMAL'
  END as status,
  -- Human-readable explanation
  CASE 
    WHEN baseline_run_count < 3 THEN 
      'Need at least 3 historical runs to establish baseline'
    WHEN current_records < lower_bound THEN 
      CONCAT('⚠️  Record count below expected range (', 
             ROUND(((baseline_avg - current_records) / baseline_avg) * 100, 1), 
             '% below baseline)')
    WHEN current_records > upper_bound THEN 
      CONCAT('⚠️  Record count above expected range (', 
             ROUND(((current_records - baseline_avg) / baseline_avg) * 100, 1), 
             '% above baseline)')
    ELSE 
      '✅ Within normal range'
  END as explanation,
  ingestion_date,
  pipeline_update_id
FROM current_run
ORDER BY 
  CASE 
    WHEN baseline_run_count < 3 THEN 1
    WHEN current_records < lower_bound THEN 2
    WHEN current_records > upper_bound THEN 3
    ELSE 4
  END,
  utility_id, 
  table_name;

-- Example Alert Query: Flag critical anomalies for notification
-- Use this in a Databricks SQL Alert with threshold: count > 0

/*
SELECT 
  COUNT(*) as critical_anomaly_count,
  CONCAT_WS(', ', COLLECT_LIST(CONCAT(utility_id, '.', table_name))) as affected_tables
FROM (
  -- Reuse baseline query above
  SELECT utility_id, table_name, status
  FROM current_run
  WHERE status IN ('ANOMALY_LOW', 'ANOMALY_HIGH')
) anomalies;
*/

-- Usage Notes:
-- 1. Run this query daily after pipeline completion
-- 2. INSUFFICIENT_BASELINE: Normal for first 3 runs, ignore
-- 3. ANOMALY_LOW: Investigate potential data loss or source issues
-- 4. ANOMALY_HIGH: Check for duplicates or unexpected data growth
-- 5. Z-score interpretation:
--    * |z| < 2.0: Normal variation (95% confidence)
--    * |z| >= 2.0: Significant deviation (alert)
--    * |z| >= 3.0: Extreme deviation (critical alert)

-- Integration with JOB_ALERT_SETUP.md:
-- Configure Databricks SQL Alert on this query with:
--   Condition: WHERE status IN ('ANOMALY_LOW', 'ANOMALY_HIGH')
--   Frequency: Daily after pipeline job
--   Destination: Slack #data-quality channel