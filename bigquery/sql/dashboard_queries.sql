-- Dashboard-ready queries (Looker Studio: add each as a custom-query data source,
-- or use the view cdc_audit.v_pipeline_health created by infra/gcp/bigquery.tf).

-- Rows processed / failed rows / deletes applied / duration, per day and table
SELECT DATE(started_at) AS run_date, table_name,
       SUM(IFNULL(input_row_count, 0))    AS rows_processed,
       SUM(IFNULL(rejected_row_count, 0)) AS failed_rows,
       SUM(IFNULL(deletes_applied, 0))    AS deletes_applied,
       ROUND(AVG(duration_seconds), 1)    AS avg_duration_seconds,
       COUNTIF(status = 'FAILED')         AS failed_runs
FROM `your-gcp-project-id.cdc_audit.cdc_pipeline_audit`
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY run_date, table_name
ORDER BY run_date DESC, table_name;

-- Latest successful watermark and freshness per table
SELECT table_name,
       MAX(watermark) AS latest_successful_watermark,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(watermark), MINUTE) AS minutes_behind
FROM `your-gcp-project-id.cdc_audit.cdc_pipeline_audit`
WHERE status = 'SUCCEEDED'
  AND started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY table_name;

-- Last 20 runs
SELECT pipeline_run_id, table_name, status, input_row_count, upsert_row_count,
       delete_row_count, rejected_row_count, merged_row_count, deletes_applied,
       duration_seconds, error_message
FROM `your-gcp-project-id.cdc_audit.cdc_pipeline_audit`
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY started_at DESC
LIMIT 20;

-- Consumer view: live rows only (soft-deleted rows hidden)
-- CREATE OR REPLACE VIEW `your-gcp-project-id.analytics.v_orders_current` AS
-- SELECT * EXCEPT (_is_deleted) FROM `your-gcp-project-id.analytics.orders` WHERE NOT _is_deleted;
