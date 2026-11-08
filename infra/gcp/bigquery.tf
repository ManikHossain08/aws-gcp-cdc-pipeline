# Datasets are owned by Terraform. Tables are created from config/pipeline_config.yaml by
# bigquery/scripts/deploy_bigquery.py --apply (single owner of table DDL, generated from the
# same contract the Glue job uses — see ARCHITECTURE.md §Schema ownership).
resource "google_bigquery_dataset" "this" {
  for_each = {
    staging = var.staging_dataset
    final   = var.final_dataset
    audit   = var.audit_dataset
  }
  dataset_id                 = each.value
  location                   = var.bq_location
  description                = "AWS RDS CDC pipeline — ${each.key}"
  delete_contents_on_destroy = var.env == "dev"

  # Staging tables are truncated per load; a default expiration is NOT set so the
  # pre-created staging schema survives between runs.

  depends_on = [google_project_service.apis]
}

# Dashboard-ready view over the audit table (Looker Studio data source).
resource "google_bigquery_table" "pipeline_health" {
  dataset_id          = google_bigquery_dataset.this["audit"].dataset_id
  table_id            = "v_pipeline_health"
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT
        DATE(started_at)                          AS run_date,
        table_name,
        COUNT(DISTINCT pipeline_run_id)           AS runs,
        COUNTIF(status = 'FAILED')                AS failed_runs,
        SUM(IFNULL(input_row_count, 0))           AS rows_processed,
        SUM(IFNULL(rejected_row_count, 0))        AS failed_rows,
        SUM(IFNULL(deletes_applied, 0))           AS deletes_applied,
        AVG(duration_seconds)                     AS avg_duration_seconds,
        MAX(IF(status = 'SUCCEEDED', watermark, NULL)) AS latest_successful_watermark
      FROM `${var.gcp_project_id}.${var.audit_dataset}.cdc_pipeline_audit`
      WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
      GROUP BY run_date, table_name
    SQL
  }

  # The audit table itself is created by deploy_bigquery.py; apply that first.
  depends_on = [google_bigquery_dataset.this]
}
