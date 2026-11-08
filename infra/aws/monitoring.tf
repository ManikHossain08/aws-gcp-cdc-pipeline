resource "aws_sns_topic" "alerts" {
  name              = "${local.name}-cdc-alerts"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.alerts.arn
      Principal = { Service = ["events.amazonaws.com", "cloudwatch.amazonaws.com"] }
    }]
  })
}

locals {
  dms_task_resource_id = element(split(":", aws_dms_replication_task.cdc.replication_task_arn), 6)
  alarms = {
    dms_cdc_latency_source = {
      namespace = "AWS/DMS", metric = "CDCLatencySource", threshold = 600, stat = "Maximum"
      dims = { ReplicationInstanceIdentifier = aws_dms_replication_instance.this.replication_instance_id,
      ReplicationTaskIdentifier = local.dms_task_resource_id }
      desc = "DMS reads changes from RDS slowly (OPERATIONS.md#dms-latency)"
    }
    dms_cdc_latency_target = {
      namespace = "AWS/DMS", metric = "CDCLatencyTarget", threshold = 900, stat = "Maximum"
      dims = { ReplicationInstanceIdentifier = aws_dms_replication_instance.this.replication_instance_id,
      ReplicationTaskIdentifier = local.dms_task_resource_id }
      desc = "DMS writes to S3 slowly (OPERATIONS.md#dms-latency)"
    }
    rds_replication_slot_lag = {
      namespace = "AWS/RDS", metric = "OldestReplicationSlotLag", threshold = var.replication_slot_lag_alarm_bytes, stat = "Maximum"
      dims      = { DBInstanceIdentifier = aws_db_instance.source.identifier }
      desc      = "WAL retained by the DMS slot is growing (OPERATIONS.md#replication-slot-lag)"
    }
    rds_transaction_logs_disk = {
      namespace = "AWS/RDS", metric = "TransactionLogsDiskUsage", threshold = var.replication_slot_lag_alarm_bytes * 2, stat = "Maximum"
      dims      = { DBInstanceIdentifier = aws_db_instance.source.identifier }
      desc      = "WAL disk usage high — risk of filling RDS storage"
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "this" {
  for_each            = local.alarms
  alarm_name          = "${local.name}-${replace(each.key, "_", "-")}"
  alarm_description   = each.value.desc
  namespace           = each.value.namespace
  metric_name         = each.value.metric
  statistic           = each.value.stat
  period              = 300
  evaluation_periods  = 3
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = each.value.dims
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_event_rule" "glue_failed" {
  name        = "${local.name}-glue-job-failed"
  description = "CDC Glue jobs FAILED / TIMEOUT / ERROR"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.cdc_transform.name, aws_glue_job.transfer.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}

resource "aws_cloudwatch_event_rule" "dms_task_state" {
  name        = "${local.name}-dms-task-state"
  description = "DMS replication task failures"
  event_pattern = jsonencode({
    source        = ["aws.dms"]
    "detail-type" = ["DMS Replication Task State Change"]
    resources     = [aws_dms_replication_task.cdc.replication_task_arn]
  })
}

resource "aws_cloudwatch_event_target" "dms_task_state" {
  rule = aws_cloudwatch_event_rule.dms_task_state.name
  arn  = aws_sns_topic.alerts.arn
}

# Optional file-arrival notifications (raw ObjectCreated) -> CloudWatch Logs.
resource "aws_cloudwatch_log_group" "file_arrivals" {
  count             = var.enable_file_arrival_events ? 1 : 0
  name              = "/aws/events/${local.name}-raw-file-arrivals"
  retention_in_days = 14
}

resource "aws_cloudwatch_event_rule" "file_arrivals" {
  count       = var.enable_file_arrival_events ? 1 : 0
  name        = "${local.name}-raw-file-arrival"
  description = "New DMS file in s3://<raw>/cdc/"
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.data["raw"].id] }
      object = { key = [{ prefix = "${local.raw_prefix}/" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "file_arrivals" {
  count = var.enable_file_arrival_events ? 1 : 0
  rule  = aws_cloudwatch_event_rule.file_arrivals[0].name
  arn   = aws_cloudwatch_log_group.file_arrivals[0].arn
}

resource "aws_cloudwatch_log_resource_policy" "file_arrivals" {
  count       = var.enable_file_arrival_events ? 1 : 0
  policy_name = "${local.name}-eventbridge-to-logs"
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = ["logs:CreateLogStream", "logs:PutLogEvents"]
      Principal = { Service = ["events.amazonaws.com", "delivery.logs.amazonaws.com"] }
      Resource  = "${aws_cloudwatch_log_group.file_arrivals[0].arn}:*"
    }]
  })
}
