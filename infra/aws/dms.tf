resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_id          = "${local.name}-dms"
  replication_subnet_group_description = "${local.name} DMS private subnets"
  subnet_ids                           = aws_subnet.private[*].id

  depends_on = [aws_iam_role_policy_attachment.dms_vpc]
}

resource "aws_dms_replication_instance" "this" {
  replication_instance_id     = "${local.name}-dms"
  replication_instance_class  = var.dms_instance_class
  allocated_storage           = var.dms_allocated_storage_gb
  engine_version              = var.dms_engine_version
  multi_az                    = var.dms_multi_az
  publicly_accessible         = false
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id
  vpc_security_group_ids      = [aws_security_group.dms.id]
  kms_key_arn                 = aws_kms_key.data.arn
  auto_minor_version_upgrade  = true
  apply_immediately           = var.env != "prod"
}

resource "aws_dms_endpoint" "source" {
  endpoint_id                     = "${local.name}-source-pg"
  endpoint_type                   = "source"
  engine_name                     = "postgres"
  database_name                   = var.db_name
  secrets_manager_arn             = aws_secretsmanager_secret.dms_source.arn
  secrets_manager_access_role_arn = aws_iam_role.dms_secrets.arn
  ssl_mode                        = "require"

  postgres_settings {
    slot_name           = "dms_cdc_slot" # created by source_db/init/03_enable_cdc.sql
    plugin_name         = "test_decoding"
    heartbeat_enable    = true # keeps the slot advancing on idle databases
    heartbeat_frequency = 5
    capture_ddls        = false
  }
}

resource "aws_dms_s3_endpoint" "target" {
  endpoint_id             = "${local.name}-target-s3"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.data["raw"].id
  bucket_folder           = local.raw_prefix
  service_access_role_arn = aws_iam_role.dms_s3.arn

  data_format                       = "parquet"
  parquet_version                   = "parquet-2-0"
  compression_type                  = "GZIP"
  encryption_mode                   = "SSE_KMS"
  server_side_encryption_kms_key_id = aws_kms_key.data.arn

  include_op_for_full_load = true             # Op = I on full-load rows
  cdc_inserts_and_updates  = false            # keep deletes
  timestamp_column_name    = "_dms_ingest_ts" # load time for full load, commit time for CDC
  add_column_name          = true

  date_partition_enabled   = true # cdc/<schema>/<table>/YYYY/MM/DD/
  date_partition_sequence  = "YYYYMMDD"
  date_partition_delimiter = "SLASH"

  cdc_max_batch_interval = var.dms_cdc_max_batch_interval_seconds
  cdc_min_file_size      = 32000 # KB — fewer, larger files

  depends_on = [aws_iam_role_policy.dms_s3]
}

locals {
  dms_task = jsondecode(file("${local.repo_root}/dms/dms_replication_task.json"))
}

resource "aws_dms_replication_task" "cdc" {
  replication_task_id       = "${local.name}-full-load-and-cdc"
  migration_type            = local.dms_task.MigrationType
  replication_instance_arn  = aws_dms_replication_instance.this.replication_instance_arn
  source_endpoint_arn       = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn       = aws_dms_s3_endpoint.target.endpoint_arn
  table_mappings            = file("${local.repo_root}/dms/table_mappings.json")
  replication_task_settings = jsonencode(local.dms_task.ReplicationTaskSettings)
  start_replication_task    = var.dms_start_task

  lifecycle {
    ignore_changes = [replication_task_settings] # DMS adds defaults => perpetual diff
  }
}
