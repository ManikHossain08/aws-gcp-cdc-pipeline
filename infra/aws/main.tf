data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name       = "${var.project}-${var.env}"
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  repo_root  = "${path.module}/../.."

  tags = {
    project    = var.project
    env        = var.env
    managed_by = "terraform"
    pipeline   = "aws_rds_cdc_to_bigquery"
  }

  raw_prefix = "cdc"
}

# ------------------------------------------------------------------ VPC (private only)
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${local.name}-vpc" }
}

resource "aws_subnet" "private" {
  count                   = length(var.private_subnet_cidrs)
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.private_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false
  tags                    = { Name = "${local.name}-private-${count.index}" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-private" }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# No NAT / internet gateway: RDS and DMS reach AWS services through VPC endpoints only.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = { Name = "${local.name}-s3-endpoint" }
}

resource "aws_vpc_endpoint" "interface" {
  for_each            = toset(["secretsmanager", "kms", "logs", "monitoring"])
  vpc_id              = aws_vpc.this.id
  service_name        = "com.amazonaws.${var.aws_region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${local.name}-${each.key}-endpoint" }
}

# ------------------------------------------------------------------ security groups
resource "aws_security_group" "rds" {
  name        = "${local.name}-rds"
  description = "RDS PostgreSQL: ingress from DMS only"
  vpc_id      = aws_vpc.this.id
}

resource "aws_security_group" "dms" {
  name        = "${local.name}-dms"
  description = "DMS replication instance"
  vpc_id      = aws_vpc.this.id
}

resource "aws_security_group" "endpoints" {
  name        = "${local.name}-vpce"
  description = "Interface VPC endpoints: HTTPS from inside the VPC"
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_dms" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.dms.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "PostgreSQL from DMS"
}

resource "aws_vpc_security_group_egress_rule" "dms_to_rds" {
  security_group_id            = aws_security_group.dms.id
  referenced_security_group_id = aws_security_group.rds.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "DMS to PostgreSQL"
}

resource "aws_vpc_security_group_egress_rule" "dms_https" {
  security_group_id = aws_security_group.dms.id
  cidr_ipv4         = "0.0.0.0/0" # resolves only to VPC endpoints (no IGW/NAT exists)
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "S3 / Secrets Manager / KMS via VPC endpoints"
}

resource "aws_vpc_security_group_ingress_rule" "endpoints_https" {
  security_group_id = aws_security_group.endpoints.id
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "HTTPS from the VPC"
}
