module metabase_rds_1 {
  source = "../../modules/rds"
  rds_identifier = "metabase-db-${var.environment}-1"
  rds_name = "metabase"
  vpc_security_group_ids = var.rds_security_groups
  subnet_ids = var.subnet_ids
  tags = local.default_tags
}