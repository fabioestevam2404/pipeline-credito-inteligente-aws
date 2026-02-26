# ============================================================
# terraform/environments/prod/variables.tf
# ============================================================

variable "aws_region"   { default = "us-east-1" }
variable "account_id"   { description = "AWS Account ID (12 dígitos)" }
variable "environment"  { default = "prod" }
variable "alert_email"  { description = "Email para alertas do pipeline" }

# Rede
variable "private_subnet_id"       { description = "Subnet privada para o Glue Connection" }
variable "glue_security_group_id"  { description = "Security Group para o Glue" }

# CRM
variable "crm_host"     { description = "Host do banco CRM" }
variable "crm_username" { default = "glue_reader" }
variable "crm_password" { sensitive = true }
