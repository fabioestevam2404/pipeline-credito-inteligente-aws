variable "aws_region"        { default = "us-east-1" }
variable "environment"       { default = "prod" }
variable "alert_email"       { description = "Email para alertas do pipeline" }
variable "crm_host"          { description = "Hostname do banco CRM" }
variable "crm_database"      { default = "credit_db" }
variable "private_subnet_id" { description = "Subnet privada para Glue Connection" }
