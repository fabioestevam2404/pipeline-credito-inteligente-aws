# Pipeline de Crédito — AWS

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20Lambda%20%7C%20SageMaker-orange?logo=amazonaws)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple?logo=terraform)
![License](https://img.shields.io/badge/license-MIT-green)
![LGPD](https://img.shields.io/badge/LGPD-Compliant-blue)

Pipeline completo de análise de crédito na AWS — da ingestão de dados ao score em tempo real em **menos de 200ms**, com conformidade total à LGPD e às exigências do Banco Central.

---

## Visão Geral

Este projeto implementa uma plataforma de crédito end-to-end utilizando a **arquitetura Medallion** (Bronze → Silver → Gold), combinando processamento em lote com Glue/PySpark, APIs em tempo real com Lambda, e scoring com SageMaker.

### O que o pipeline entrega

- **Decisão de crédito em <200ms** via API REST
- **Score 0–1000** baseado em 15 features comportamentais e cadastrais
- **Conformidade LGPD** com direito ao esquecimento implementado
- **Trilha de auditoria de 5 anos** conforme exigência do Banco Central
- **Infraestrutura 100% como código** com Terraform

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                              │
│  PostgreSQL CRM          Serasa/SPC API        Kinesis Stream        │
│  (batch diário)          (real-time)           (transações)          │
└────────┬────────────────────────┬──────────────────────┬────────────┘
         │                        │                      │
         ▼                        ▼                      ▼
┌────────────────┐    ┌───────────────────┐   ┌──────────────────────┐
│  Glue Job      │    │ Lambda            │   │ Lambda               │
│  ingest_crm    │    │ bureau_query      │   │ kinesis_consumer     │
│  (Bronze)      │    │ → S3 audit trail  │   │ → S3 particionado    │
└───────┬────────┘    └────────┬──────────┘   └──────────┬───────────┘
        │                      │                         │
        ▼                      └─────────┬───────────────┘
┌───────────────────────────────────────▼──────────────────────────┐
│  S3 Raw (Bronze)     Parquet particionado por data               │
│  Retenção: 90 dias   KMS encrypted   Versioning enabled          │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  Glue Job: silver_processing                                     │
│  • Validação de qualidade (descarte máx. 20%)                   │
│  • Deduplicação por CPF (window function)                        │
│  • Features: faixa_etaria, faixa_renda_log, dias_relacionamento  │
│  • UPSERT via Apache Iceberg                                     │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  S3 Silver (Iceberg)   Glue Catalog: credit_db.clientes_silver   │
│  Retenção: 2 anos      ACID compliance   Schema evolution        │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  Glue Job: feature_engineering                                   │
│  • Join: CRM + Bureau + Transações (90 dias)                     │
│  • 19 features calculadas para o modelo ML                       │
│  • Normalização e tratamento conservador de nulos               │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  S3 Gold + SageMaker Feature Store                               │
│  Online Store (<10ms lookup)   Offline Store (histórico S3)      │
│  Retenção: 5 anos              Glue Catalog integrado            │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                       API Gateway POST /score
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  Lambda: credit_score                                            │
│  1. Busca features no Feature Store Online                       │
│  2. Invoca SageMaker endpoint (15 features)                      │
│  3. Converte probabilidade → score 0–1000                        │
│  4. Aplica política: APROVADO / ANALISE_MANUAL / NEGADO          │
│  5. Grava auditoria no DynamoDB (TTL 5 anos)                     │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
              { "score": 750, "decision": "APROVADO" }
                         SLA: P99 < 200ms
```

---

## Stack de Tecnologias

| Camada | Tecnologia | Finalidade |
|---|---|---|
| **Ingestão batch** | AWS Glue + PySpark | ETL do CRM PostgreSQL |
| **Streaming** | AWS Kinesis + Lambda | Transações em tempo real |
| **Armazenamento** | S3 + Apache Iceberg | Data lake com ACID |
| **Feature Store** | SageMaker Feature Store | Features online/offline |
| **Scoring** | SageMaker Endpoint + Lambda | Inferência ML <200ms |
| **API** | API Gateway | Endpoint REST público |
| **Auditoria** | DynamoDB | Trilha de decisões 5 anos |
| **Criptografia** | AWS KMS | Chaves gerenciadas pelo cliente |
| **Segredos** | AWS Secrets Manager | Credenciais sem hardcode |
| **Orquestração** | AWS Step Functions | State machine do pipeline |
| **Monitoramento** | CloudWatch | Métricas, alarmes, dashboards |
| **IaC** | Terraform | Infraestrutura versionada |

---

## Estrutura do Projeto

```
Pipeline de Crédito/
├── config/
│   ├── dev.yaml                  # Thresholds relaxados para desenvolvimento
│   └── prod.yaml                 # Configurações de produção (cron, SLAs, retenção)
├── glue_jobs/
│   ├── ingest_crm.py             # Bronze: ingestão incremental do PostgreSQL
│   ├── silver_processing.py      # Silver: limpeza, dedup e Iceberg UPSERT
│   └── feature_engineering.py    # Gold: 19 features para o modelo ML
├── lambda_functions/
│   ├── bureau_query/             # Consulta Serasa/SPC com fallback conservador
│   ├── credit_score/             # Scoring em tempo real (<200ms)
│   └── kinesis_consumer/         # Ingestão de transações em stream
├── sagemaker/
│   └── feature_store/
│       ├── setup_feature_group.py  # Setup único do Feature Group
│       └── ingest_features.py      # Ingestão diária Gold → Feature Store
├── src/
│   ├── compliance/
│   │   └── lgpd_data_deletion.py   # Direito ao esquecimento (LGPD Art. 18)
│   └── utils/
│       └── aws_helpers.py          # Clientes AWS cacheados, retry, métricas
├── scripts/
│   ├── bootstrap.sh              # Setup inicial da conta AWS
│   ├── deploy.sh                 # Deploy por componente
│   └── lgpd_deletion.py          # CLI para exclusão LGPD
├── terraform/
│   ├── envs/prod/                # Orquestração do ambiente de produção
│   └── modules/                  # Módulos: s3, iam, kms, glue, lambda,
│       │                         #          step_functions, monitoring
├── tests/
│   └── test_scoring.py           # Testes unitários e de integração
├── requirements.txt
└── LICENSE
```

---

## Política de Crédito

| Score | Decisão | Critério |
|---|---|---|
| 700 – 1000 | **APROVADO** | Baixo risco de inadimplência |
| 500 – 699 | **ANALISE_MANUAL** | Risco moderado, requer revisão |
| 0 – 499 | **NEGADO** | Alto risco de inadimplência |

---

## Deploy

### Pré-requisitos

- AWS CLI configurado (`aws configure`)
- Terraform >= 1.5
- Python 3.11+
- Permissões IAM: S3, Glue, Lambda, SageMaker, DynamoDB, KMS, Secrets Manager

### Passo a passo

```bash
# 1. Setup inicial (executa uma vez por conta AWS)
./scripts/bootstrap.sh

# 2. Atualizar segredos no Secrets Manager
aws secretsmanager put-secret-value \
  --secret-id credit-pipeline/crm-db \
  --secret-string '{"host":"...","port":"5432","database":"crm","username":"...","password":"..."}'

aws secretsmanager put-secret-value \
  --secret-id credit-pipeline/bureau-api \
  --secret-string '{"client_id":"...","client_secret":"...","base_url":"https://api.serasa.com.br"}'

# 3. Provisionar infraestrutura
cd terraform/envs/prod
cp prod.tfvars.example prod.tfvars   # edite com seus valores
terraform init
terraform plan
terraform apply

# 4. Deploy dos jobs e funções
./scripts/deploy.sh --component glue
./scripts/deploy.sh --component lambda
./scripts/deploy.sh --component sagemaker

# 5. Validar instalação
./scripts/deploy.sh --validate
```

### Deploy de componente único

```bash
./scripts/deploy.sh --component infra      # Terraform
./scripts/deploy.sh --component glue       # Glue jobs
./scripts/deploy.sh --component lambda     # Lambda functions
./scripts/deploy.sh --component sagemaker  # SageMaker scripts
```

---

## Uso da API

### Solicitar score de crédito

```bash
curl -X POST https://<api-id>.execute-api.us-east-1.amazonaws.com/prod/score \
  -H "Content-Type: application/json" \
  -d '{
    "application_id": "APP-2026-001",
    "cpf_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  }'
```

**Resposta (< 200ms):**

```json
{
  "application_id": "APP-2026-001",
  "score": 750,
  "decision": "APROVADO",
  "latency_ms": 87
}
```

### Execução manual do pipeline batch

```bash
./scripts/trigger_pipeline.sh
```

---

## Conformidade LGPD

Implementa o **Artigo 18, Inciso VI** da LGPD (direito ao esquecimento):

```bash
# Simular exclusão (sem deletar)
python scripts/lgpd_deletion.py \
  --cpf-hash <SHA256_DO_CPF> \
  --request-id REQ-2026-001 \
  --requester "João Silva - Protocolo #001" \
  --dry-run

# Executar exclusão real
python scripts/lgpd_deletion.py \
  --cpf-hash <SHA256_DO_CPF> \
  --request-id REQ-2026-001 \
  --requester "João Silva - Protocolo #001"
```

A exclusão cobre: Feature Store Online, DynamoDB (anonimização), S3 Gold/Silver/Raw. Toda operação gera evidência imutável no bucket de compliance.

---

## Testes

```bash
pip install -r requirements.txt

# Testes unitários
pytest tests/ -v

# Testes de integração (requer conta AWS configurada)
pytest tests/ -v -m integration
```

---

## Segurança

- **CPF nunca em texto plano** — pseudoanonimizado com SHA-256
- **Zero credenciais hardcoded** — 100% via AWS Secrets Manager
- **Criptografia em repouso** — KMS com chaves gerenciadas pelo cliente em todos os buckets S3 e DynamoDB
- **S3 Object Lock (COMPLIANCE mode)** — logs de auditoria imutáveis por 5 anos
- **Acesso público bloqueado** em todos os buckets S3
- **IAM least privilege** — roles separadas por serviço (Glue, Lambda, StepFunctions, SageMaker)

---

## SLAs de Performance

| Operação | Target | Medição |
|---|---|---|
| Scoring em tempo real | P99 < 200ms | CloudWatch `ScoringLatency` |
| Feature Store lookup | < 10ms | Incluído no budget de 200ms |
| Pipeline batch completo | < 3h | Step Functions execution time |
| Taxa de erro da API | < 5% | CloudWatch `ErrorRate` |

---

## License

Distribuído sob a licença MIT. Veja [LICENSE](LICENSE) para mais detalhes.
