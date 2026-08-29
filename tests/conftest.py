"""
tests/conftest.py
Garante que os módulos sob teste (que criam clientes boto3 no nível do
módulo, ex. src/scoring/lambda_credit_score.py) importem com sucesso mesmo
sem credenciais/região AWS reais configuradas no ambiente — necessário para
rodar em CI/containers limpos, não só em máquinas com AWS CLI já configurado.
Precisa rodar antes da coleta dos testes, por isso fica no nível do módulo.
"""
import os

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
