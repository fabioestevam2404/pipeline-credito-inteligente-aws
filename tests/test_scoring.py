"""
tests/test_scoring.py
Testes unitários para o Lambda de scoring de crédito.

Execução: pytest tests/ -v
"""
import json
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime


# ── Fixtures ─────────────────────────────────────────────────
@pytest.fixture
def valid_cpf_hash():
    """SHA-256 válido de um CPF de teste."""
    import hashlib
    return hashlib.sha256("12345678901".encode()).hexdigest()


@pytest.fixture
def valid_event(valid_cpf_hash):
    """Evento válido para o Lambda de scoring."""
    return {
        "application_id": "APP-2025-TEST-001",
        "cpf_hash":        valid_cpf_hash,
    }


@pytest.fixture
def sample_features():
    """Features simuladas do Feature Store."""
    return {
        "faixa_renda_log":       "3",
        "renda_padronizada":     "5000.0",
        "dias_relacionamento":   "730",
        "anos_relacionamento":   "2.0",
        "cliente_antigo":        "1",
        "limite_aprovado":       "10000.0",
        "qtd_contratos_ativos":  "2",
        "score_externo_norm":    "0.72",
        "score_risco_bureau":    "2",
        "num_restricoes":        "0",
        "divida_total":          "0.0",
        "flag_cheque":           "0",
        "meses_negativado":      "0",
        "flag_inadimplente":     "0",
        "qtd_tx_30d":            "15",
        "volume_tx_30d":         "2500.0",
        "ticket_medio_30d":      "166.67",
        "ratio_credito_30d":     "0.6",
        "qtd_tx_90d":            "42",
        "volume_tx_90d":         "7200.0",
        "ticket_medio_90d":      "171.43",
        "ratio_credito_90d":     "0.57",
        "qtd_tx_180d":           "80",
        "volume_tx_180d":        "13500.0",
        "ratio_credito_180d":    "0.55",
        "utilizacao_limite":     "0.25",
        "tendencia_volume":      "1.04",
    }


# ── Testes: build_feature_vector ─────────────────────────────
class TestBuildFeatureVector:
    def test_retorna_vetor_com_ordem_correta(self, sample_features):
        from src.scoring.lambda_credit_score import build_feature_vector, FEATURE_ORDER
        vector = build_feature_vector(sample_features)
        assert len(vector) == len(FEATURE_ORDER)
        assert all(isinstance(v, float) for v in vector)

    def test_features_ausentes_viram_zero(self):
        from src.scoring.lambda_credit_score import build_feature_vector
        vector = build_feature_vector({})  # Dicionário vazio
        assert all(v == 0.0 for v in vector)

    def test_converte_string_para_float(self, sample_features):
        from src.scoring.lambda_credit_score import build_feature_vector
        vector = build_feature_vector(sample_features)
        # score_externo_norm = 0.72
        idx = __import__("src.scoring.lambda_credit_score",
                         fromlist=["FEATURE_ORDER"]).FEATURE_ORDER.index("score_externo_norm")
        assert abs(vector[idx] - 0.72) < 0.001


# ── Testes: apply_credit_policy ───────────────────────────────
class TestApplyCreditPolicy:
    def test_score_alto_aprovado(self):
        from src.scoring.lambda_credit_score import apply_credit_policy
        result = apply_credit_policy(750)
        assert result["decision"] == "APROVADO"
        assert result["score_band"] == "ALTO"

    def test_score_medio_analise_manual(self):
        from src.scoring.lambda_credit_score import apply_credit_policy
        result = apply_credit_policy(600)
        assert result["decision"] == "ANALISE_MANUAL"
        assert result["score_band"] == "MEDIO"

    def test_score_baixo_negado(self):
        from src.scoring.lambda_credit_score import apply_credit_policy
        result = apply_credit_policy(300)
        assert result["decision"] == "NEGADO"
        assert result["score_band"] == "BAIXO"

    def test_score_no_limite_inferior_aprovado(self):
        from src.scoring.lambda_credit_score import apply_credit_policy
        result = apply_credit_policy(700)  # Exatamente no threshold
        assert result["decision"] == "APROVADO"

    def test_score_zero_negado(self):
        from src.scoring.lambda_credit_score import apply_credit_policy
        result = apply_credit_policy(0)
        assert result["decision"] == "NEGADO"


# ── Testes: lambda_handler ────────────────────────────────────
class TestLambdaHandler:
    @patch("src.scoring.lambda_credit_score.get_features_online")
    @patch("src.scoring.lambda_credit_score.invoke_model")
    @patch("src.scoring.lambda_credit_score.save_audit_record")
    @patch("src.scoring.lambda_credit_score.publish_score_metrics")
    def test_retorna_200_para_requisicao_valida(
        self, mock_metrics, mock_audit, mock_model, mock_features,
        valid_event, sample_features
    ):
        mock_features.return_value = sample_features
        mock_model.return_value    = {"probability": 0.75, "score": 750}

        from src.scoring.lambda_credit_score import lambda_handler
        result = lambda_handler(valid_event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["score"] == 750
        assert body["decision"] == "APROVADO"
        assert "latency_ms" in body

    @patch("src.scoring.lambda_credit_score.get_features_online")
    def test_retorna_404_quando_cpf_nao_encontrado(
        self, mock_features, valid_event
    ):
        mock_features.return_value = None  # CPF não existe no Feature Store

        from src.scoring.lambda_credit_score import lambda_handler
        result = lambda_handler(valid_event, None)

        assert result["statusCode"] == 404

    def test_retorna_400_sem_application_id(self, valid_cpf_hash):
        from src.scoring.lambda_credit_score import lambda_handler
        result = lambda_handler({"cpf_hash": valid_cpf_hash}, None)
        assert result["statusCode"] == 400

    def test_retorna_400_com_cpf_hash_invalido(self):
        from src.scoring.lambda_credit_score import lambda_handler
        result = lambda_handler({
            "application_id": "APP-001",
            "cpf_hash": "hash_muito_curto"  # Não é SHA-256
        }, None)
        assert result["statusCode"] == 400

    @patch("src.scoring.lambda_credit_score.get_features_online")
    @patch("src.scoring.lambda_credit_score.invoke_model")
    @patch("src.scoring.lambda_credit_score.save_audit_record")
    @patch("src.scoring.lambda_credit_score.publish_score_metrics")
    def test_aceita_evento_via_api_gateway(
        self, mock_metrics, mock_audit, mock_model, mock_features,
        valid_event, sample_features
    ):
        """Lambda deve aceitar tanto invocação direta quanto via API Gateway."""
        mock_features.return_value = sample_features
        mock_model.return_value    = {"probability": 0.5, "score": 500}

        # Formato API Gateway: body como string JSON
        api_gw_event = {"body": json.dumps(valid_event)}

        from src.scoring.lambda_credit_score import lambda_handler
        result = lambda_handler(api_gw_event, None)

        assert result["statusCode"] == 200

    @patch("src.scoring.lambda_credit_score.get_features_online")
    @patch("src.scoring.lambda_credit_score.invoke_model")
    @patch("src.scoring.lambda_credit_score.save_audit_record")
    @patch("src.scoring.lambda_credit_score.publish_score_metrics")
    def test_retorna_200_mesmo_se_auditoria_falha(
        self, mock_metrics, mock_audit, mock_model, mock_features,
        valid_event, sample_features
    ):
        """Falha na auditoria NÃO deve derrubar o scoring."""
        mock_features.return_value = sample_features
        mock_model.return_value    = {"probability": 0.8, "score": 800}
        mock_audit.side_effect     = Exception("DynamoDB indisponível")  # Falha simulada

        from src.scoring.lambda_credit_score import lambda_handler
        result = lambda_handler(valid_event, None)

        # Mesmo com auditoria falhando, o score deve ser retornado
        assert result["statusCode"] == 200


# ── Testes: aws_helpers ───────────────────────────────────────
class TestAWSHelpers:
    def test_hash_cpf_valido(self):
        from src.utils.aws_helpers import hash_cpf
        result = hash_cpf("12345678901")
        assert len(result) == 64   # SHA-256 = 64 hex chars
        assert result == hash_cpf("12345678901")  # Determinístico

    def test_hash_cpf_com_pontuacao(self):
        from src.utils.aws_helpers import hash_cpf
        # Deve funcionar mesmo com formatação
        assert hash_cpf("123.456.789-01") == hash_cpf("12345678901")

    def test_hash_cpf_invalido_lanca_erro(self):
        from src.utils.aws_helpers import hash_cpf
        with pytest.raises(ValueError):
            hash_cpf("1234")   # Menos de 11 dígitos

    def test_s3_partition_path(self):
        from src.utils.aws_helpers import s3_partition_path
        path = s3_partition_path(
            "s3://bucket/prefix",
            datetime(2025, 1, 20)
        )
        assert path == "s3://bucket/prefix/year=2025/month=01/day=20"


# ── Testes de Integração (marcados para execução separada) ────
@pytest.mark.integration
class TestIntegration:
    """
    Testes de integração — requerem AWS credentials e infraestrutura real.
    Execução: pytest tests/ -v -m integration
    """

    def test_feature_store_roundtrip(self):
        """Testa escrita e leitura no Feature Store."""
        pytest.skip("Requer Feature Store AWS provisionado")

    def test_scoring_endpoint_health(self):
        """Testa se o endpoint SageMaker está respondendo."""
        pytest.skip("Requer endpoint SageMaker provisionado")
