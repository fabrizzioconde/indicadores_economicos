"""
Testes básicos do ETL.

Focam em validações estruturais (colunas, tipos, existência de arquivos),
não em valores específicos. Em produção, testes mais robustos (mocks de API,
testes de integração, dados fixos) seriam recomendados.
"""
from __future__ import annotations

from pathlib import Path
from contextlib import ExitStack
from unittest.mock import patch

import pandas as pd
import pytest

from etl.bacen import fetch_bcb_series, run_bacen_etl
from etl.ibge_ipca import fetch_ibge_ipca_series, run_ipca_etl
from etl.pipeline import run_full_pipeline, run_minimal_etl


# -----------------------------------------------------------------------------
# Testes de importação e executabilidade (básicos)
# -----------------------------------------------------------------------------


def test_bacen_etl_importado() -> None:
    """Garante que run_bacen_etl existe e é callable."""
    assert callable(run_bacen_etl)


def test_ipca_etl_importado() -> None:
    """Garante que run_ipca_etl existe e é callable."""
    assert callable(run_ipca_etl)


def test_run_full_pipeline_executa_sem_erro(tmp_path: Path) -> None:
    """
    Executar o pipeline completo não deve levantar exceção.

    Importante: o pipeline real chama APIs externas (BACEN, IBGE, PDFs), então aqui
    usamos mocks para garantir que o teste seja determinístico e rápido.
    """
    # DataFrames mínimos por "tipo" de série
    def _make_bacen_df(code: str = "1") -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
                "value": [1.0, 2.0],
                "series_code": [code, code],
                "source": ["BACEN_SGS", "BACEN_SGS"],
            }
        )

    def _make_ibge_df(ind: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
                "value": [0.1, 0.2],
                "indicator": [ind, ind],
                "source": ["IBGE", "IBGE"],
            }
        )

    def _make_city_df(ind: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01"]),
                "city": ["São Paulo"],
                "uf": ["SP"],
                "value": [50.0],
                "indicator": [ind],
                "source": ["SRC"],
            }
        )

    def _make_uf_df(ind: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01"]),
                "uf": ["SP"],
                "value": [100.0],
                "indicator": [ind],
                "source": ["SRC"],
            }
        )

    def _make_edu_df(ind: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01"]),
                "uf": ["BR"],
                "rede": ["TOTAL"],
                "modalidade": ["TOTAL"],
                "area": ["TOTAL"],
                "value": [123.0],
                "indicator": [ind],
                "source": ["SRC"],
            }
        )

    with ExitStack() as stack:
        stack.enter_context(patch("etl.bacen.GOLD_DIR", tmp_path))  # evita sujar gold real
        stack.enter_context(patch("etl.pipeline.get_selic_diaria", return_value=_make_bacen_df("432")))
        stack.enter_context(patch("etl.pipeline.get_cambio_usdbrl", return_value=_make_bacen_df("1")))
        stack.enter_context(patch("etl.pipeline.get_ibcbr", return_value=_make_bacen_df("24364")))
        stack.enter_context(patch("etl.pipeline.get_focus_ipca12", return_value=_make_bacen_df("27574")))
        stack.enter_context(patch("etl.pipeline.get_focus_selic", return_value=_make_bacen_df("27573")))
        stack.enter_context(patch("etl.pipeline.get_reservas", return_value=_make_bacen_df("13982")))
        stack.enter_context(patch("etl.pipeline.get_ipca_mensal", return_value=_make_ibge_df("IPCA")))
        stack.enter_context(patch("etl.pipeline.get_ipca15_mensal", return_value=_make_ibge_df("IPCA15")))
        stack.enter_context(patch("etl.pipeline.get_inpc_mensal", return_value=_make_ibge_df("INPC")))
        stack.enter_context(patch("etl.pipeline.get_desocupacao_mensal", return_value=_make_ibge_df("DESOCUPACAO")))
        stack.enter_context(patch("etl.pipeline.get_varejo_restrito_mom_sa", return_value=_make_ibge_df("VAREJO_RESTRITO_MOM_SA")))
        stack.enter_context(patch("etl.pipeline.get_varejo_ampliado_mom_sa", return_value=_make_ibge_df("VAREJO_AMPLIADO_MOM_SA")))
        stack.enter_context(patch("etl.pipeline.get_servicos_mom_sa", return_value=_make_ibge_df("SERVICOS_MOM_SA")))
        stack.enter_context(patch("etl.pipeline.get_fipezap_locacao_preco_m2", return_value=_make_city_df("FIPEZAP_LOCACAO_PRECO_M2")))
        stack.enter_context(patch("etl.pipeline.get_fipezap_locacao_mom_pct", return_value=_make_city_df("FIPEZAP_LOCACAO_MOM_PCT")))
        stack.enter_context(patch("etl.pipeline.get_fipezap_venda_preco_m2", return_value=_make_city_df("FIPEZAP_VENDA_PRECO_M2")))
        stack.enter_context(patch("etl.pipeline.get_fipezap_venda_mom_pct", return_value=_make_city_df("FIPEZAP_VENDA_MOM_PCT")))
        stack.enter_context(patch("etl.pipeline.get_ivgr", return_value=_make_bacen_df("21340")))
        stack.enter_context(patch("etl.pipeline.get_credito_imob_saldo_total_pf", return_value=_make_bacen_df("20612")))
        stack.enter_context(patch("etl.pipeline.get_credito_imob_saldo_mercado_pf", return_value=_make_bacen_df("20611")))
        stack.enter_context(patch("etl.pipeline.get_credito_imob_concessoes_mercado_pf", return_value=_make_bacen_df("20702")))
        stack.enter_context(patch("etl.pipeline.get_credito_imob_taxa_juros_mercado_pf", return_value=_make_bacen_df("20772")))
        stack.enter_context(patch("etl.pipeline.get_credito_imob_inadimplencia_mercado_pf", return_value=_make_bacen_df("21149")))
        stack.enter_context(patch("etl.pipeline.get_sinapi_custo_m2_uf", return_value=_make_uf_df("SINAPI_CUSTO_M2_RS")))
        stack.enter_context(patch("etl.pipeline.get_sinapi_var_mensal_uf", return_value=_make_uf_df("SINAPI_VAR_MENSAL_PCT")))
        stack.enter_context(patch("etl.pipeline.get_sinapi_var_12m_uf", return_value=_make_uf_df("SINAPI_VAR_12M_PCT")))
        stack.enter_context(patch("etl.pipeline.get_edu_sup_matriculas", return_value=_make_edu_df("EDU_SUP_MATRICULAS")))
        stack.enter_context(patch("etl.pipeline.get_edu_sup_ingressantes", return_value=_make_edu_df("EDU_SUP_INGRESSANTES")))
        stack.enter_context(patch("etl.pipeline.get_edu_sup_concluintes", return_value=_make_edu_df("EDU_SUP_CONCLUINTES")))
        stack.enter_context(patch("etl.pipeline.get_edu_sup_docentes_exercicio", return_value=_make_edu_df("EDU_SUP_DOCENTES_EXERCICIO")))
        stack.enter_context(patch("etl.pipeline.get_edu_sup_igc_medio", return_value=_make_edu_df("EDU_SUP_IGC_MEDIO")))

        run_full_pipeline()


# -----------------------------------------------------------------------------
# Testes estruturais: fetch_bcb_series (BACEN)
# Valida que o DataFrame retornado tem as colunas esperadas e date em datetime.
# -----------------------------------------------------------------------------


@pytest.mark.basic
def test_fetch_bcb_series_retorna_dataframe_com_colunas_esperadas() -> None:
    """
    Valida que fetch_bcb_series retorna um DataFrame com colunas date, value,
    series_code e source quando a API responde com dados válidos.

    Usa mock da resposta HTTP para não depender da API real. Em produção,
    testes de integração com API real ou com dados fixos seriam recomendados.
    """
    # Resposta mínima no formato da API SGS do BCB (data DD/MM/YYYY, valor)
    mock_response = [
        {"data": "01/01/2020", "valor": "4.25"},
        {"data": "02/01/2020", "valor": "4.25"},
    ]
    with patch("etl.bacen.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        df = fetch_bcb_series("432", "2020-01-01", "2020-01-31")

    assert isinstance(df, pd.DataFrame), "Deve retornar um DataFrame"
    assert not df.empty, "DataFrame não deve estar vazio quando a API retorna dados"
    assert "date" in df.columns, "Deve conter coluna date"
    assert "value" in df.columns, "Deve conter coluna value"
    assert "series_code" in df.columns, "Deve conter coluna series_code"
    assert "source" in df.columns, "Deve conter coluna source"
    assert pd.api.types.is_datetime64_any_dtype(df["date"]), "Coluna date deve ser datetime"


@pytest.mark.basic
def test_fetch_bcb_series_resposta_vazia_retorna_dataframe_vazio_com_colunas() -> None:
    """
    Valida que, quando a API retorna lista vazia, o DataFrame retornado
    tem as colunas esperadas (mesmo vazio), para manter o contrato estrutural.
    """
    with patch("etl.bacen.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = []

        df = fetch_bcb_series("432", "2020-01-01", "2020-01-31")

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    for col in ("date", "value", "series_code", "source"):
        assert col in df.columns


# -----------------------------------------------------------------------------
# Testes estruturais: fetch_ibge_ipca_series (IBGE SIDRA)
# Valida colunas date, value, indicator, source e tipo de date.
# -----------------------------------------------------------------------------


@pytest.mark.basic
def test_fetch_ibge_ipca_series_retorna_dataframe_com_colunas_esperadas() -> None:
    """
    Valida que fetch_ibge_ipca_series retorna um DataFrame com colunas date,
    value, indicator e source quando a API SIDRA responde com dados válidos.

    Usa mock da resposta HTTP. Em produção, testes com dados fixos ou
    integração com a API real seriam recomendados.
    """
    # Formato SIDRA: primeira linha pode ser cabeçalho; linhas de dados têm V e D3C
    mock_response = [
        {"NC": "1", "V": "0.45", "D3C": "202001", "D3N": "janeiro 2020"},
        {"NC": "1", "V": "0.30", "D3C": "202002", "D3N": "fevereiro 2020"},
    ]
    with patch("etl.ibge_ipca.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        df = fetch_ibge_ipca_series("IPCA", "2020-01", "2020-06")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "date" in df.columns
    assert "value" in df.columns
    assert "indicator" in df.columns
    assert "source" in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df["date"]), "Coluna date deve ser datetime"


@pytest.mark.basic
def test_fetch_ibge_ipca_series_resposta_vazia_retorna_dataframe_vazio_com_colunas() -> None:
    """
    Valida que, quando não há registros no período, o DataFrame retornado
    mantém as colunas esperadas (contrato estrutural).
    """
    # Apenas cabeçalho ou lista vazia de dados
    mock_response = []
    with patch("etl.ibge_ipca.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        df = fetch_ibge_ipca_series("IPCA", "2020-01", "2020-06")

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    for col in ("date", "value", "indicator", "source"):
        assert col in df.columns


# -----------------------------------------------------------------------------
# Teste: run_minimal_etl cria os arquivos esperados em data/gold
# Usa diretório temporário para não alterar a pasta real.
# -----------------------------------------------------------------------------


@pytest.mark.basic
def test_run_minimal_etl_cria_arquivos_em_gold(tmp_path: Path) -> None:
    """
    Valida que, após executar run_minimal_etl(), existem os arquivos
    selic.parquet, usdbrl.parquet e ipca.parquet na pasta gold.

    Usa tmp_path como pasta gold (via patch) e mocks dos fetchers para
    não depender da API. Assim não sujamos a pasta data/gold real.
    """
    # DataFrames mínimos no formato esperado pelo pipeline (todas as colunas com mesmo length)
    def _make_bacen_df():
        return pd.DataFrame({
            "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "value": [4.25, 4.25],
            "series_code": ["432", "432"],
            "source": ["BACEN_SGS", "BACEN_SGS"],
        })

    def _make_ipca_df():
        return pd.DataFrame({
            "date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            "value": [0.45, 0.30],
            "indicator": ["IPCA", "IPCA"],
            "source": ["IBGE_IPCA", "IBGE_IPCA"],
        })

    with (
        patch("etl.bacen.GOLD_DIR", tmp_path),
        patch("etl.pipeline.get_selic_diaria", return_value=_make_bacen_df()),
        patch("etl.pipeline.get_cambio_usdbrl", return_value=_make_bacen_df()),
        patch("etl.pipeline.get_ipca_mensal", return_value=_make_ipca_df()),
    ):
        run_minimal_etl()

    assert (tmp_path / "selic.parquet").exists(), "Deve existir data/gold/selic.parquet"
    assert (tmp_path / "usdbrl.parquet").exists(), "Deve existir data/gold/usdbrl.parquet"
    assert (tmp_path / "ipca.parquet").exists(), "Deve existir data/gold/ipca.parquet"
