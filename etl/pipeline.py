"""
Orquestração do pipeline ETL.

Coordena a execução das rotinas de ETL (BACEN e IBGE), aplica as datas
padrão de config.settings e persiste os dados em data/gold com nomes
consistentes. Pode ser executado via run_etl.py ou diretamente.
"""
from __future__ import annotations

import pandas as pd

from config.settings import get_date_range, get_month_range
from data_loader import GOLD_DIR

from etl.bacen import (
    get_cambio_usdbrl,
    get_cambio_usdbrl_incremental,
    get_focus_ipca12,
    get_focus_ipca12_incremental,
    get_focus_selic,
    get_focus_selic_incremental,
    get_ibcbr,
    get_ibcbr_incremental,
    get_reservas,
    get_reservas_incremental,
    get_selic_diaria,
    get_selic_diaria_incremental,
    save_series_to_parquet,
)
from etl.ibge_ipca import (
    get_inpc_mensal,
    get_inpc_mensal_incremental,
    get_ipca15_mensal,
    get_ipca15_mensal_incremental,
    get_ipca_mensal,
    get_ipca_mensal_incremental,
)
from etl.ibge_pnad import (
    fetch_desocupacao_uf,
    fetch_salario_real,
    get_desocupacao_incremental,
    get_desocupacao_mensal,
)
from etl.ibge_populacao import get_populacao
from etl.ibge_demanda import (
    get_servicos_mom_sa,
    get_servicos_mom_sa_incremental,
    get_varejo_ampliado_mom_sa,
    get_varejo_ampliado_mom_sa_incremental,
    get_varejo_restrito_mom_sa,
    get_varejo_restrito_mom_sa_incremental,
)
from etl.ibge_ipca_grupos import (
    get_ipca_alimentacao_mom,
    get_ipca_alimentacao_mom_incremental,
    get_ipca_transportes_mom,
    get_ipca_transportes_mom_incremental,
    get_ipca_vestuario_mom,
    get_ipca_vestuario_mom_incremental,
)
from etl.fipezap import (
    get_fipezap_locacao_mom_pct,
    get_fipezap_locacao_mom_pct_incremental,
    get_fipezap_locacao_preco_m2,
    get_fipezap_locacao_preco_m2_incremental,
    get_fipezap_venda_mom_pct,
    get_fipezap_venda_mom_pct_incremental,
    get_fipezap_venda_preco_m2,
    get_fipezap_venda_preco_m2_incremental,
)
from etl.bacen_ivgr import (
    get_ivgr,
    get_ivgr_incremental,
)
from etl.bacen_credito_imobiliario import (
    get_credito_imob_concessoes_mercado_pf,
    get_credito_imob_concessoes_mercado_pf_incremental,
    get_credito_imob_inadimplencia_mercado_pf,
    get_credito_imob_inadimplencia_mercado_pf_incremental,
    get_credito_imob_saldo_mercado_pf,
    get_credito_imob_saldo_mercado_pf_incremental,
    get_credito_imob_saldo_total_pf,
    get_credito_imob_saldo_total_pf_incremental,
    get_credito_imob_taxa_juros_mercado_pf,
    get_credito_imob_taxa_juros_mercado_pf_incremental,
)
from etl.bacen_credito_consumo import (
    get_credito_consumo_juros_pf,
    get_credito_consumo_juros_pf_incremental,
    get_credito_consumo_saldo_pf,
    get_credito_consumo_saldo_pf_incremental,
)
from etl.ibge_sinapi import (
    get_sinapi_custo_m2_uf,
    get_sinapi_custo_m2_uf_incremental,
    get_sinapi_var_12m_uf,
    get_sinapi_var_12m_uf_incremental,
    get_sinapi_var_mensal_uf,
    get_sinapi_var_mensal_uf_incremental,
)
from etl.inep_educacao_superior import (
    get_edu_sup_concluintes,
    get_edu_sup_concluintes_incremental,
    get_edu_sup_docentes_exercicio,
    get_edu_sup_docentes_exercicio_incremental,
    get_edu_sup_igc_medio,
    get_edu_sup_igc_medio_incremental,
    get_edu_sup_ingressantes,
    get_edu_sup_ingressantes_incremental,
    get_edu_sup_matriculas,
    get_edu_sup_matriculas_incremental,
)

# Nomes dos arquivos em data/gold (sem extensão)
GOLD_SELIC = "selic"
GOLD_USDBRL = "usdbrl"
GOLD_IBCBR = "ibcbr"
GOLD_FOCUS = "focus_ipca12"
GOLD_FOCUS_SELIC = "focus_selic"
GOLD_RESERVAS = "reservas"
GOLD_IPCA = "ipca"
GOLD_IPCA15 = "ipca15"
GOLD_INPC = "inpc"
GOLD_DESOCUPACAO = "desocupacao"
GOLD_VAREJO_RESTRITO = "varejo_restrito"
GOLD_VAREJO_AMPLIADO = "varejo_ampliado"
GOLD_SERVICOS = "servicos"
GOLD_IPCA_ALIMENTACAO = "ipca_alimentacao"
GOLD_IPCA_TRANSPORTES = "ipca_transportes"
GOLD_IPCA_VESTUARIO = "ipca_vestuario"
GOLD_FIPEZAP_LOCACAO_PRECO = "fipezap_locacao_preco_m2"
GOLD_FIPEZAP_LOCACAO_MOM = "fipezap_locacao_mom_pct"
GOLD_FIPEZAP_VENDA_PRECO = "fipezap_venda_preco_m2"
GOLD_FIPEZAP_VENDA_MOM = "fipezap_venda_mom_pct"
GOLD_IVGR = "ivgr"
GOLD_CRED_IMOB_SALDO_TOTAL_PF = "credito_imob_saldo_total_pf"
GOLD_CRED_IMOB_SALDO_MERCADO_PF = "credito_imob_saldo_mercado_pf"
GOLD_CRED_IMOB_CONCESSOES_MERCADO_PF = "credito_imob_concessoes_mercado_pf"
GOLD_CRED_IMOB_TAXA_JUROS_MERCADO_PF = "credito_imob_taxa_juros_mercado_pf"
GOLD_CRED_IMOB_INADIMPLENCIA_MERCADO_PF = "credito_imob_inadimplencia_mercado_pf"
GOLD_CRED_CONSUMO_SALDO_PF = "credito_consumo_saldo_pf"
GOLD_CRED_CONSUMO_JUROS_PF = "credito_consumo_juros_pf"
GOLD_SINAPI_CUSTO_M2_UF = "sinapi_custo_m2_uf"
GOLD_SINAPI_VAR_MENSAL_UF = "sinapi_var_mensal_uf"
GOLD_SINAPI_VAR_12M_UF = "sinapi_var_12m_uf"
GOLD_EDU_SUP_MATRICULAS = "edu_sup_matriculas"
GOLD_EDU_SUP_INGRESSANTES = "edu_sup_ingressantes"
GOLD_EDU_SUP_CONCLUINTES = "edu_sup_concluintes"
GOLD_EDU_SUP_DOCENTES_EXERCICIO = "edu_sup_docentes_exercicio"
GOLD_EDU_SUP_IGC_MEDIO = "edu_sup_igc_medio"
GOLD_POPULACAO = "populacao"
GOLD_SALARIO_REAL = "salario_real"
GOLD_DESOCUPACAO_UF = "desocupacao_uf"


def _edu_end_year(end_month: str) -> int:
    try:
        return int(str(end_month).strip()[:4])
    except Exception:
        return 2017


def run_full_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    incremental: bool = False,
) -> None:
    """
    Executa o pipeline completo de ETL.

    Lê as datas padrão de config.settings (ou usa as passadas como argumento),
    roda em sequência o ETL da SELIC, câmbio USD/BRL, IBC-Br, IPCA e IPCA-15,
    e salva os resultados em data/gold com nomes consistentes:
    selic.parquet, usdbrl.parquet, ibcbr.parquet, ipca.parquet, ipca15.parquet.

    Se incremental=True, para cada série lê o parquet existente em data/gold,
    obtém a última data/mês e requisita à API apenas os dados novos (evita
    retrabalho e reduz consumo da API).

    Erros em uma série não interrompem as demais; mensagens são exibidas.
    """
    start_d, end_d = get_date_range(start=start_date, end=end_date)
    start_m, end_m = get_month_range(start=start_month, end=end_month)

    if incremental:
        steps = [
            ("SELIC", lambda: _run_selic_incremental(end_d)),
            ("Câmbio USD/BRL", lambda: _run_usdbrl_incremental(end_d)),
            ("IBC-Br", lambda: _run_ibcbr_incremental(end_d)),
            ("FOCUS IPCA 12m", lambda: _run_focus_incremental(end_d)),
            ("FOCUS SELIC", lambda: _run_focus_selic_incremental(end_d)),
            ("Reservas", lambda: _run_reservas_incremental(end_d)),
            ("IPCA", lambda: _run_ipca_incremental(end_m)),
            ("IPCA-15", lambda: _run_ipca15_incremental(end_m)),
            ("INPC", lambda: _run_inpc_incremental(end_m)),
            ("Desocupação", lambda: _run_desocupacao_incremental(end_m)),
            ("População", lambda: _run_populacao()),
            ("Salário real", lambda: _run_salario_real()),
            ("Desocupação por UF", lambda: _run_desocupacao_uf(start_m, end_m)),
            ("Varejo (restrito)", lambda: _run_varejo_restrito_incremental(end_m)),
            ("Varejo (ampliado)", lambda: _run_varejo_ampliado_incremental(end_m)),
            ("Serviços", lambda: _run_servicos_incremental(end_m)),
            ("IPCA Alimentação", lambda: _run_ipca_alimentacao_incremental(end_m)),
            ("IPCA Transportes", lambda: _run_ipca_transportes_incremental(end_m)),
            ("IPCA Vestuário", lambda: _run_ipca_vestuario_incremental(end_m)),
            ("FipeZAP Locação (preço m²)", lambda: _run_fipezap_locacao_preco_incremental(end_m)),
            ("FipeZAP Locação (var. mensal)", lambda: _run_fipezap_locacao_mom_incremental(end_m)),
            ("FipeZAP Venda (preço m²)", lambda: _run_fipezap_venda_preco_incremental(end_m)),
            ("FipeZAP Venda (var. mensal)", lambda: _run_fipezap_venda_mom_incremental(end_m)),
            ("IVG-R", lambda: _run_ivgr_incremental(end_d)),
            ("Crédito Imob. (saldo total PF)", lambda: _run_credito_imob_saldo_total_pf_incremental(end_d)),
            ("Crédito Imob. (saldo mercado PF)", lambda: _run_credito_imob_saldo_mercado_pf_incremental(end_d)),
            ("Crédito Imob. (concessões mercado PF)", lambda: _run_credito_imob_concessoes_mercado_pf_incremental(end_d)),
            ("Crédito Imob. (taxa juros mercado PF)", lambda: _run_credito_imob_taxa_juros_mercado_pf_incremental(end_d)),
            ("Crédito Imob. (inadimplência mercado PF)", lambda: _run_credito_imob_inadimplencia_mercado_pf_incremental(end_d)),
            ("Crédito Consumo (saldo PF)", lambda: _run_credito_consumo_saldo_pf_incremental(end_d)),
            ("Crédito Consumo (juros PF)", lambda: _run_credito_consumo_juros_pf_incremental(end_d)),
            ("SINAPI (custo m² por UF)", lambda: _run_sinapi_custo_m2_uf_incremental(end_m)),
            ("SINAPI (var. mensal por UF)", lambda: _run_sinapi_var_mensal_uf_incremental(end_m)),
            ("SINAPI (var. 12m por UF)", lambda: _run_sinapi_var_12m_uf_incremental(end_m)),
            ("Educação Sup. — Matrículas", lambda: _run_edu_sup_matriculas_incremental(_edu_end_year(end_m))),
            ("Educação Sup. — Ingressantes", lambda: _run_edu_sup_ingressantes_incremental(_edu_end_year(end_m))),
            ("Educação Sup. — Concluintes", lambda: _run_edu_sup_concluintes_incremental(_edu_end_year(end_m))),
            ("Educação Sup. — Docentes (em exercício)", lambda: _run_edu_sup_docentes_exercicio_incremental(_edu_end_year(end_m))),
            ("Educação Sup. — IGC médio", lambda: _run_edu_sup_igc_medio_incremental(_edu_end_year(end_m))),
        ]
    else:
        steps = [
            ("SELIC", lambda: _run_selic(start_d, end_d)),
            ("Câmbio USD/BRL", lambda: _run_usdbrl(start_d, end_d)),
            ("IBC-Br", lambda: _run_ibcbr(start_d, end_d)),
            ("FOCUS IPCA 12m", lambda: _run_focus(start_d, end_d)),
            ("FOCUS SELIC", lambda: _run_focus_selic(start_d, end_d)),
            ("Reservas", lambda: _run_reservas(start_d, end_d)),
            ("IPCA", lambda: _run_ipca(start_m, end_m)),
            ("IPCA-15", lambda: _run_ipca15(start_m, end_m)),
            ("INPC", lambda: _run_inpc(start_m, end_m)),
            ("Desocupação", lambda: _run_desocupacao(start_m, end_m)),
            ("População", lambda: _run_populacao()),
            ("Salário real", lambda: _run_salario_real()),
            ("Desocupação por UF", lambda: _run_desocupacao_uf(start_m, end_m)),
            ("Varejo (restrito)", lambda: _run_varejo_restrito(start_m, end_m)),
            ("Varejo (ampliado)", lambda: _run_varejo_ampliado(start_m, end_m)),
            ("Serviços", lambda: _run_servicos(start_m, end_m)),
            ("IPCA Alimentação", lambda: _run_ipca_alimentacao(start_m, end_m)),
            ("IPCA Transportes", lambda: _run_ipca_transportes(start_m, end_m)),
            ("IPCA Vestuário", lambda: _run_ipca_vestuario(start_m, end_m)),
            ("FipeZAP Locação (preço m²)", lambda: _run_fipezap_locacao_preco(start_m, end_m)),
            ("FipeZAP Locação (var. mensal)", lambda: _run_fipezap_locacao_mom(start_m, end_m)),
            ("FipeZAP Venda (preço m²)", lambda: _run_fipezap_venda_preco(start_m, end_m)),
            ("FipeZAP Venda (var. mensal)", lambda: _run_fipezap_venda_mom(start_m, end_m)),
            ("IVG-R", lambda: _run_ivgr(start_d, end_d)),
            ("Crédito Imob. (saldo total PF)", lambda: _run_credito_imob_saldo_total_pf(start_d, end_d)),
            ("Crédito Imob. (saldo mercado PF)", lambda: _run_credito_imob_saldo_mercado_pf(start_d, end_d)),
            ("Crédito Imob. (concessões mercado PF)", lambda: _run_credito_imob_concessoes_mercado_pf(start_d, end_d)),
            ("Crédito Imob. (taxa juros mercado PF)", lambda: _run_credito_imob_taxa_juros_mercado_pf(start_d, end_d)),
            ("Crédito Imob. (inadimplência mercado PF)", lambda: _run_credito_imob_inadimplencia_mercado_pf(start_d, end_d)),
            ("Crédito Consumo (saldo PF)", lambda: _run_credito_consumo_saldo_pf(start_d, end_d)),
            ("Crédito Consumo (juros PF)", lambda: _run_credito_consumo_juros_pf(start_d, end_d)),
            ("SINAPI (custo m² por UF)", lambda: _run_sinapi_custo_m2_uf(start_m, end_m)),
            ("SINAPI (var. mensal por UF)", lambda: _run_sinapi_var_mensal_uf(start_m, end_m)),
            ("SINAPI (var. 12m por UF)", lambda: _run_sinapi_var_12m_uf(start_m, end_m)),
            ("Educação Sup. — Matrículas", lambda: _run_edu_sup_matriculas(2017, _edu_end_year(end_m))),
            ("Educação Sup. — Ingressantes", lambda: _run_edu_sup_ingressantes(2017, _edu_end_year(end_m))),
            ("Educação Sup. — Concluintes", lambda: _run_edu_sup_concluintes(2017, _edu_end_year(end_m))),
            ("Educação Sup. — Docentes (em exercício)", lambda: _run_edu_sup_docentes_exercicio(2017, _edu_end_year(end_m))),
            ("Educação Sup. — IGC médio", lambda: _run_edu_sup_igc_medio(2017, _edu_end_year(end_m))),
        ]

    for label, run_step in steps:
        try:
            run_step()
        except Exception as e:
            print(f"[Pipeline] Erro no ETL {label}: {e}")


def run_minimal_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    incremental: bool = False,
) -> None:
    """
    Executa um ETL reduzido (SELIC, câmbio USD/BRL e IPCA) para testes rápidos.

    Se incremental=True, busca apenas dados novos a partir do último parquet em data/gold.
    Útil para validar ambiente e conectividade sem rodar todas as séries.
    """
    start_d, end_d = get_date_range(start=start_date, end=end_date)
    start_m, end_m = get_month_range(start=start_month, end=end_month)

    if incremental:
        steps = [
            ("SELIC", lambda: _run_selic_incremental(end_d)),
            ("Câmbio USD/BRL", lambda: _run_usdbrl_incremental(end_d)),
            ("IPCA", lambda: _run_ipca_incremental(end_m)),
        ]
    else:
        steps = [
            ("SELIC", lambda: _run_selic(start_d, end_d)),
            ("Câmbio USD/BRL", lambda: _run_usdbrl(start_d, end_d)),
            ("IPCA", lambda: _run_ipca(start_m, end_m)),
        ]
    for label, run_step in steps:
        try:
            run_step()
        except Exception as e:
            print(f"[Pipeline] Erro no ETL {label}: {e}")


def _run_selic(start_date: str, end_date: str) -> None:
    """ETL da SELIC e gravação em data/gold/selic.parquet."""
    print("[Pipeline] Iniciando ETL SELIC…")
    df = get_selic_diaria(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SELIC, layer="gold")
        print(f"[Pipeline] Concluído SELIC com {len(df)} linhas.")
    else:
        print("[Pipeline] SELIC sem dados no período.")


def _run_selic_incremental(end_date: str) -> None:
    """ETL incremental da SELIC: apenas dados novos desde o último parquet."""
    print("[Pipeline] Iniciando ETL SELIC (incremental)…")
    df = get_selic_diaria_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SELIC, layer="gold")
        print(f"[Pipeline] Concluído SELIC com {len(df)} linhas.")
    else:
        print("[Pipeline] SELIC sem dados no período.")


def _run_usdbrl(start_date: str, end_date: str) -> None:
    """ETL do câmbio USD/BRL e gravação em data/gold/usdbrl.parquet."""
    print("[Pipeline] Iniciando ETL Câmbio USD/BRL…")
    df = get_cambio_usdbrl(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_USDBRL, layer="gold")
        print(f"[Pipeline] Concluído Câmbio USD/BRL com {len(df)} linhas.")
    else:
        print("[Pipeline] Câmbio USD/BRL sem dados no período.")


def _run_usdbrl_incremental(end_date: str) -> None:
    """ETL incremental do câmbio USD/BRL."""
    print("[Pipeline] Iniciando ETL Câmbio USD/BRL (incremental)…")
    df = get_cambio_usdbrl_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_USDBRL, layer="gold")
        print(f"[Pipeline] Concluído Câmbio USD/BRL com {len(df)} linhas.")
    else:
        print("[Pipeline] Câmbio USD/BRL sem dados no período.")


def _run_ibcbr(start_date: str, end_date: str) -> None:
    """ETL do IBC-Br e gravação em data/gold/ibcbr.parquet."""
    print("[Pipeline] Iniciando ETL IBC-Br…")
    df = get_ibcbr(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IBCBR, layer="gold")
        print(f"[Pipeline] Concluído IBC-Br com {len(df)} linhas.")
    else:
        print("[Pipeline] IBC-Br sem dados no período.")


def _run_ibcbr_incremental(end_date: str) -> None:
    """ETL incremental do IBC-Br."""
    print("[Pipeline] Iniciando ETL IBC-Br (incremental)…")
    df = get_ibcbr_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IBCBR, layer="gold")
        print(f"[Pipeline] Concluído IBC-Br com {len(df)} linhas.")
    else:
        print("[Pipeline] IBC-Br sem dados no período.")


def _run_focus(start_date: str, end_date: str) -> None:
    """ETL da expectativa FOCUS IPCA 12 meses."""
    print("[Pipeline] Iniciando ETL FOCUS IPCA 12m…")
    df = get_focus_ipca12(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FOCUS, layer="gold")
        print(f"[Pipeline] Concluído FOCUS com {len(df)} linhas.")
    else:
        print("[Pipeline] FOCUS sem dados no período.")


def _run_focus_incremental(end_date: str) -> None:
    """ETL incremental da expectativa FOCUS IPCA 12 meses."""
    print("[Pipeline] Iniciando ETL FOCUS IPCA 12m (incremental)…")
    df = get_focus_ipca12_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FOCUS, layer="gold")
        print(f"[Pipeline] Concluído FOCUS com {len(df)} linhas.")
    else:
        print("[Pipeline] FOCUS sem dados no período.")


def _run_focus_selic(start_date: str, end_date: str) -> None:
    """ETL da expectativa FOCUS SELIC."""
    print("[Pipeline] Iniciando ETL FOCUS SELIC…")
    df = get_focus_selic(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FOCUS_SELIC, layer="gold")
        print(f"[Pipeline] Concluído FOCUS SELIC com {len(df)} linhas.")
    else:
        print("[Pipeline] FOCUS SELIC sem dados no período.")


def _run_focus_selic_incremental(end_date: str) -> None:
    """ETL incremental da expectativa FOCUS SELIC."""
    print("[Pipeline] Iniciando ETL FOCUS SELIC (incremental)…")
    df = get_focus_selic_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FOCUS_SELIC, layer="gold")
        print(f"[Pipeline] Concluído FOCUS SELIC com {len(df)} linhas.")
    else:
        print("[Pipeline] FOCUS SELIC sem dados no período.")


def _run_reservas(start_date: str, end_date: str) -> None:
    """ETL das reservas internacionais."""
    print("[Pipeline] Iniciando ETL Reservas…")
    df = get_reservas(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_RESERVAS, layer="gold")
        print(f"[Pipeline] Concluído Reservas com {len(df)} linhas.")
    else:
        print("[Pipeline] Reservas sem dados no período.")


def _run_reservas_incremental(end_date: str) -> None:
    """ETL incremental das reservas internacionais."""
    print("[Pipeline] Iniciando ETL Reservas (incremental)…")
    df = get_reservas_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_RESERVAS, layer="gold")
        print(f"[Pipeline] Concluído Reservas com {len(df)} linhas.")
    else:
        print("[Pipeline] Reservas sem dados no período.")


def _run_ipca(start_month: str, end_month: str) -> None:
    """ETL do IPCA e gravação em data/gold/ipca.parquet."""
    print("[Pipeline] Iniciando ETL IPCA…")
    df = get_ipca_mensal(start_date=start_month, end_date=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA, layer="gold")
        print(f"[Pipeline] Concluído IPCA com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA sem dados no período.")


def _run_ipca_incremental(end_month: str) -> None:
    """ETL incremental do IPCA."""
    print("[Pipeline] Iniciando ETL IPCA (incremental)…")
    df = get_ipca_mensal_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA, layer="gold")
        print(f"[Pipeline] Concluído IPCA com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA sem dados no período.")


def _run_ipca15(start_month: str, end_month: str) -> None:
    """ETL do IPCA-15 e gravação em data/gold/ipca15.parquet."""
    print("[Pipeline] Iniciando ETL IPCA-15…")
    df = get_ipca15_mensal(start_date=start_month, end_date=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA15, layer="gold")
        print(f"[Pipeline] Concluído IPCA-15 com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA-15 sem dados no período.")


def _run_ipca15_incremental(end_month: str) -> None:
    """ETL incremental do IPCA-15."""
    print("[Pipeline] Iniciando ETL IPCA-15 (incremental)…")
    df = get_ipca15_mensal_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA15, layer="gold")
        print(f"[Pipeline] Concluído IPCA-15 com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA-15 sem dados no período.")


def _run_inpc(start_month: str, end_month: str) -> None:
    """ETL do INPC."""
    print("[Pipeline] Iniciando ETL INPC…")
    df = get_inpc_mensal(start_date=start_month, end_date=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_INPC, layer="gold")
        print(f"[Pipeline] Concluído INPC com {len(df)} linhas.")
    else:
        print("[Pipeline] INPC sem dados no período.")


def _run_inpc_incremental(end_month: str) -> None:
    """ETL incremental do INPC."""
    print("[Pipeline] Iniciando ETL INPC (incremental)…")
    df = get_inpc_mensal_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_INPC, layer="gold")
        print(f"[Pipeline] Concluído INPC com {len(df)} linhas.")
    else:
        print("[Pipeline] INPC sem dados no período.")


def _run_desocupacao(start_month: str, end_month: str) -> None:
    """ETL da taxa de desocupação (PNAD trimestral)."""
    print("[Pipeline] Iniciando ETL Desocupação…")
    df = get_desocupacao_mensal(start_date=start_month, end_date=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_DESOCUPACAO, layer="gold")
        print(f"[Pipeline] Concluído Desocupação com {len(df)} linhas.")
    else:
        print("[Pipeline] Desocupação sem dados no período.")


def _run_populacao() -> None:
    """ETL da população residente estimada (Brasil e UFs)."""
    print("[Pipeline] Iniciando ETL População…")
    df = get_populacao()
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_POPULACAO, layer="gold")
        print(f"[Pipeline] Concluído População com {len(df)} linhas.")
    else:
        print("[Pipeline] População sem dados.")


def _run_salario_real() -> None:
    """ETL do salário real (PNAD trimestral, tabela 5436)."""
    print("[Pipeline] Iniciando ETL Salário real…")
    df = fetch_salario_real()
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SALARIO_REAL, layer="gold")
        print(f"[Pipeline] Concluído Salário real com {len(df)} linhas.")
    else:
        print("[Pipeline] Salário real sem dados.")


def _run_desocupacao_uf(start_month: str, end_month: str) -> None:
    """ETL da taxa de desocupação por UF."""
    print("[Pipeline] Iniciando ETL Desocupação por UF…")
    df = fetch_desocupacao_uf(start_date=start_month, end_date=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_DESOCUPACAO_UF, layer="gold")
        print(f"[Pipeline] Concluído Desocupação por UF com {len(df)} linhas.")
    else:
        print("[Pipeline] Desocupação por UF sem dados.")


def _run_desocupacao_incremental(end_month: str) -> None:
    """ETL incremental da taxa de desocupação."""
    print("[Pipeline] Iniciando ETL Desocupação (incremental)…")
    df = get_desocupacao_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_DESOCUPACAO, layer="gold")
        print(f"[Pipeline] Concluído Desocupação com {len(df)} linhas.")
    else:
        print("[Pipeline] Desocupação sem dados no período.")


def _run_varejo_restrito(start_month: str, end_month: str) -> None:
    """ETL do varejo restrito (PMC) e gravação em data/gold/varejo_restrito.parquet."""
    print("[Pipeline] Iniciando ETL Varejo (restrito)…")
    df = get_varejo_restrito_mom_sa(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_VAREJO_RESTRITO, layer="gold")
        print(f"[Pipeline] Concluído Varejo (restrito) com {len(df)} linhas.")
    else:
        print("[Pipeline] Varejo (restrito) sem dados no período.")


def _run_varejo_restrito_incremental(end_month: str) -> None:
    """ETL incremental do varejo restrito (PMC): apenas meses novos."""
    print("[Pipeline] Iniciando ETL Varejo (restrito) (incremental)…")
    df = get_varejo_restrito_mom_sa_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_VAREJO_RESTRITO, layer="gold")
        print(f"[Pipeline] Concluído Varejo (restrito) com {len(df)} linhas.")
    else:
        print("[Pipeline] Varejo (restrito) sem dados no período.")


def _run_varejo_ampliado(start_month: str, end_month: str) -> None:
    """ETL do varejo ampliado (PMC) e gravação em data/gold/varejo_ampliado.parquet."""
    print("[Pipeline] Iniciando ETL Varejo (ampliado)…")
    df = get_varejo_ampliado_mom_sa(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_VAREJO_AMPLIADO, layer="gold")
        print(f"[Pipeline] Concluído Varejo (ampliado) com {len(df)} linhas.")
    else:
        print("[Pipeline] Varejo (ampliado) sem dados no período.")


def _run_varejo_ampliado_incremental(end_month: str) -> None:
    """ETL incremental do varejo ampliado (PMC): apenas meses novos."""
    print("[Pipeline] Iniciando ETL Varejo (ampliado) (incremental)…")
    df = get_varejo_ampliado_mom_sa_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_VAREJO_AMPLIADO, layer="gold")
        print(f"[Pipeline] Concluído Varejo (ampliado) com {len(df)} linhas.")
    else:
        print("[Pipeline] Varejo (ampliado) sem dados no período.")


def _run_servicos(start_month: str, end_month: str) -> None:
    """ETL de serviços (PMS) e gravação em data/gold/servicos.parquet."""
    print("[Pipeline] Iniciando ETL Serviços…")
    df = get_servicos_mom_sa(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SERVICOS, layer="gold")
        print(f"[Pipeline] Concluído Serviços com {len(df)} linhas.")
    else:
        print("[Pipeline] Serviços sem dados no período.")


def _run_servicos_incremental(end_month: str) -> None:
    """ETL incremental de serviços (PMS): apenas meses novos."""
    print("[Pipeline] Iniciando ETL Serviços (incremental)…")
    df = get_servicos_mom_sa_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SERVICOS, layer="gold")
        print(f"[Pipeline] Concluído Serviços com {len(df)} linhas.")
    else:
        print("[Pipeline] Serviços sem dados no período.")


def _run_ipca_alimentacao(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL IPCA Alimentação…")
    df = get_ipca_alimentacao_mom(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA_ALIMENTACAO, layer="gold")
        print(f"[Pipeline] Concluído IPCA Alimentação com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA Alimentação sem dados no período.")


def _run_ipca_alimentacao_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL IPCA Alimentação (incremental)…")
    df = get_ipca_alimentacao_mom_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA_ALIMENTACAO, layer="gold")
        print(f"[Pipeline] Concluído IPCA Alimentação com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA Alimentação sem dados no período.")


def _run_ipca_transportes(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL IPCA Transportes…")
    df = get_ipca_transportes_mom(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA_TRANSPORTES, layer="gold")
        print(f"[Pipeline] Concluído IPCA Transportes com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA Transportes sem dados no período.")


def _run_ipca_transportes_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL IPCA Transportes (incremental)…")
    df = get_ipca_transportes_mom_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA_TRANSPORTES, layer="gold")
        print(f"[Pipeline] Concluído IPCA Transportes com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA Transportes sem dados no período.")


def _run_ipca_vestuario(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL IPCA Vestuário…")
    df = get_ipca_vestuario_mom(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA_VESTUARIO, layer="gold")
        print(f"[Pipeline] Concluído IPCA Vestuário com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA Vestuário sem dados no período.")


def _run_ipca_vestuario_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL IPCA Vestuário (incremental)…")
    df = get_ipca_vestuario_mom_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IPCA_VESTUARIO, layer="gold")
        print(f"[Pipeline] Concluído IPCA Vestuário com {len(df)} linhas.")
    else:
        print("[Pipeline] IPCA Vestuário sem dados no período.")


def _run_fipezap_locacao_preco(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Locação (preço m²)…")
    df = get_fipezap_locacao_preco_m2(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_LOCACAO_PRECO, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Locação (preço m²) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Locação (preço m²) sem dados no período.")


def _run_fipezap_locacao_preco_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Locação (preço m²) (incremental)…")
    df = get_fipezap_locacao_preco_m2_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_LOCACAO_PRECO, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Locação (preço m²) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Locação (preço m²) sem dados no período.")


def _run_fipezap_locacao_mom(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Locação (var. mensal)…")
    df = get_fipezap_locacao_mom_pct(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_LOCACAO_MOM, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Locação (var. mensal) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Locação (var. mensal) sem dados no período.")


def _run_fipezap_locacao_mom_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Locação (var. mensal) (incremental)…")
    df = get_fipezap_locacao_mom_pct_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_LOCACAO_MOM, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Locação (var. mensal) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Locação (var. mensal) sem dados no período.")


def _run_fipezap_venda_preco(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Venda (preço m²)…")
    df = get_fipezap_venda_preco_m2(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_VENDA_PRECO, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Venda (preço m²) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Venda (preço m²) sem dados no período.")


def _run_fipezap_venda_preco_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Venda (preço m²) (incremental)…")
    df = get_fipezap_venda_preco_m2_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_VENDA_PRECO, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Venda (preço m²) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Venda (preço m²) sem dados no período.")


def _run_fipezap_venda_mom(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Venda (var. mensal)…")
    df = get_fipezap_venda_mom_pct(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_VENDA_MOM, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Venda (var. mensal) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Venda (var. mensal) sem dados no período.")


def _run_fipezap_venda_mom_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL FipeZAP Venda (var. mensal) (incremental)…")
    df = get_fipezap_venda_mom_pct_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_FIPEZAP_VENDA_MOM, layer="gold")
        print(f"[Pipeline] Concluído FipeZAP Venda (var. mensal) com {len(df)} linhas.")
    else:
        print("[Pipeline] FipeZAP Venda (var. mensal) sem dados no período.")


def _run_ivgr(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL IVG-R…")
    df = get_ivgr(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IVGR, layer="gold")
        print(f"[Pipeline] Concluído IVG-R com {len(df)} linhas.")
    else:
        print("[Pipeline] IVG-R sem dados no período.")


def _run_ivgr_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL IVG-R (incremental)…")
    df = get_ivgr_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_IVGR, layer="gold")
        print(f"[Pipeline] Concluído IVG-R com {len(df)} linhas.")
    else:
        print("[Pipeline] IVG-R sem dados no período.")


def _run_credito_imob_saldo_total_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (saldo total PF)…")
    df = get_credito_imob_saldo_total_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_SALDO_TOTAL_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (saldo total PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (saldo total PF) sem dados no período.")


def _run_credito_imob_saldo_total_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (saldo total PF) (incremental)…")
    df = get_credito_imob_saldo_total_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_SALDO_TOTAL_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (saldo total PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (saldo total PF) sem dados no período.")


def _run_credito_imob_saldo_mercado_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (saldo mercado PF)…")
    df = get_credito_imob_saldo_mercado_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_SALDO_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (saldo mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (saldo mercado PF) sem dados no período.")


def _run_credito_imob_saldo_mercado_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (saldo mercado PF) (incremental)…")
    df = get_credito_imob_saldo_mercado_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_SALDO_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (saldo mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (saldo mercado PF) sem dados no período.")


def _run_credito_imob_concessoes_mercado_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (concessões mercado PF)…")
    df = get_credito_imob_concessoes_mercado_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_CONCESSOES_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (concessões mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (concessões mercado PF) sem dados no período.")


def _run_credito_imob_concessoes_mercado_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (concessões mercado PF) (incremental)…")
    df = get_credito_imob_concessoes_mercado_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_CONCESSOES_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (concessões mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (concessões mercado PF) sem dados no período.")


def _run_credito_imob_taxa_juros_mercado_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (taxa juros mercado PF)…")
    df = get_credito_imob_taxa_juros_mercado_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_TAXA_JUROS_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (taxa juros mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (taxa juros mercado PF) sem dados no período.")


def _run_credito_imob_taxa_juros_mercado_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (taxa juros mercado PF) (incremental)…")
    df = get_credito_imob_taxa_juros_mercado_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_TAXA_JUROS_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (taxa juros mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (taxa juros mercado PF) sem dados no período.")


def _run_credito_imob_inadimplencia_mercado_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (inadimplência mercado PF)…")
    df = get_credito_imob_inadimplencia_mercado_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_INADIMPLENCIA_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (inadimplência mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (inadimplência mercado PF) sem dados no período.")


def _run_credito_imob_inadimplencia_mercado_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Imob. (inadimplência mercado PF) (incremental)…")
    df = get_credito_imob_inadimplencia_mercado_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_IMOB_INADIMPLENCIA_MERCADO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Imob. (inadimplência mercado PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Imob. (inadimplência mercado PF) sem dados no período.")


def _run_credito_consumo_saldo_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Consumo (saldo PF)…")
    df = get_credito_consumo_saldo_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_CONSUMO_SALDO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Consumo (saldo PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Consumo (saldo PF) sem dados no período.")


def _run_credito_consumo_saldo_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Consumo (saldo PF) (incremental)…")
    df = get_credito_consumo_saldo_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_CONSUMO_SALDO_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Consumo (saldo PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Consumo (saldo PF) sem dados no período.")


def _run_credito_consumo_juros_pf(start_date: str, end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Consumo (juros PF)…")
    df = get_credito_consumo_juros_pf(start_date=start_date, end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_CONSUMO_JUROS_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Consumo (juros PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Consumo (juros PF) sem dados no período.")


def _run_credito_consumo_juros_pf_incremental(end_date: str) -> None:
    print("[Pipeline] Iniciando ETL Crédito Consumo (juros PF) (incremental)…")
    df = get_credito_consumo_juros_pf_incremental(end_date=end_date)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_CRED_CONSUMO_JUROS_PF, layer="gold")
        print(f"[Pipeline] Concluído Crédito Consumo (juros PF) com {len(df)} linhas.")
    else:
        print("[Pipeline] Crédito Consumo (juros PF) sem dados no período.")


def _run_sinapi_custo_m2_uf(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL SINAPI (custo m² por UF)…")
    df = get_sinapi_custo_m2_uf(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SINAPI_CUSTO_M2_UF, layer="gold")
        print(f"[Pipeline] Concluído SINAPI (custo m² UF) com {len(df)} linhas.")
    else:
        print("[Pipeline] SINAPI (custo m² UF) sem dados no período.")


def _run_sinapi_custo_m2_uf_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL SINAPI (custo m² por UF) (incremental)…")
    df = get_sinapi_custo_m2_uf_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SINAPI_CUSTO_M2_UF, layer="gold")
        print(f"[Pipeline] Concluído SINAPI (custo m² UF) com {len(df)} linhas.")
    else:
        print("[Pipeline] SINAPI (custo m² UF) sem dados no período.")


def _run_sinapi_var_mensal_uf(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL SINAPI (var. mensal por UF)…")
    df = get_sinapi_var_mensal_uf(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SINAPI_VAR_MENSAL_UF, layer="gold")
        print(f"[Pipeline] Concluído SINAPI (var. mensal UF) com {len(df)} linhas.")
    else:
        print("[Pipeline] SINAPI (var. mensal UF) sem dados no período.")


def _run_sinapi_var_mensal_uf_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL SINAPI (var. mensal por UF) (incremental)…")
    df = get_sinapi_var_mensal_uf_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SINAPI_VAR_MENSAL_UF, layer="gold")
        print(f"[Pipeline] Concluído SINAPI (var. mensal UF) com {len(df)} linhas.")
    else:
        print("[Pipeline] SINAPI (var. mensal UF) sem dados no período.")


def _run_sinapi_var_12m_uf(start_month: str, end_month: str) -> None:
    print("[Pipeline] Iniciando ETL SINAPI (var. 12m por UF)…")
    df = get_sinapi_var_12m_uf(start_month=start_month, end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SINAPI_VAR_12M_UF, layer="gold")
        print(f"[Pipeline] Concluído SINAPI (var. 12m UF) com {len(df)} linhas.")
    else:
        print("[Pipeline] SINAPI (var. 12m UF) sem dados no período.")


def _run_sinapi_var_12m_uf_incremental(end_month: str) -> None:
    print("[Pipeline] Iniciando ETL SINAPI (var. 12m por UF) (incremental)…")
    df = get_sinapi_var_12m_uf_incremental(end_month=end_month)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_SINAPI_VAR_12M_UF, layer="gold")
        print(f"[Pipeline] Concluído SINAPI (var. 12m UF) com {len(df)} linhas.")
    else:
        print("[Pipeline] SINAPI (var. 12m UF) sem dados no período.")


def _run_edu_sup_matriculas(start_year: int, end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Matrículas…")
    df = get_edu_sup_matriculas(start_year=start_year, end_year=end_year)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_MATRICULAS, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Matrículas com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Matrículas sem dados no período.")


def _run_edu_sup_matriculas_incremental(end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Matrículas (incremental)…")
    try:
        existing = pd.read_parquet(GOLD_DIR / f"{GOLD_EDU_SUP_MATRICULAS}.parquet")
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"])
    except Exception:
        existing = None
    df = get_edu_sup_matriculas_incremental(end_year=end_year, existing=existing)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_MATRICULAS, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Matrículas com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Matrículas sem dados no período.")


def _run_edu_sup_ingressantes(start_year: int, end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Ingressantes…")
    df = get_edu_sup_ingressantes(start_year=start_year, end_year=end_year)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_INGRESSANTES, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Ingressantes com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Ingressantes sem dados no período.")


def _run_edu_sup_ingressantes_incremental(end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Ingressantes (incremental)…")
    try:
        existing = pd.read_parquet(GOLD_DIR / f"{GOLD_EDU_SUP_INGRESSANTES}.parquet")
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"])
    except Exception:
        existing = None
    df = get_edu_sup_ingressantes_incremental(end_year=end_year, existing=existing)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_INGRESSANTES, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Ingressantes com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Ingressantes sem dados no período.")


def _run_edu_sup_concluintes(start_year: int, end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Concluintes…")
    df = get_edu_sup_concluintes(start_year=start_year, end_year=end_year)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_CONCLUINTES, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Concluintes com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Concluintes sem dados no período.")


def _run_edu_sup_concluintes_incremental(end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Concluintes (incremental)…")
    try:
        existing = pd.read_parquet(GOLD_DIR / f"{GOLD_EDU_SUP_CONCLUINTES}.parquet")
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"])
    except Exception:
        existing = None
    df = get_edu_sup_concluintes_incremental(end_year=end_year, existing=existing)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_CONCLUINTES, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Concluintes com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Concluintes sem dados no período.")


def _run_edu_sup_docentes_exercicio(start_year: int, end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Docentes (em exercício)…")
    df = get_edu_sup_docentes_exercicio(start_year=start_year, end_year=end_year)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_DOCENTES_EXERCICIO, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Docentes (em exercício) com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Docentes (em exercício) sem dados no período.")


def _run_edu_sup_docentes_exercicio_incremental(end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — Docentes (em exercício) (incremental)…")
    try:
        existing = pd.read_parquet(GOLD_DIR / f"{GOLD_EDU_SUP_DOCENTES_EXERCICIO}.parquet")
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"])
    except Exception:
        existing = None
    df = get_edu_sup_docentes_exercicio_incremental(end_year=end_year, existing=existing)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_DOCENTES_EXERCICIO, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — Docentes (em exercício) com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — Docentes (em exercício) sem dados no período.")


def _run_edu_sup_igc_medio(start_year: int, end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — IGC médio…")
    df = get_edu_sup_igc_medio(start_year=start_year, end_year=end_year)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_IGC_MEDIO, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — IGC médio com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — IGC médio sem dados no período.")


def _run_edu_sup_igc_medio_incremental(end_year: int) -> None:
    print("[Pipeline] Iniciando ETL Educação Sup. — IGC médio (incremental)…")
    try:
        existing = pd.read_parquet(GOLD_DIR / f"{GOLD_EDU_SUP_IGC_MEDIO}.parquet")
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"])
    except Exception:
        existing = None
    df = get_edu_sup_igc_medio_incremental(end_year=end_year, existing=existing)
    if df is not None and not df.empty:
        save_series_to_parquet(df, GOLD_EDU_SUP_IGC_MEDIO, layer="gold")
        print(f"[Pipeline] Concluído Educação Sup. — IGC médio com {len(df)} linhas.")
    else:
        print("[Pipeline] Educação Sup. — IGC médio sem dados no período.")


def run_full_pipeline() -> None:
    """
    Alias para run_full_etl(); mantido para compatibilidade com run_etl.py.
    """
    run_full_etl()


if __name__ == "__main__":
    run_full_etl()
