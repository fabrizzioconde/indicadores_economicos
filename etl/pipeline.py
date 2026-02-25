"""
Orquestração do pipeline ETL.

Coordena a execução das rotinas de ETL (BACEN e IBGE), aplica as datas
padrão de config.settings e persiste os dados em data/gold com nomes
consistentes. Pode ser executado via run_etl.py ou diretamente.
"""
from __future__ import annotations

from config.settings import get_date_range, get_month_range

from etl.bacen import (
    get_cambio_usdbrl,
    get_cambio_usdbrl_incremental,
    get_ibcbr,
    get_ibcbr_incremental,
    get_selic_diaria,
    get_selic_diaria_incremental,
    save_series_to_parquet,
)
from etl.ibge_ipca import (
    get_ipca15_mensal,
    get_ipca15_mensal_incremental,
    get_ipca_mensal,
    get_ipca_mensal_incremental,
)

# Nomes dos arquivos em data/gold (sem extensão)
GOLD_SELIC = "selic"
GOLD_USDBRL = "usdbrl"
GOLD_IBCBR = "ibcbr"
GOLD_IPCA = "ipca"
GOLD_IPCA15 = "ipca15"


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
            ("IPCA", lambda: _run_ipca_incremental(end_m)),
            ("IPCA-15", lambda: _run_ipca15_incremental(end_m)),
        ]
    else:
        steps = [
            ("SELIC", lambda: _run_selic(start_d, end_d)),
            ("Câmbio USD/BRL", lambda: _run_usdbrl(start_d, end_d)),
            ("IBC-Br", lambda: _run_ibcbr(start_d, end_d)),
            ("IPCA", lambda: _run_ipca(start_m, end_m)),
            ("IPCA-15", lambda: _run_ipca15(start_m, end_m)),
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


def run_full_pipeline() -> None:
    """
    Alias para run_full_etl(); mantido para compatibilidade com run_etl.py.
    """
    run_full_etl()


if __name__ == "__main__":
    run_full_etl()
