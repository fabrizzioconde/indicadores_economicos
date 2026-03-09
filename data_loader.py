"""
Módulo compartilhado de leitura de indicadores a partir de data/gold.

Usado pela API FastAPI e pelo dashboard Streamlit (dash/app.py).
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parent
GOLD_DIR = _PROJECT_ROOT / "data" / "gold"

# Indicadores e nomes de arquivo aceitos (primeiro que existir em data/gold será usado)
PARQUET_CANDIDATES: dict[str, list[str]] = {
    "selic": ["selic", "selic_m"],
    "usdbrl": ["usdbrl", "cambio_usdbrl"],
    "ibcbr": ["ibcbr", "ibc_br", "ibcb"],
    "ipca": ["ipca", "ipc"],
    "ipca15": ["ipca15"],
    "inpc": ["inpc"],
    "focus_ipca12": ["focus_ipca12"],
    "focus_selic": ["focus_selic"],
    "reservas": ["reservas"],
    "desocupacao": ["desocupacao"],
    "varejo_restrito": ["varejo_restrito"],
    "varejo_ampliado": ["varejo_ampliado"],
    "servicos": ["servicos"],
    "ipca_alimentacao": ["ipca_alimentacao"],
    "ipca_transportes": ["ipca_transportes"],
    "ipca_vestuario": ["ipca_vestuario"],
    "fipezap_locacao_preco_m2": ["fipezap_locacao_preco_m2"],
    "fipezap_locacao_mom_pct": ["fipezap_locacao_mom_pct"],
    "fipezap_venda_preco_m2": ["fipezap_venda_preco_m2"],
    "fipezap_venda_mom_pct": ["fipezap_venda_mom_pct"],
    "ivgr": ["ivgr"],
    "credito_imob_saldo_total_pf": ["credito_imob_saldo_total_pf"],
    "credito_imob_saldo_mercado_pf": ["credito_imob_saldo_mercado_pf"],
    "credito_imob_concessoes_mercado_pf": ["credito_imob_concessoes_mercado_pf"],
    "credito_imob_taxa_juros_mercado_pf": ["credito_imob_taxa_juros_mercado_pf"],
    "credito_imob_inadimplencia_mercado_pf": ["credito_imob_inadimplencia_mercado_pf"],
    "credito_consumo_saldo_pf": ["credito_consumo_saldo_pf"],
    "credito_consumo_juros_pf": ["credito_consumo_juros_pf"],
    "sinapi_custo_m2_uf": ["sinapi_custo_m2_uf"],
    "sinapi_var_mensal_uf": ["sinapi_var_mensal_uf"],
    "sinapi_var_12m_uf": ["sinapi_var_12m_uf"],
    "edu_sup_matriculas": ["edu_sup_matriculas"],
    "edu_sup_ingressantes": ["edu_sup_ingressantes"],
    "edu_sup_concluintes": ["edu_sup_concluintes"],
    "edu_sup_docentes_exercicio": ["edu_sup_docentes_exercicio"],
    "edu_sup_igc_medio": ["edu_sup_igc_medio"],
    "populacao": ["populacao"],
    "desocupacao_uf": ["desocupacao_uf"],
    "salario_real": ["salario_real"],
}
PARQUET_CANDIDATES["meta_inflacao"] = []
PARQUET_FILES = list(PARQUET_CANDIDATES.keys())

_META_INFLACAO_TARGETS: dict[int, float] = {
    2017: 4.50, 2018: 4.50, 2019: 4.25, 2020: 4.00,
    2021: 3.75, 2022: 3.50, 2023: 3.25, 2024: 3.00,
    2025: 3.00, 2026: 3.00,
}


def _generate_meta_inflacao() -> pd.DataFrame:
    """Gera série mensal da meta de inflação do CMN (dado público)."""
    rows = []
    now = pd.Timestamp.now().normalize()
    for year, target in _META_INFLACAO_TARGETS.items():
        for month in range(1, 13):
            dt = pd.Timestamp(year, month, 1)
            if dt <= now:
                rows.append({"date": dt, "value": target})
    return pd.DataFrame(rows)


def _read_parquet_file(path: Path) -> pd.DataFrame | None:
    """Lê um parquet e normaliza a coluna date para datetime. Retorna None em erro."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return None


def load_parquet(indicator_key: str) -> pd.DataFrame | None:
    """
    Carrega os dados de um indicador a partir de data/gold.

    Tenta cada nome de arquivo em PARQUET_CANDIDATES para esse indicador.
    Retorna DataFrame com date e value, ou None se nenhum arquivo for encontrado.
    """
    if indicator_key == "meta_inflacao":
        return _generate_meta_inflacao()
    for filename in PARQUET_CANDIDATES.get(indicator_key, [indicator_key]):
        path = GOLD_DIR / f"{filename}.parquet"
        df = _read_parquet_file(path)
        if df is not None and not df.empty:
            return df
    return None


def load_all_data() -> dict[str, pd.DataFrame | None]:
    """Carrega todos os parquets de data/gold (um DataFrame por indicador)."""
    return {key: load_parquet(key) for key in PARQUET_FILES}


def filter_by_date_range(
    df: pd.DataFrame | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    """Filtra o DataFrame pelo intervalo [start, end] na coluna date."""
    if df is None or df.empty or "date" not in df.columns:
        return df
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].copy()
