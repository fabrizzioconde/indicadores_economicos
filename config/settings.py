"""
Configurações centralizadas do macro_insights_mvp.

Centraliza códigos de séries (BACEN e IBGE), datas padrão do ETL
e caminhos das pastas de dados. Use a instância `settings` ou
as constantes exportadas para acessar os valores.

Como alterar as datas padrão:
    Edite `Settings.default_start_date` e `Settings.default_end_date`
    (ou as constantes DEFAULT_START_DATE / DEFAULT_END_DATE).
    Para "até hoje", deixe default_end_date vazio ("").

Como adicionar novas séries no futuro:
    BACEN: inclua uma entrada em `Settings.bacen_series` com a chave
    desejada e o código numérico da série no SGS (api.bcb.gov.br).
    IBGE: inclua em `Settings.ibge_ipca_config` com tabela e variável
    SIDRA (apisidra.ibge.gov.br), ou crie um novo dicionário de config
    no mesmo padrão.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import TypedDict


# -----------------------------------------------------------------------------
# Raiz do projeto (pasta onde está pyproject.toml)
# -----------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent


class BacenSeriesConfig(TypedDict):
    """Códigos de séries do BACEN (SGS). Chave lógica -> código numérico da API."""

    selic: str
    cambio: str
    ibc_br: str


class IbgeIpcaEntry(TypedDict):
    """Entrada de config do IPCA/IPCA-15: tabela e variável SIDRA."""

    table: str
    variable: str


class IbgeIpcaConfig(TypedDict):
    """Configuração por indicador (IPCA, IPCA15)."""

    IPCA: IbgeIpcaEntry
    IPCA15: IbgeIpcaEntry


class Settings:
    """
    Configuração centralizada do projeto.

    Agrupa códigos de séries, datas padrão e caminhos de dados.
    Use a instância global `settings` ou as constantes de módulo
    (DATA_DIR, BACEN_SERIES, etc.) que espelham estes valores.
    """

    # -------------------------------------------------------------------------
    # Códigos de séries do BACEN (API SGS - api.bcb.gov.br)
    # -------------------------------------------------------------------------
    # SELIC diária: taxa meta (código 432, divulgação mensal no SGS).
    # Câmbio USD/BRL: dólar americano venda (código 1, série diária).
    # IBC-Br: Índice de Atividade Econômica do BCB com ajuste sazonal (código 24364).
    bacen_series: BacenSeriesConfig = {
        "selic": "432",
        "cambio": "1",
        "ibc_br": "24364",
    }

    # -------------------------------------------------------------------------
    # IPCA e IPCA-15 – API SIDRA (apisidra.ibge.gov.br)
    # -------------------------------------------------------------------------
    # Tabela 1737: IPCA série histórica Brasil; variável 63 = variação mensal (%).
    # Tabela 3065: IPCA-15 série histórica Brasil; variável 355 = variação mensal (%).
    ibge_ipca_config: IbgeIpcaConfig = {
        "IPCA": {"table": "1737", "variable": "63"},
        "IPCA15": {"table": "3065", "variable": "355"},
    }

    # -------------------------------------------------------------------------
    # Datas padrão para o ETL
    # -------------------------------------------------------------------------
    # Formato YYYY-MM-DD para séries diárias (BACEN). End vazio = "até hoje".
    default_start_date: str = "2010-01-01"
    default_end_date: str = ""  # vazio: use get_date_range() ou "hoje" na lógica do ETL

    # Formato YYYY-MM para séries mensais (IBGE). End vazio = mês atual.
    default_start_month: str = "2010-01"
    default_end_month: str = ""

    # -------------------------------------------------------------------------
    # Caminhos de dados (relativos à raiz do projeto)
    # -------------------------------------------------------------------------
    _data_dir: Path
    _raw_dir: Path
    _processed_dir: Path
    _gold_dir: Path

    def __init__(self) -> None:
        self._data_dir = PROJECT_ROOT / "data"
        self._raw_dir = self._data_dir / "raw"
        self._processed_dir = self._data_dir / "processed"
        self._gold_dir = self._data_dir / "gold"
        self._ensure_data_dirs()

    def _ensure_data_dirs(self) -> None:
        """Cria as pastas de dados se não existirem (ao importar o módulo)."""
        for folder in (self._data_dir, self._raw_dir, self._processed_dir, self._gold_dir):
            folder.mkdir(parents=True, exist_ok=True)

    @property
    def data_dir(self) -> Path:
        """Raiz da pasta de dados (data/)."""
        return self._data_dir

    @property
    def raw_dir(self) -> Path:
        """Pasta de dados brutos (data/raw/)."""
        return self._raw_dir

    @property
    def processed_dir(self) -> Path:
        """Pasta de dados processados (data/processed/)."""
        return self._processed_dir

    @property
    def gold_dir(self) -> Path:
        """Pasta de dados prontos para consumo (data/gold/)."""
        return self._gold_dir

    def get_date_range(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> tuple[str, str]:
        """
        Retorna o par (data_inicial, data_final) para uso no ETL.

        Se end estiver vazio, usa a data de hoje (date.today()) no formato
        YYYY-MM-DD. start e end no formato YYYY-MM-DD.

        Args:
            start: Data inicial; se None, usa default_start_date.
            end: Data final; se None ou "", usa data de hoje.

        Returns:
            Tupla (start_date, end_date) em formato YYYY-MM-DD.
        """
        start_date = (start or self.default_start_date).strip()
        end_date = (end or self.default_end_date).strip()
        if not end_date:
            end_date = date.today().strftime("%Y-%m-%d")
        return (start_date, end_date)

    def get_month_range(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> tuple[str, str]:
        """
        Retorna o par (mês_inicial, mês_final) em YYYY-MM para séries mensais.

        Se end estiver vazio, usa o mês atual.

        Args:
            start: Mês inicial (YYYY-MM); None = default_start_month.
            end: Mês final (YYYY-MM); None ou "" = mês atual.

        Returns:
            Tupla (start_month, end_month).
        """
        start_month = (start or self.default_start_month).strip()
        end_month = (end or self.default_end_month).strip()
        if not end_month:
            end_month = datetime.now().strftime("%Y-%m")
        return (start_month, end_month)


# Instância global de configuração
settings = Settings()

# -----------------------------------------------------------------------------
# Constantes de módulo (compatibilidade com imports existentes)
# -----------------------------------------------------------------------------
DATA_DIR: Path = settings.data_dir
RAW_DIR: Path = settings.raw_dir
PROCESSED_DIR: Path = settings.processed_dir
GOLD_DIR: Path = settings.gold_dir

BACEN_SERIES: BacenSeriesConfig = settings.bacen_series
IBGE_IPCA_CONFIG: IbgeIpcaConfig = settings.ibge_ipca_config

DEFAULT_START_DATE: str = settings.default_start_date
DEFAULT_END_DATE: str = settings.default_end_date
DEFAULT_START_MONTH: str = settings.default_start_month
DEFAULT_END_MONTH: str = settings.default_end_month


def get_date_range(
    start: str | None = None,
    end: str | None = None,
) -> tuple[str, str]:
    """
    Retorna (data_inicial, data_final) para o ETL com base nas configurações.

    Se end for omitido ou vazio, a data final será hoje (date.today()).
    Formato das strings: YYYY-MM-DD.

    Args:
        start: Data inicial; None usa DEFAULT_START_DATE.
        end: Data final; None ou "" usa a data de hoje.

    Returns:
        Tupla (start_date, end_date).
    """
    return settings.get_date_range(start=start, end=end)


def get_month_range(
    start: str | None = None,
    end: str | None = None,
) -> tuple[str, str]:
    """
    Retorna (mês_inicial, mês_final) em YYYY-MM para séries mensais (ex.: IPCA).

    Se end for omitido ou vazio, usa o mês atual.

    Args:
        start: Mês inicial (YYYY-MM); None usa DEFAULT_START_MONTH.
        end: Mês final (YYYY-MM); None ou "" usa o mês atual.

    Returns:
        Tupla (start_month, end_month).
    """
    return settings.get_month_range(start=start, end=end)
