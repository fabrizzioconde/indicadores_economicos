"""
ETL de indicadores de crédito ao consumidor (BACEN/SGS).

Séries escolhidas (todas mensais):
- Saldo da carteira de crédito - Pessoas físicas - Total (R$ milhões)
- Taxa média de juros - Operações de crédito - Pessoas físicas - Total (% a.m.)

Schema padrão do módulo BACEN:
    date (datetime), value (float), series_code (str), source (str)
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

if __name__ == "__main__":
    _project_root = Path(__file__).resolve().parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

from config.settings import (
    BACEN_CREDITO_CONSUMO_SERIES,
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    GOLD_DIR,
)
from etl.bacen import _fetch_series_incremental, fetch_bcb_series


def _get_code(key: str, fallback: str) -> str:
    return (BACEN_CREDITO_CONSUMO_SERIES.get(key) or fallback).strip()


def get_credito_consumo_saldo_pf(
    start_date: str | None = None, end_date: str | None = None
) -> pd.DataFrame:
    """Saldo da carteira de crédito - PF - Total (R$ milhões)."""
    code = _get_code("credito_consumo_saldo_pf", "20542")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_consumo_juros_pf(
    start_date: str | None = None, end_date: str | None = None
) -> pd.DataFrame:
    """Taxa média de juros - Operações de crédito - PF - Total (% a.m.)."""
    code = _get_code("credito_consumo_juros_pf", "4189")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_consumo_saldo_pf_incremental(
    end_date: str | None = None, gold_dir: Path | None = None
) -> pd.DataFrame:
    """ETL incremental do saldo de crédito ao consumidor."""
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_consumo_saldo_pf", "20542")
    return _fetch_series_incremental(
        code, "credito_consumo_saldo_pf", DEFAULT_START_DATE, end, gold
    )


def get_credito_consumo_juros_pf_incremental(
    end_date: str | None = None, gold_dir: Path | None = None
) -> pd.DataFrame:
    """ETL incremental da taxa de juros do crédito ao consumidor."""
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_consumo_juros_pf", "4189")
    return _fetch_series_incremental(
        code, "credito_consumo_juros_pf", DEFAULT_START_DATE, end, gold
    )
