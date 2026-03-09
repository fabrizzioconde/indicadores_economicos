"""
ETL de indicadores de crédito imobiliário (BACEN/SGS).

Séries escolhidas (todas mensais):
- Saldo da carteira (PF) – financiamento imobiliário total
- Saldo da carteira (PF) – financiamento imobiliário com taxas de mercado
- Concessões (PF) – financiamento imobiliário com taxas de mercado
- Taxa média de juros (PF) – financiamento imobiliário com taxas de mercado
- Inadimplência (PF) – financiamento imobiliário com taxas de mercado

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

from config.settings import BACEN_RE_SERIES, DEFAULT_END_DATE, DEFAULT_START_DATE, GOLD_DIR
from etl.bacen import _fetch_series_incremental, fetch_bcb_series


def _get_code(key: str, fallback: str) -> str:
    return (BACEN_RE_SERIES.get(key) or fallback).strip()


def get_credito_imob_saldo_total_pf(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    code = _get_code("credito_imob_saldo_total_pf", "20612")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_imob_saldo_mercado_pf(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    code = _get_code("credito_imob_saldo_mercado_pf", "20611")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_imob_concessoes_mercado_pf(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    code = _get_code("credito_imob_concessoes_mercado_pf", "20702")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_imob_taxa_juros_mercado_pf(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    code = _get_code("credito_imob_taxa_juros_mercado_pf", "20772")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_imob_inadimplencia_mercado_pf(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    code = _get_code("credito_imob_inadimplencia_mercado_pf", "21149")
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    return fetch_bcb_series(code, start, end[:10])


def get_credito_imob_saldo_total_pf_incremental(end_date: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_imob_saldo_total_pf", "20612")
    return _fetch_series_incremental(code, "credito_imob_saldo_total_pf", DEFAULT_START_DATE, end, gold)


def get_credito_imob_saldo_mercado_pf_incremental(end_date: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_imob_saldo_mercado_pf", "20611")
    return _fetch_series_incremental(code, "credito_imob_saldo_mercado_pf", DEFAULT_START_DATE, end, gold)


def get_credito_imob_concessoes_mercado_pf_incremental(end_date: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_imob_concessoes_mercado_pf", "20702")
    return _fetch_series_incremental(code, "credito_imob_concessoes_mercado_pf", DEFAULT_START_DATE, end, gold)


def get_credito_imob_taxa_juros_mercado_pf_incremental(end_date: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_imob_taxa_juros_mercado_pf", "20772")
    return _fetch_series_incremental(code, "credito_imob_taxa_juros_mercado_pf", DEFAULT_START_DATE, end, gold)


def get_credito_imob_inadimplencia_mercado_pf_incremental(end_date: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = _get_code("credito_imob_inadimplencia_mercado_pf", "21149")
    return _fetch_series_incremental(code, "credito_imob_inadimplencia_mercado_pf", DEFAULT_START_DATE, end, gold)

