"""
ETL da população residente estimada (IBGE EstimaPop).

Fonte: SIDRA tabela 6579 (população residente estimada).
Cobertura: Brasil (n1/1) e UFs (n3/all).
Schema: date (primeiro dia do ano), value (habitantes), indicator, source, uf (opcional).
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from requests.exceptions import RequestException

if __name__ == "__main__":
    _project_root = Path(__file__).resolve().parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

from config.settings import GOLD_DIR
from etl.bacen import save_series_to_parquet

SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"
SOURCE_LABEL = "IBGE_ESTIMAPOP"
REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3

POP_TABLE = "6579"
POP_VAR = "9324"  # População residente estimada

# Map IBGE UF code (D1C) -> UF sigla (mesmo do SINAPI)
UF_BY_IBGE_CODE: dict[str, str] = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}


def _year_to_date(year: int) -> datetime:
    """Primeiro dia do ano."""
    return datetime(year, 1, 1)


def fetch_populacao_brasil() -> pd.DataFrame:
    """Busca população do Brasil (n1/1) na API SIDRA."""
    url = f"{SIDRA_BASE_URL}/t/{POP_TABLE}/n1/1/v/{POP_VAR}/p/all"
    params = {"formato": "json"}

    print(f"[IBGE População] Requisitando Brasil (tabela {POP_TABLE}).")
    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            break
        except RequestException:
            if attempt < REQUEST_RETRIES - 1:
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                raise

    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf"])

    rows = [r for r in data if isinstance(r, dict) and "V" in r and "D3C" in r]
    out = []
    for r in rows:
        period = str(r.get("D3C", "")).strip()
        if not period.isdigit() or len(period) != 4:
            continue
        try:
            year = int(period)
            if year < 2000 or year > 2030:
                continue
        except ValueError:
            continue
        val = r.get("V")
        if val is None:
            continue
        sval = str(val).replace(",", ".")
        if sval in ("..", "...", "X", "-", ""):
            continue
        try:
            v = float(sval)
        except (TypeError, ValueError):
            continue
        out.append({
            "date": _year_to_date(year),
            "value": v,
            "indicator": "POPULACAO",
            "source": SOURCE_LABEL,
            "uf": "BR",
        })

    return pd.DataFrame(out)


def fetch_populacao_uf() -> pd.DataFrame:
    """Busca população por UF (n3/all) na API SIDRA."""
    url = f"{SIDRA_BASE_URL}/t/{POP_TABLE}/n3/all/v/{POP_VAR}/p/all"
    params = {"formato": "json"}

    print(f"[IBGE População] Requisitando UFs (tabela {POP_TABLE}).")
    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            break
        except RequestException:
            if attempt < REQUEST_RETRIES - 1:
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                raise

    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf"])

    rows = [r for r in data if isinstance(r, dict) and "V" in r and "D3C" in r and "D1C" in r]
    out = []
    for r in rows:
        period = str(r.get("D3C", "")).strip()
        if not period.isdigit() or len(period) != 4:
            continue
        try:
            year = int(period)
            if year < 2000 or year > 2030:
                continue
        except ValueError:
            continue
        uf_code = str(r.get("D1C", "")).strip()
        uf = UF_BY_IBGE_CODE.get(uf_code)
        if not uf:
            continue
        val = r.get("V")
        if val is None:
            continue
        sval = str(val).replace(",", ".")
        if sval in ("..", "...", "X", "-", ""):
            continue
        try:
            v = float(sval)
        except (TypeError, ValueError):
            continue
        out.append({
            "date": _year_to_date(year),
            "value": v,
            "indicator": "POPULACAO",
            "source": SOURCE_LABEL,
            "uf": uf,
        })

    return pd.DataFrame(out)


def get_populacao() -> pd.DataFrame:
    """Retorna população do Brasil e por UF combinados."""
    df_br = fetch_populacao_brasil()
    df_uf = fetch_populacao_uf()
    if df_br.empty and df_uf.empty:
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf"])
    dfs = [d for d in [df_br, df_uf] if not d.empty]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["date", "uf"]).sort_values(["uf", "date"]).reset_index(drop=True)
    print(f"[IBGE População] Total: {len(df)} registros.")
    return df


def run_populacao_etl(layer: str = "gold") -> None:
    """Executa o ETL da população e salva em data/gold/populacao.parquet."""
    df = get_populacao()
    if df is not None and not df.empty:
        save_series_to_parquet(df, "populacao", layer=layer)


if __name__ == "__main__":
    print("Executando ETL IBGE População...")
    run_populacao_etl(layer="gold")
