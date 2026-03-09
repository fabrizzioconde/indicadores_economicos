"""
ETL do SINAPI (IBGE/SIDRA) — custos e variações do custo de construção (m²).

Fonte: SIDRA tabela 2296 (SINAPI): custo médio m² e variações (%).
Cobertura: Brasil / Grande Região / UF. Para o app, extraímos por UF (N3).

Schema (compatível com o app, com dimensão adicional 'uf'):
    date (datetime), uf (str), value (float), indicator (str), source (str)
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

from config.settings import DEFAULT_END_MONTH, DEFAULT_START_MONTH, GOLD_DIR


SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"
SOURCE_LABEL = "IBGE_SINAPI"
REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3

SINAPI_TABLE = "2296"

# Variáveis (tabela 2296)
VAR_CUSTO_M2_RS = "48"
VAR_VAR_MENSAL_PCT = "1196"
VAR_VAR_12M_PCT = "1198"

# Map IBGE UF code (D1C) -> UF sigla
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


def _month_to_period(ym: str) -> str:
    if not ym or len(ym) < 7:
        return ""
    return ym.strip()[:7].replace("-", "")


def _period_to_date(period_code: str) -> datetime | None:
    if not period_code or len(str(period_code).strip()) != 6:
        return None
    try:
        s = str(period_code).strip()
        year = int(s[:4])
        month = int(s[4:6])
        if 1 <= month <= 12:
            return datetime(year, month, 1)
    except (ValueError, TypeError):
        return None
    return None


def _max_month_from_parquet(gold_dir: Path, filename: str) -> str | None:
    path = gold_dir / f"{filename}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty or "date" not in df.columns:
            return None
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        max_val = df["date"].max()
        if pd.isna(max_val):
            return None
        return pd.Timestamp(max_val).strftime("%Y-%m")
    except Exception:
        return None


def _next_month(ym: str) -> str:
    if not ym or len(ym) < 7:
        return ym
    year, month = int(ym[:4]), int(ym[5:7])
    if month == 12:
        return f"{year + 1}-01"
    return f"{year}-{month + 1:02d}"


def fetch_sinapi_uf(
    *,
    variable: str,
    start_month: str,
    end_month: str,
) -> pd.DataFrame:
    """
    Busca série do SINAPI por UF (n3/all) para uma variável da tabela 2296.
    """
    start_ym = (start_month or "").strip() or DEFAULT_START_MONTH
    end_ym = (end_month or "").strip() or DEFAULT_END_MONTH
    if not end_ym:
        end_ym = datetime.now().strftime("%Y-%m")

    start_p = _month_to_period(start_ym)
    end_p = _month_to_period(end_ym)
    if not start_p or not end_p:
        raise ValueError(f"Datas devem estar em YYYY-MM. start={start_month!r} end={end_month!r}")

    url = f"{SIDRA_BASE_URL}/t/{SINAPI_TABLE}/n3/all/v/{variable}/p/{start_p}-{end_p}"
    params = {"formato": "json"}

    print(f"[IBGE SINAPI] Requisitando tabela {SINAPI_TABLE} var {variable} de {start_ym} a {end_ym} (UF).")
    for attempt in range(REQUEST_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            break
        except RequestException:
            if attempt < REQUEST_RETRIES - 1:
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                raise

    if resp.status_code != 200:
        resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        return pd.DataFrame(columns=["date", "uf", "value"])

    rows = [r for r in data if isinstance(r, dict) and str(r.get("D3C", "")).isdigit() and "V" in r and "D1C" in r]
    out: list[dict[str, object]] = []
    for r in rows:
        dt = _period_to_date(str(r.get("D3C", "")).strip())
        if dt is None:
            continue
        uf_code = str(r.get("D1C", "")).strip()
        uf = UF_BY_IBGE_CODE.get(uf_code)
        if not uf:
            continue
        val = r.get("V")
        if val is None:
            continue
        sval = str(val).strip()
        if sval in ("..", "...", "X", "-", ""):
            continue
        try:
            v = float(sval.replace(",", "."))
        except (TypeError, ValueError):
            continue
        out.append({"date": dt, "uf": uf, "value": v})

    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame(columns=["date", "uf", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset=["date", "uf"]).sort_values(["uf", "date"]).reset_index(drop=True)
    return df


def get_sinapi_custo_m2_uf(start_month: str | None = None, end_month: str | None = None) -> pd.DataFrame:
    df = fetch_sinapi_uf(variable=VAR_CUSTO_M2_RS, start_month=start_month or DEFAULT_START_MONTH, end_month=end_month or DEFAULT_END_MONTH)
    if df.empty:
        return pd.DataFrame(columns=["date", "uf", "value", "indicator", "source"])
    df = df.copy()
    df["indicator"] = "SINAPI_CUSTO_M2_RS"
    df["source"] = SOURCE_LABEL
    return df[["date", "uf", "value", "indicator", "source"]]


def get_sinapi_var_mensal_uf(start_month: str | None = None, end_month: str | None = None) -> pd.DataFrame:
    df = fetch_sinapi_uf(variable=VAR_VAR_MENSAL_PCT, start_month=start_month or DEFAULT_START_MONTH, end_month=end_month or DEFAULT_END_MONTH)
    if df.empty:
        return pd.DataFrame(columns=["date", "uf", "value", "indicator", "source"])
    df = df.copy()
    df["indicator"] = "SINAPI_VAR_MENSAL_PCT"
    df["source"] = SOURCE_LABEL
    return df[["date", "uf", "value", "indicator", "source"]]


def get_sinapi_var_12m_uf(start_month: str | None = None, end_month: str | None = None) -> pd.DataFrame:
    df = fetch_sinapi_uf(variable=VAR_VAR_12M_PCT, start_month=start_month or DEFAULT_START_MONTH, end_month=end_month or DEFAULT_END_MONTH)
    if df.empty:
        return pd.DataFrame(columns=["date", "uf", "value", "indicator", "source"])
    df = df.copy()
    df["indicator"] = "SINAPI_VAR_12M_PCT"
    df["source"] = SOURCE_LABEL
    return df[["date", "uf", "value", "indicator", "source"]]


def _incremental(filename: str, fetcher, end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, filename)
    if not max_ym:
        return fetcher(DEFAULT_START_MONTH, end)
    start_new = _next_month(max_ym)
    if start_new > end:
        df = pd.read_parquet(gold / f"{filename}.parquet")
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    new_df = fetcher(start_new, end)
    if new_df is None or new_df.empty:
        df = pd.read_parquet(gold / f"{filename}.parquet")
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    existing = pd.read_parquet(gold / f"{filename}.parquet")
    if "date" in existing.columns:
        existing["date"] = pd.to_datetime(existing["date"])
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"])
    combined = combined.drop_duplicates(subset=["date", "uf"]).sort_values(["uf", "date"]).reset_index(drop=True)
    return combined


def get_sinapi_custo_m2_uf_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental("sinapi_custo_m2_uf", get_sinapi_custo_m2_uf, end_month=end_month, gold_dir=gold_dir)


def get_sinapi_var_mensal_uf_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental("sinapi_var_mensal_uf", get_sinapi_var_mensal_uf, end_month=end_month, gold_dir=gold_dir)


def get_sinapi_var_12m_uf_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental("sinapi_var_12m_uf", get_sinapi_var_12m_uf, end_month=end_month, gold_dir=gold_dir)

