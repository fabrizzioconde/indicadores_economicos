"""
ETL da taxa de desocupação e rendimento (PNAD Contínua trimestral) do IBGE.

Extrai da API SIDRA:
- Taxa de desocupação (tabela 4093, variável 4099) — Brasil e por UF
- Rendimento médio nominal (tabela 5447, variável 516) — PNAD Contínua trimestral
Schema: date, value, indicator, source, uf (quando regional).
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

from config.settings import (
    DEFAULT_END_MONTH,
    DEFAULT_START_MONTH,
    GOLD_DIR,
)
from etl.bacen import save_series_to_parquet

SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"
SOURCE_LABEL = "IBGE_PNAD"
REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3

# Tabela 4093: PNAD Contínua trimestral; variável 4099 = taxa de desocupação (%).
# Período no SIDRA: AAAATTT (ano + trimestre 01-04), ex.: 202301 = 1º trim 2023.
PNAD_TABLE = "4093"
PNAD_VAR_DESOCUPACAO = "4099"

# Tabela 5436: PNAD Contínua trimestral — rendimento médio mensal real (R$).
# Variável 5932 = Rendimento médio mensal real (já deflacionado = salário real).
PNAD_RENDA_TABLE = "5436"
PNAD_VAR_RENDA = "5932"

# Map IBGE UF code (D1C) -> UF sigla
UF_BY_IBGE_CODE: dict[str, str] = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}


def _month_to_quarter_code(ym: str) -> str:
    """Converte YYYY-MM para código trimestral SIDRA AAAATTT (ex.: 2012-04 -> 201202)."""
    if not ym or len(ym) < 7:
        return ""
    try:
        year = int(ym[:4])
        month = int(ym[5:7])
        quarter = (month - 1) // 3 + 1
        return f"{year}{quarter:02d}"
    except (ValueError, TypeError):
        return ""


def _quarter_code_to_date(code: str) -> datetime | None:
    """Converte código trimestral (202301) para o primeiro dia do trimestre (2023-01-01)."""
    if not code or len(str(code).strip()) != 6:
        return None
    try:
        s = str(code).strip()
        year = int(s[:4])
        q = int(s[4:6])
        if 1 <= q <= 4:
            month = (q - 1) * 3 + 1
            return datetime(year, month, 1)
    except (ValueError, TypeError):
        pass
    return None


def _quarter_range(start_ym: str, end_ym: str) -> tuple[str, str]:
    """Retorna (primeiro_trimestre, último_trimestre) em formato AAAATTT para o intervalo de meses."""
    start_q = _month_to_quarter_code(start_ym)
    end_q = _month_to_quarter_code(end_ym)
    return (start_q, end_q)


def fetch_desocupacao(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Busca a taxa de desocupação (PNAD Contínua trimestral) na API SIDRA.

    Args:
        start_date: Mês inicial 'YYYY-MM' (será convertido para trimestre).
        end_date: Mês final 'YYYY-MM'.

    Returns:
        DataFrame com colunas date (primeiro dia do trimestre), value (%), indicator, source.
    """
    start_ym = (start_date or "").strip() or DEFAULT_START_MONTH
    end_ym = (end_date or "").strip() or DEFAULT_END_MONTH
    if not end_ym:
        end_ym = datetime.now().strftime("%Y-%m")

    start_q, end_q = _quarter_range(start_ym, end_ym)
    if not start_q or not end_q:
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    period_param = f"{start_q}-{end_q}"
    url = f"{SIDRA_BASE_URL}/t/{PNAD_TABLE}/n1/1/v/{PNAD_VAR_DESOCUPACAO}/p/{period_param}"
    params = {"formato": "json"}

    print(f"[IBGE PNAD] Requisitando taxa de desocupação (tabela {PNAD_TABLE}) de {start_q} a {end_q}.")
    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            break
        except RequestException as e:
            if attempt < REQUEST_RETRIES - 1:
                print(f"[IBGE PNAD] Erro de conexão (tentativa {attempt + 1}/{REQUEST_RETRIES}), aguardando {RETRY_SLEEP_SECONDS}s...")
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                raise

    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    # Resposta: linhas com D3C = período (202301), V = valor. D4C = 6794 = Total (se existir, filtrar).
    rows = [r for r in data if isinstance(r, dict) and "V" in r and "D3C" in r]
    if rows and "D4C" in rows[0]:
        rows = [r for r in rows if str(r.get("D4C")) == "6794"]
    out = []
    for r in rows:
        period_code = r.get("D3C")
        val = r.get("V")
        dt = _quarter_code_to_date(str(period_code) if period_code is not None else "")
        if dt is None:
            continue
        try:
            v_float = float(str(val).replace(",", "."))
        except (TypeError, ValueError):
            continue
        out.append({
            "date": dt,
            "value": v_float,
            "indicator": "DESOCUPACAO",
            "source": SOURCE_LABEL,
        })

    df = pd.DataFrame(out)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    print(f"[IBGE PNAD] Taxa de desocupação: {len(df)} registros.")
    return df


def get_desocupacao_mensal(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Retorna a série trimestral da taxa de desocupação (%). Datas em YYYY-MM."""
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH
    return fetch_desocupacao(start_date=start, end_date=end)


def _max_quarter_from_parquet(gold_dir: Path, filename: str) -> str | None:
    """Retorna o trimestre (AAAATTT) da última data no parquet, ou None."""
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
        t = pd.Timestamp(max_val)
        q = (t.month - 1) // 3 + 1
        return f"{t.year}{q:02d}"
    except Exception:
        return None


def _next_quarter(quarter_code: str) -> str:
    """Retorna o trimestre seguinte (202304 -> 202401)."""
    if not quarter_code or len(quarter_code) != 6:
        return quarter_code
    year = int(quarter_code[:4])
    q = int(quarter_code[4:6])
    if q == 4:
        return f"{year + 1}01"
    return f"{year}{q + 1:02d}"


def get_desocupacao_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série desocupação de forma incremental (a partir do último trimestre em gold)."""
    gold = gold_dir or GOLD_DIR
    end_ym = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_q = _max_quarter_from_parquet(gold, "desocupacao")
    if not max_q:
        return fetch_desocupacao(start_date=DEFAULT_START_MONTH, end_date=end_ym)
    start_next = _next_quarter(max_q)
    q = int(start_next[4:6])
    start_ym = f"{start_next[:4]}-{(q - 1) * 3 + 1:02d}"
    if start_ym > end_ym:
        return pd.read_parquet(gold / "desocupacao.parquet")
    new_df = fetch_desocupacao(start_date=start_ym, end_date=end_ym)
    if new_df.empty:
        return pd.read_parquet(gold / "desocupacao.parquet")
    existing = pd.read_parquet(gold / "desocupacao.parquet")
    existing["date"] = pd.to_datetime(existing["date"])
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return combined


def fetch_salario_real() -> pd.DataFrame:
    """Busca salário real (tabela 5436, rendimento médio mensal real) na API SIDRA."""
    url = f"{SIDRA_BASE_URL}/t/{PNAD_RENDA_TABLE}/n1/1/v/{PNAD_VAR_RENDA}/p/all"
    params = {"formato": "json"}

    print(f"[IBGE PNAD] Requisitando salário real (tabela {PNAD_RENDA_TABLE}).")
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
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    rows = [r for r in data if isinstance(r, dict) and "V" in r and "D3C" in r]
    if rows and "D4C" in rows[0]:
        rows = [r for r in rows if str(r.get("D4C")) == "6794"]
    out = []
    for r in rows:
        period_code = str(r.get("D3C", "")).strip()
        dt = _quarter_code_to_date(period_code)
        if dt is None:
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
            "date": dt,
            "value": v,
            "indicator": "SALARIO_REAL",
            "source": SOURCE_LABEL,
        })

    df = pd.DataFrame(out)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    print(f"[IBGE PNAD] Salário real: {len(df)} registros.")
    return df


def fetch_desocupacao_uf(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Busca taxa de desocupação por UF (tabela 4093, n3/all) na API SIDRA."""
    start_ym = (start_date or "").strip() or DEFAULT_START_MONTH
    end_ym = (end_date or "").strip() or DEFAULT_END_MONTH
    if not end_ym:
        end_ym = datetime.now().strftime("%Y-%m")

    start_q, end_q = _quarter_range(start_ym, end_ym)
    if not start_q or not end_q:
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf"])

    period_param = f"{start_q}-{end_q}"
    url = f"{SIDRA_BASE_URL}/t/{PNAD_TABLE}/n3/all/v/{PNAD_VAR_DESOCUPACAO}/p/{period_param}"
    params = {"formato": "json"}

    print(f"[IBGE PNAD] Requisitando desocupação por UF (tabela {PNAD_TABLE}) de {start_q} a {end_q}.")
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
    if rows and "D4C" in rows[0]:
        rows = [r for r in rows if str(r.get("D4C")) == "6794"]
    out = []
    for r in rows:
        period_code = str(r.get("D3C", "")).strip()
        dt = _quarter_code_to_date(period_code)
        if dt is None:
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
            "date": dt,
            "value": v,
            "indicator": "DESOCUPACAO",
            "source": SOURCE_LABEL,
            "uf": uf,
        })

    df = pd.DataFrame(out)
    df = df.drop_duplicates(subset=["date", "uf"]).sort_values(["uf", "date"]).reset_index(drop=True)
    print(f"[IBGE PNAD] Desocupação por UF: {len(df)} registros.")
    return df


def run_pnad_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    layer: str = "gold",
) -> None:
    """Executa o ETL da taxa de desocupação e salva em data/gold/desocupacao.parquet."""
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH
    df = get_desocupacao_mensal(start_date=start, end_date=end)
    if df is not None and not df.empty:
        save_series_to_parquet(df, "desocupacao", layer=layer)


def run_salario_real_etl(layer: str = "gold") -> None:
    """Executa o ETL do salário real e salva em data/gold/salario_real.parquet."""
    df = fetch_salario_real()
    if df is not None and not df.empty:
        save_series_to_parquet(df, "salario_real", layer=layer)


def run_desocupacao_uf_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    layer: str = "gold",
) -> None:
    """Executa o ETL da desocupação por UF e salva em data/gold/desocupacao_uf.parquet."""
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH
    df = fetch_desocupacao_uf(start_date=start, end_date=end)
    if df is not None and not df.empty:
        save_series_to_parquet(df, "desocupacao_uf", layer=layer)


if __name__ == "__main__":
    print("Executando ETL IBGE PNAD (desocupação, salário real, desocupação UF)...")
    run_pnad_etl(layer="gold")
    run_salario_real_etl(layer="gold")
    run_desocupacao_uf_etl(layer="gold")
