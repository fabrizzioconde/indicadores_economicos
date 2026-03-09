"""
ETL de IPCA e IPCA-15 do IBGE.

Extrai índices de preços (variação mensal) da API SIDRA do IBGE
(apisidra.ibge.gov.br), normaliza para o mesmo schema do BACEN
(date, value, indicator, source) e salva em parquet.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from requests.exceptions import RequestException

# Permite importar config e etl.bacen quando o módulo é executado como script
if __name__ == "__main__":
    _project_root = Path(__file__).resolve().parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

from config.settings import (
    DEFAULT_END_MONTH,
    DEFAULT_START_MONTH,
    GOLD_DIR,
    IBGE_IPCA_CONFIG,
)
from etl.bacen import save_series_to_parquet

# URL base da API SIDRA (parâmetros: t=tabela, n1=1 Brasil, v=variável, p=período)
SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"
SOURCE_LABEL = "IBGE_IPCA"
# Timeout e tentativas para requisições (API pode ser lenta)
REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3


def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que o DataFrame tenha coluna 'date' (datetime). Aceita 'data' como alias."""
    if df is None or df.empty:
        return df
    df = df.copy()
    if "date" not in df.columns and "data" in df.columns:
        df = df.rename(columns={"data": "date"})
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _max_month_from_parquet(gold_dir: Path, filename: str) -> str | None:
    """
    Retorna o mês (YYYY-MM) da última data no parquet em gold_dir, ou None se não existir/vazio.
    Usado para carga incremental: buscar apenas a partir do mês seguinte.
    """
    path = gold_dir / f"{filename}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty or "date" not in df.columns:
            return None
        # Garante coluna date para evitar KeyError em parquets com schema antigo
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        if df["date"].isna().all():
            return None
        max_val = df["date"].max()
        if pd.isna(max_val):
            return None
        return pd.Timestamp(max_val).strftime("%Y-%m")
    except Exception:
        return None


def _next_month(ym: str) -> str:
    """Retorna o mês seguinte a YYYY-MM no formato YYYY-MM."""
    if not ym or len(ym) < 7:
        return ym
    year, month = int(ym[:4]), int(ym[5:7])
    if month == 12:
        return f"{year + 1}-01"
    return f"{year}-{month + 1:02d}"


def _month_to_period(ym: str) -> str:
    """
    Converte 'YYYY-MM' para o código de período da SIDRA (AAAAMM).

    Args:
        ym: Ano-mês no formato 'YYYY-MM'.

    Returns:
        Código no formato 'AAAAMM' (ex.: '201001').
    """
    if not ym or len(ym) < 7:
        return ""
    return ym.strip()[:7].replace("-", "")


def _period_to_date(period_code: str) -> datetime | None:
    """
    Converte código de período SIDRA (AAAAMM) em data no dia 01 do mês.

    Args:
        period_code: Código 'AAAAMM' (ex.: '201001').

    Returns:
        datetime no dia 01 do mês, ou None se inválido.
    """
    if not period_code or len(str(period_code).strip()) != 6:
        return None
    try:
        s = str(period_code).strip()
        year = int(s[:4])
        month = int(s[4:6])
        if 1 <= month <= 12:
            return datetime(year, month, 1)
    except (ValueError, TypeError):
        pass
    return None


def fetch_ibge_ipca_series(
    indicator: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Busca série de IPCA ou IPCA-15 na API SIDRA e retorna DataFrame normalizado.

    Utiliza a API do IBGE (apisidra.ibge.gov.br). Os parâmetros de data
    devem estar no formato 'YYYY-MM'. O período é convertido para o formato
    da API (AAAAMM) e a resposta JSON é parseada para colunas padronizadas:
    date (dia 01 do mês), value, indicator, source.

    Args:
        indicator: Nome do indicador: 'IPCA', 'IPCA-15' / 'IPCA15' ou 'INPC'.
        start_date: Data inicial no formato 'YYYY-MM'.
        end_date: Data final no formato 'YYYY-MM' (vazio = mês atual).

    Returns:
        DataFrame com colunas: date (datetime), value (float), indicator (str), source (str).

    Raises:
        ValueError: Se o indicador não estiver em config ou a resposta não for uma lista.
        requests.RequestException: Em falha de rede ou HTTP.
    """
    # Normaliza indicador para chave do config (IPCA, IPCA15 ou INPC)
    ind = indicator.upper().strip().replace("-", "")
    if ind == "INPC":
        key = "INPC"
    elif ind == "IPCA15":
        key = "IPCA15"
    else:
        key = "IPCA"
    config = IBGE_IPCA_CONFIG.get(key)
    if not config:
        raise ValueError(
            f"Indicador '{indicator}' não configurado em config.settings.IBGE_IPCA_CONFIG. "
            "Use 'IPCA', 'IPCA15' ou 'INPC'."
        )

    table = config["table"]
    variable = config["variable"]

    start_ym = (start_date or "").strip() or DEFAULT_START_MONTH
    end_ym = (end_date or "").strip() or DEFAULT_END_MONTH
    if not end_ym:
        end_ym = datetime.now().strftime("%Y-%m")

    start_period = _month_to_period(start_ym)
    end_period = _month_to_period(end_ym)
    if not start_period or not end_period:
        raise ValueError(
            f"Datas devem estar no formato YYYY-MM. Recebido: start_date={start_date!r}, end_date={end_date!r}"
        )

    # Período no formato aceito pela SIDRA: intervalo AAAAMM-AAAAMM
    period_param = f"{start_period}-{end_period}"
    url = f"{SIDRA_BASE_URL}/t/{table}/n1/1/v/{variable}/p/{period_param}"
    params = {"formato": "json"}

    print(f"[IBGE] Requisitando {key} (tabela {table}) de {start_ym} a {end_ym}.")
    last_error: Exception | None = None
    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            break
        except RequestException as e:
            last_error = e
            if attempt < REQUEST_RETRIES - 1:
                print(
                    f"[IBGE] Erro de conexão (tentativa {attempt + 1}/{REQUEST_RETRIES}), "
                    f"aguardando {RETRY_SLEEP_SECONDS}s..."
                )
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                raise

    if response.status_code != 200:
        msg = f"Erro HTTP {response.status_code} para {key}: {response.text[:300]}"
        print(f"[IBGE] {msg}")
        response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError(
            f"Resposta inesperada da API SIDRA para {key}: esperado lista JSON, obtido {type(data)}"
        )

    # Primeira linha pode ser cabeçalho (dict com chaves como "V", "D3C")
    rows = [r for r in data if isinstance(r, dict) and "V" in r and "D3C" in r]
    if not rows:
        print(f"[IBGE] Nenhum registro retornado para {key} no período.")
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    out = []
    for r in rows:
        period_code = r.get("D3C")
        val = r.get("V")
        dt = _period_to_date(str(period_code) if period_code is not None else "")
        if dt is None:
            continue
        try:
            v_float = float(str(val).replace(",", "."))
        except (TypeError, ValueError):
            continue
        out.append({
            "date": dt,
            "value": v_float,
            "indicator": key,
            "source": SOURCE_LABEL,
        })

    if not out:
        print(f"[IBGE] Nenhum dado válido para {key} no período (apenas cabeçalho da API).")
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    df = pd.DataFrame(out)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    print(f"[IBGE] {key}: {len(df)} registros.")
    return df


def get_ipca_mensal(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série mensal do IPCA (variação mensal %) no Brasil.

    Utiliza a tabela e variável definidas em config.settings.IBGE_IPCA_CONFIG['IPCA'].
    Datas no formato 'YYYY-MM'. Se omitidas, usam-se os defaults do settings.

    Args:
        start_date: Data inicial 'YYYY-MM'. Opcional.
        end_date: Data final 'YYYY-MM'. Opcional.

    Returns:
        DataFrame com colunas date, value, indicator, source.
    """
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH
    return fetch_ibge_ipca_series("IPCA", start, end)


def get_ipca_mensal_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Retorna a série IPCA de forma incremental: lê data/gold/ipca.parquet,
    obtém o último mês e requisita à API apenas a partir do mês seguinte.
    """
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "ipca")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            df = _ensure_date_column(pd.read_parquet(gold / "ipca.parquet"))
            print(f"[IBGE] IPCA já atualizado até {max_ym}; nenhuma requisição.")
            return df
        print(f"[IBGE] Incremental IPCA: de {start_new} a {end} (existente até {max_ym}).")
        new_df = fetch_ibge_ipca_series("IPCA", start_new, end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "ipca.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "ipca.parquet"))
        if "date" not in existing.columns or "date" not in new_df.columns:
            return fetch_ibge_ipca_series("IPCA", DEFAULT_START_MONTH, end)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = (
            combined.drop_duplicates(subset=["date"])
            .sort_values("date")
            .reset_index(drop=True)
        )
        print(f"[IBGE] IPCA: {len(combined)} registros (incl. {len(new_df)} novos).")
        return combined
    print(f"[IBGE] Sem arquivo ipca.parquet em gold; carga completa.")
    return fetch_ibge_ipca_series("IPCA", DEFAULT_START_MONTH, end)


def get_ipca15_mensal_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série IPCA-15 de forma incremental (apenas meses novos)."""
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "ipca15")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            df = _ensure_date_column(pd.read_parquet(gold / "ipca15.parquet"))
            print(f"[IBGE] IPCA-15 já atualizado até {max_ym}; nenhuma requisição.")
            return df
        print(f"[IBGE] Incremental IPCA-15: de {start_new} a {end} (existente até {max_ym}).")
        new_df = fetch_ibge_ipca_series("IPCA15", start_new, end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "ipca15.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "ipca15.parquet"))
        if "date" not in existing.columns or "date" not in new_df.columns:
            return fetch_ibge_ipca_series("IPCA15", DEFAULT_START_MONTH, end)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = (
            combined.drop_duplicates(subset=["date"])
            .sort_values("date")
            .reset_index(drop=True)
        )
        print(f"[IBGE] IPCA-15: {len(combined)} registros (incl. {len(new_df)} novos).")
        return combined
    print(f"[IBGE] Sem arquivo ipca15.parquet em gold; carga completa.")
    return fetch_ibge_ipca_series("IPCA15", DEFAULT_START_MONTH, end)


def get_inpc_mensal(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série mensal do INPC (variação mensal %) no Brasil.

    Utiliza a tabela e variável definidas em config.settings.IBGE_IPCA_CONFIG['INPC'].
    """
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH
    return fetch_ibge_ipca_series("INPC", start, end)


def get_inpc_mensal_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série INPC de forma incremental (apenas meses novos)."""
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "inpc")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            df = _ensure_date_column(pd.read_parquet(gold / "inpc.parquet"))
            print(f"[IBGE] INPC já atualizado até {max_ym}; nenhuma requisição.")
            return df
        print(f"[IBGE] Incremental INPC: de {start_new} a {end} (existente até {max_ym}).")
        new_df = fetch_ibge_ipca_series("INPC", start_new, end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "inpc.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "inpc.parquet"))
        if "date" not in existing.columns or "date" not in new_df.columns:
            return fetch_ibge_ipca_series("INPC", DEFAULT_START_MONTH, end)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = (
            combined.drop_duplicates(subset=["date"])
            .sort_values("date")
            .reset_index(drop=True)
        )
        print(f"[IBGE] INPC: {len(combined)} registros (incl. {len(new_df)} novos).")
        return combined
    print(f"[IBGE] Sem arquivo inpc.parquet em gold; carga completa.")
    return fetch_ibge_ipca_series("INPC", DEFAULT_START_MONTH, end)


def get_ipca15_mensal(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série mensal do IPCA-15 (variação mensal %) no Brasil.

    Utiliza a tabela e variável definidas em config.settings.IBGE_IPCA_CONFIG['IPCA15'].
    Datas no formato 'YYYY-MM'.

    Args:
        start_date: Data inicial 'YYYY-MM'. Opcional.
        end_date: Data final 'YYYY-MM'. Opcional.

    Returns:
        DataFrame com colunas date, value, indicator, source.
    """
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH
    return fetch_ibge_ipca_series("IPCA15", start, end)


def run_ipca_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    layer: str = "gold",
) -> None:
    """
    Executa o ETL do IPCA, IPCA-15 e INPC.

    Busca as séries na API SIDRA, normaliza e salva em Parquet
    (data/gold/ipca.parquet, ipca15.parquet, inpc.parquet) usando a mesma
    função de persistência do módulo BACEN para manter padrão.

    Args:
        start_date: Data inicial 'YYYY-MM'. Opcional.
        end_date: Data final 'YYYY-MM'. Opcional.
        layer: Camada de persistência: 'raw', 'processed' ou 'gold'.
    """
    start = start_date or DEFAULT_START_MONTH
    end = end_date or DEFAULT_END_MONTH

    for name, fetcher in [
        ("ipca", get_ipca_mensal),
        ("ipca15", get_ipca15_mensal),
        ("inpc", get_inpc_mensal),
    ]:
        try:
            df = fetcher(start_date=start, end_date=end)
            if df is not None and not df.empty:
                save_series_to_parquet(df, name, layer=layer)
        except Exception as e:
            print(f"[IBGE] Falha ao processar {name}: {e}")
            raise


if __name__ == "__main__":
    print("Executando ETL IBGE IPCA / IPCA-15 (teste)...")
    run_ipca_etl(layer="gold")
