"""
ETL de séries do Banco Central (BACEN).

Extrai SELIC, câmbio e IBC-Br via API do SGS (api.bcb.gov.br),
normaliza datas e valores e salva em parquet.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.exceptions import ReadTimeout, RequestException

# Permite importar config quando o módulo é executado como script (python -m etl.bacen)
if __name__ == "__main__":
    _project_root = Path(__file__).resolve().parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

from config.settings import (
    BACEN_SERIES,
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    GOLD_DIR,
    PROCESSED_DIR,
    RAW_DIR,
)

# URL base da API SGS do Banco Central (datas em DD/MM/YYYY)
BCB_SGS_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
SOURCE_LABEL = "BACEN_SGS"
# Janela máxima permitida pela API para séries diárias (em anos)
MAX_YEARS_DAILY_WINDOW = 10
# Janela usada por requisição (menor = menos dados por chamada, menor risco de timeout)
REQUEST_WINDOW_YEARS = 5
# Timeout por requisição (segundos); a API pode demorar em janelas grandes
REQUEST_TIMEOUT = 90
# Tentativas e espera entre elas em caso de timeout/erro de conexão
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 5


def _today_iso() -> str:
    """Retorna a data de hoje no formato YYYY-MM-DD."""
    return datetime.now().strftime("%Y-%m-%d")


def _format_date_for_api(iso_date: str) -> str:
    """
    Converte data no formato ISO (YYYY-MM-DD) para o formato da API (DD/MM/YYYY).

    Args:
        iso_date: Data em 'YYYY-MM-DD'.

    Returns:
        Data em 'DD/MM/YYYY'.
    """
    if not iso_date or not iso_date.strip():
        return ""
    parsed = datetime.strptime(iso_date.strip()[:10], "%Y-%m-%d")
    return parsed.strftime("%d/%m/%Y")


def fetch_bcb_series(
    series_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Busca uma série temporal no SGS do Banco Central e retorna um DataFrame normalizado.

    Acessa a API do BCB (api.bcb.gov.br), trata erros HTTP e normaliza a resposta
    para colunas padronizadas: date, value, series_code, source.

    Para séries diárias a API aceita no máximo 10 anos por requisição; se o
    intervalo for maior, são feitas várias chamadas em janelas de 10 anos e
    os resultados são concatenados. Se end_date estiver vazio, usa a data de hoje.

    Args:
        series_code: Código numérico da série no SGS (ex: '432' para SELIC).
        start_date: Data inicial no formato 'YYYY-MM-DD'.
        end_date: Data final no formato 'YYYY-MM-DD' (vazio = hoje).

    Returns:
        DataFrame com colunas: date (datetime), value (float), series_code (str), source (str).

    Raises:
        requests.HTTPError: Quando a API retorna status code diferente de 200.
        ValueError: Quando a resposta não é uma lista de registros.
    """
    # Data final vazia = até hoje (exigido pela API para séries diárias)
    end = (end_date or "").strip() or _today_iso()
    start = (start_date or "").strip() or _today_iso()

    start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
    end_dt = datetime.strptime(end[:10], "%Y-%m-%d")
    if start_dt > end_dt:
        start, end = end, start
        start_dt, end_dt = end_dt, start_dt

    # Janelas de no máximo REQUEST_WINDOW_YEARS anos por requisição (evita timeout com payload grande)
    delta_max = timedelta(days=REQUEST_WINDOW_YEARS * 365)
    chunks: list[pd.DataFrame] = []
    chunk_start = start_dt

    while chunk_start <= end_dt:
        chunk_end_dt = min(chunk_start + delta_max, end_dt)
        chunk_start_str = chunk_start.strftime("%Y-%m-%d")
        chunk_end_str = chunk_end_dt.strftime("%Y-%m-%d")

        url = BCB_SGS_BASE_URL.format(code=series_code)
        params: dict[str, str] = {
            "formato": "json",
            "dataInicial": _format_date_for_api(chunk_start_str),
            "dataFinal": _format_date_for_api(chunk_end_str),
        }

        print(f"[BACEN] Requisitando série {series_code} de {chunk_start_str} a {chunk_end_str}.")
        last_error: Exception | None = None
        for attempt in range(REQUEST_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                break
            except (ReadTimeout, RequestException) as e:
                last_error = e
                if attempt < REQUEST_RETRIES - 1:
                    print(f"[BACEN] Timeout/erro de conexão (tentativa {attempt + 1}/{REQUEST_RETRIES}), aguardando {RETRY_SLEEP_SECONDS}s...")
                    time.sleep(RETRY_SLEEP_SECONDS)
                else:
                    raise

        if response.status_code != 200:
            print(f"[BACEN] Erro HTTP {response.status_code} para série {series_code}: {response.text[:200]}")
            response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            raise ValueError(
                f"Resposta inesperada da API para série {series_code}: esperado lista, obtido {type(data)}"
            )

        if not data:
            chunk_start = chunk_end_dt + timedelta(days=1)
            continue

        # API retorna lista de dicts com chaves 'data' (DD/MM/YYYY) e 'valor' (str ou número)
        rows = []
        for item in data:
            date_str = item.get("data", "")
            valor = item.get("valor")
            if date_str is None or valor is None:
                continue
            try:
                dt = datetime.strptime(str(date_str).strip()[:10], "%d/%m/%Y")
            except ValueError:
                continue
            try:
                v = float(str(valor).replace(",", "."))
            except (TypeError, ValueError):
                continue
            rows.append({"date": dt, "value": v, "series_code": series_code, "source": SOURCE_LABEL})

        chunk_df = pd.DataFrame(rows)
        if not chunk_df.empty:
            chunks.append(chunk_df)
        chunk_start = chunk_end_dt + timedelta(days=1)

    if not chunks:
        print(f"[BACEN] Série {series_code} sem registros no período.")
        return pd.DataFrame(columns=["date", "value", "series_code", "source"])

    df = pd.concat(chunks, ignore_index=True)
    df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    print(f"[BACEN] Série {series_code}: {len(df)} registros.")
    return df


def get_selic_diaria(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série da taxa SELIC (meta) no SGS.

    Utiliza o código configurado em config.settings.BACEN_SERIES['selic'].
    Se start_date ou end_date forem omitidos, usa as datas padrão do settings.

    Args:
        start_date: Data inicial 'YYYY-MM-DD'. Opcional.
        end_date: Data final 'YYYY-MM-DD'. Opcional.

    Returns:
        DataFrame com colunas date, value, series_code, source.
    """
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE
    code = BACEN_SERIES.get("selic") or "432"
    return fetch_bcb_series(code, start, end)


def get_cambio_usdbrl(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série de câmbio USD/BRL (dólar venda) no SGS.

    Utiliza o código configurado em config.settings.BACEN_SERIES['cambio'].

    Args:
        start_date: Data inicial 'YYYY-MM-DD'. Opcional.
        end_date: Data final 'YYYY-MM-DD'. Opcional.

    Returns:
        DataFrame com colunas date, value, series_code, source.
    """
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE
    code = BACEN_SERIES.get("cambio") or "1"
    return fetch_bcb_series(code, start, end)


def get_ibcbr(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série do IBC-Br (Índice de Atividade Econômica do BCB) no SGS.

    Utiliza o código configurado em config.settings.BACEN_SERIES['ibc_br'].

    Args:
        start_date: Data inicial 'YYYY-MM-DD'. Opcional.
        end_date: Data final 'YYYY-MM-DD'. Opcional.

    Returns:
        DataFrame com colunas date, value, series_code, source.
    """
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE
    code = BACEN_SERIES.get("ibc_br") or "24364"
    return fetch_bcb_series(code, start, end)


def save_series_to_parquet(
    df: pd.DataFrame,
    filename: str,
    layer: str = "gold",
) -> None:
    """
    Salva um DataFrame de série em arquivo Parquet na camada de dados indicada.

    Cria a pasta data/{layer} se não existir. O nome do arquivo não deve incluir
    a extensão .parquet (é adicionada automaticamente).

    Args:
        df: DataFrame a ser salvo (deve ter coluna date para ordenação).
        filename: Nome do arquivo sem extensão (ex: 'selic_meta').
        layer: Camada de dados: 'raw', 'processed' ou 'gold'. Default 'gold'.
    """
    if layer == "raw":
        folder = RAW_DIR
    elif layer == "processed":
        folder = PROCESSED_DIR
    elif layer == "gold":
        folder = GOLD_DIR
    else:
        raise ValueError(f"layer deve ser 'raw', 'processed' ou 'gold'; recebido: {layer}")

    folder.mkdir(parents=True, exist_ok=True)
    base = filename.removesuffix(".parquet") if filename.endswith(".parquet") else filename
    path = folder / f"{base}.parquet"

    to_save = df.copy()
    if "date" in to_save.columns and pd.api.types.is_datetime64_any_dtype(to_save["date"]):
        to_save = to_save.sort_values("date").reset_index(drop=True)

    to_save.to_parquet(path, index=False)
    print(f"[BACEN] Salvo: {path}")


def run_bacen_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    layer: str = "gold",
) -> None:
    """
    Executa o ETL das séries do BACEN (SELIC, câmbio, IBC-Br).

    Busca cada série na API do Banco Central, normaliza e persiste em Parquet
    na camada indicada (default: gold). Usado pelo pipeline principal.

    Args:
        start_date: Data inicial 'YYYY-MM-DD'. Opcional.
        end_date: Data final 'YYYY-MM-DD'. Opcional.
        layer: Camada de persistência: 'raw', 'processed' ou 'gold'.
    """
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE

    series_fetchers = [
        ("selic_meta", get_selic_diaria),
        ("cambio_usdbrl", get_cambio_usdbrl),
        ("ibcbr", get_ibcbr),
    ]
    for filename, fetcher in series_fetchers:
        try:
            df = fetcher(start_date=start, end_date=end)
            if df is not None and not df.empty:
                save_series_to_parquet(df, filename, layer=layer)
        except Exception as e:
            print(f"[BACEN] Falha ao processar {filename}: {e}")
            raise


if __name__ == "__main__":
    # Teste rápido ao executar: python -m etl.bacen
    print("Executando ETL BACEN (teste)...")
    run_bacen_etl(layer="gold")
