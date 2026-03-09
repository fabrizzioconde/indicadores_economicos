"""
ETL de séries do Banco Central (BACEN).

Extrai SELIC, câmbio e IBC-Br via API do SGS (api.bcb.gov.br),
normaliza datas e valores e salva em parquet.
"""
from __future__ import annotations

import json
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
# API Olinda - Expectativas de Mercado (fallback para FOCUS SELIC quando SGS 27573 falhar)
_OLINDA_BASE = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odt/ExpectativasMercadoSelic"
_OLINDA_ALT = "https://olinda.bcb.gov.br/olinda/service/Expectativas/version/v1/odt/ExpectativasMercadoSelic"
SOURCE_LABEL = "BACEN_SGS"
SOURCE_OLINDA = "BACEN_OLINDA"
# Janela máxima permitida pela API para séries diárias (em anos)
MAX_YEARS_DAILY_WINDOW = 10
# Janela usada por requisição (menor = menos dados por chamada, menor risco de timeout)
REQUEST_WINDOW_YEARS = 5
# Timeout por requisição (segundos); a API pode demorar em janelas grandes
REQUEST_TIMEOUT = 90
# Tentativas e espera entre elas em caso de timeout/erro de conexão
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 5
# User-Agent para APIs que bloqueiam requisições sem browser (ex.: Olinda)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


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


def get_focus_ipca12(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série de expectativa FOCUS para IPCA 12 meses (%).

    Utiliza o código configurado em config.settings.BACEN_SERIES['focus_ipca12'].
    """
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE
    code = BACEN_SERIES.get("focus_ipca12") or "27574"
    return fetch_bcb_series(code, start, end)


def get_reservas(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série de reservas internacionais (conceito caixa, total, diária).

    Utiliza o código configurado em config.settings.BACEN_SERIES['reservas'].
    """
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE
    code = BACEN_SERIES.get("reservas") or "13982"
    return fetch_bcb_series(code, start, end)


def _fetch_focus_selic_olinda(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Busca expectativa FOCUS SELIC na API Olinda (Expectativas de Mercado).
    Retorna DataFrame com colunas date, value, series_code, source.
    Valor = mediana da expectativa (% a.a.); data = Data da expectativa (reunião).
    """
    # Olinda OData: sem $filter primeiro (evita 403 em alguns ambientes); depois filtramos por data
    params = {
        "$format": "json",
        "$orderby": "DataReferencia asc",
        "$top": "5000",
    }
    response = None
    for base_url in (_OLINDA_BASE, _OLINDA_ALT):
        try:
            r = requests.get(base_url, params=params, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response = r
            if r.status_code == 200:
                break
        except Exception:
            continue
    if response is None or response.status_code != 200:
        if response is not None and response.text:
            print(f"[BACEN] Olinda FOCUS SELIC: HTTP {response.status_code}")
        return pd.DataFrame(columns=["date", "value", "series_code", "source"])
    try:
        body = response.json()
        raw = body.get("value", body) if isinstance(body, dict) else body
        if not isinstance(raw, list) or not raw:
            return pd.DataFrame(columns=["date", "value", "series_code", "source"])
        rows = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            # Campos possíveis: DataReferencia, Data, DataReuniao, Mediana, media
            data_ref = item.get("DataReferencia") or item.get("Data") or item.get("data")
            valor = item.get("Mediana") or item.get("mediana") or item.get("media") or item.get("Media")
            if data_ref is None or valor is None:
                continue
            try:
                if isinstance(data_ref, str) and "T" in data_ref:
                    dt = datetime.fromisoformat(data_ref.replace("Z", "+00:00")).replace(tzinfo=None)
                else:
                    dt = datetime.strptime(str(data_ref).strip()[:10], "%Y-%m-%d")
            except (ValueError, TypeError):
                continue
            try:
                v = float(str(valor).replace(",", "."))
            except (TypeError, ValueError):
                continue
            # Filtrar pelo intervalo solicitado
            if dt < datetime.strptime(start_date[:10], "%Y-%m-%d") or dt > datetime.strptime(end_date[:10], "%Y-%m-%d"):
                continue
            # Olinda costuma retornar em % (ex.: 10.25); SGS 27573 em pontos base (1025). Normalizar para bp.
            if v < 100:
                v = v * 100.0
            rows.append({"date": dt, "value": v, "series_code": "27573", "source": SOURCE_OLINDA})
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
            print(f"[BACEN] FOCUS SELIC (Olinda): {len(df)} registros.")
        return df if not df.empty else pd.DataFrame(columns=["date", "value", "series_code", "source"])
    except Exception as e:
        print(f"[BACEN] Erro ao buscar FOCUS SELIC na Olinda: {e}")
        return pd.DataFrame(columns=["date", "value", "series_code", "source"])


def _fetch_focus_selic_sgs_ultimos(
    start_date: str,
    end_date: str,
    code: str = "27573",
    n_ultimos: int = 5000,
) -> pd.DataFrame:
    """
    Busca FOCUS SELIC no SGS via endpoint /dados/ultimos/N (às vezes o único que responde para 27573).
    Filtra o resultado pelo intervalo [start_date, end_date].
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/{n_ultimos}"
    try:
        resp = requests.get(url, params={"formato": "json"}, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return pd.DataFrame(columns=["date", "value", "series_code", "source"])
        text = (resp.text or "").strip()
        if not text or (text[0:1] not in ("[", "{")):
            return pd.DataFrame(columns=["date", "value", "series_code", "source"])
        data = json.loads(text)
        if not isinstance(data, list) or not data:
            return pd.DataFrame(columns=["date", "value", "series_code", "source"])
        rows = []
        for item in data:
            date_str = item.get("data", "")
            valor = item.get("valor")
            if not date_str or valor is None:
                continue
            try:
                dt = datetime.strptime(str(date_str).strip()[:10], "%d/%m/%Y")
            except ValueError:
                continue
            try:
                v = float(str(valor).replace(",", "."))
            except (TypeError, ValueError):
                continue
            rows.append({"date": dt, "value": v, "series_code": code, "source": SOURCE_LABEL})
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d")
        end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d")
        df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
        if not df.empty:
            print(f"[BACEN] FOCUS SELIC (SGS ultimos): {len(df)} registros.")
        return df
    except Exception as e:
        print(f"[BACEN] SGS ultimos (FOCUS SELIC) falhou: {e}")
        return pd.DataFrame(columns=["date", "value", "series_code", "source"])


def get_focus_selic(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Retorna a série de expectativa FOCUS para taxa SELIC (% a.a.).
    Ordem de tentativa: 1) SGS por período; 2) SGS por ultimos/N; 3) API Olinda.
    """
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or _today_iso()
    end = end[:10]
    code = BACEN_SERIES.get("focus_selic") or "27573"
    df = pd.DataFrame(columns=["date", "value", "series_code", "source"])
    try:
        df = fetch_bcb_series(code, start, end)
    except Exception as e:
        print(f"[BACEN] SGS {code} (FOCUS SELIC) falhou: {e}; tentando alternativas.")
    if df is None or df.empty:
        df = _fetch_focus_selic_sgs_ultimos(start, end, code=code)
    if df is None or df.empty:
        df = _fetch_focus_selic_olinda(start, end)
    if df is None or df.empty:
        print("[BACEN] FOCUS SELIC: nenhum dado obtido (SGS 27573 e Olinda). Verifique conectividade com api.bcb.gov.br / olinda.bcb.gov.br.")
    return df if df is not None and not df.empty else pd.DataFrame(columns=["date", "value", "series_code", "source"])


def _read_gold_parquet(path: Path) -> pd.DataFrame | None:
    """
    Lê um parquet em data/gold e normaliza a coluna date para datetime.
    Retorna None se o arquivo não existir ou em caso de erro.
    """
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return df
        if "date" in df.columns:
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return None


def _max_date_iso(df: pd.DataFrame) -> str | None:
    """Retorna a data máxima da coluna date no formato YYYY-MM-DD, ou None se vazio."""
    if df is None or df.empty or "date" not in df.columns:
        return None
    max_val = df["date"].max()
    if pd.isna(max_val):
        return None
    if hasattr(max_val, "strftime"):
        return max_val.strftime("%Y-%m-%d")
    return pd.Timestamp(max_val).strftime("%Y-%m-%d")


def _fetch_series_incremental(
    series_code: str,
    gold_filename: str,
    default_start: str,
    end_date: str,
    gold_dir: Path,
) -> pd.DataFrame:
    """
    Busca série do BACEN de forma incremental: lê o parquet existente em gold,
    obtém a última data e requisita à API apenas de (última_data + 1 dia) até end_date.
    Se não houver arquivo existente, faz carga full de default_start até end_date.
    """
    path = gold_dir / f"{gold_filename}.parquet"
    existing = _read_gold_parquet(path)
    end = (end_date or "").strip() or _today_iso()

    if existing is not None and not existing.empty:
        max_d = _max_date_iso(existing)
        if max_d:
            max_dt = datetime.strptime(max_d[:10], "%Y-%m-%d")
            start_new = (max_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            if start_new > end:
                print(f"[BACEN] Série {series_code} já atualizada até {max_d}; nenhuma requisição.")
                return existing
            print(f"[BACEN] Incremental: buscando de {start_new} a {end} (existente até {max_d}).")
            new_df = fetch_bcb_series(series_code, start_new, end)
            if new_df.empty:
                return existing
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = (
                combined.drop_duplicates(subset=["date"])
                .sort_values("date")
                .reset_index(drop=True)
            )
            print(f"[BACEN] Série {series_code}: {len(combined)} registros (incl. {len(new_df)} novos).")
            return combined

    print(f"[BACEN] Sem arquivo em {path}; carga completa.")
    return fetch_bcb_series(series_code, default_start, end)


def get_selic_diaria_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Retorna a série SELIC (incremental): lê data/gold/selic.parquet, busca na API
    apenas os dias após a última data existente e concatena. Evita retrabalho e reduz consumo da API.
    """
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or _today_iso()
    code = BACEN_SERIES.get("selic") or "432"
    return _fetch_series_incremental(code, "selic", DEFAULT_START_DATE, end, gold)


def get_cambio_usdbrl_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série câmbio USD/BRL de forma incremental (apenas dados novos)."""
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or _today_iso()
    code = BACEN_SERIES.get("cambio") or "1"
    return _fetch_series_incremental(code, "usdbrl", DEFAULT_START_DATE, end, gold)


def get_ibcbr_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série IBC-Br de forma incremental (apenas dados novos)."""
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or _today_iso()
    code = BACEN_SERIES.get("ibc_br") or "24364"
    return _fetch_series_incremental(code, "ibcbr", DEFAULT_START_DATE, end, gold)


def get_focus_ipca12_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série FOCUS IPCA 12 meses de forma incremental."""
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or _today_iso()
    code = BACEN_SERIES.get("focus_ipca12") or "27574"
    return _fetch_series_incremental(code, "focus_ipca12", DEFAULT_START_DATE, end, gold)


def get_reservas_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """Retorna a série de reservas internacionais de forma incremental."""
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or _today_iso()
    code = BACEN_SERIES.get("reservas") or "13982"
    return _fetch_series_incremental(code, "reservas", DEFAULT_START_DATE, end, gold)


def get_focus_selic_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Retorna a série FOCUS SELIC de forma incremental.
    Usa get_focus_selic (SGS + fallback Olinda) para garantir dados mesmo quando SGS 27573 falha.
    """
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or _today_iso()
    path = gold / "focus_selic.parquet"
    existing = _read_gold_parquet(path)
    if existing is not None and not existing.empty:
        max_d = _max_date_iso(existing)
        if max_d:
            max_dt = datetime.strptime(max_d[:10], "%Y-%m-%d")
            start_new = (max_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            if start_new > end:
                print(f"[BACEN] FOCUS SELIC já atualizada até {max_d}; nenhuma requisição.")
                return existing
            print(f"[BACEN] FOCUS SELIC incremental: de {start_new} a {end} (existente até {max_d}).")
            new_df = get_focus_selic(start_date=start_new, end_date=end)
            if new_df.empty:
                return existing
            # Unificar coluna source se Olinda retornar series_code
            if "series_code" not in new_df.columns and "source" in new_df.columns:
                new_df = new_df.copy()
                new_df["series_code"] = "27573"
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = (
                combined.drop_duplicates(subset=["date"])
                .sort_values("date")
                .reset_index(drop=True)
            )
            print(f"[BACEN] FOCUS SELIC: {len(combined)} registros (incl. {len(new_df)} novos).")
            return combined
    print(f"[BACEN] Sem arquivo em {path}; carga completa FOCUS SELIC.")
    return get_focus_selic(start_date=DEFAULT_START_DATE, end_date=end)


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
    Executa o ETL das séries do BACEN (SELIC, câmbio, IBC-Br, FOCUS IPCA 12m, reservas).

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
        ("focus_ipca12", get_focus_ipca12),
        ("focus_selic", get_focus_selic),
        ("reservas", get_reservas),
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
