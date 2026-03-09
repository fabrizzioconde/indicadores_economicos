"""
ETL do IPCA por grupos (IBGE/SIDRA) — a partir de 2020 (tabela 7060).

Objetivo: extrair variação mensal (%) do IPCA para grupos específicos
e normalizar para o schema padrão do projeto:
    date (datetime), value (float), indicator (str), source (str)

Séries V1 (Brasil):
- Alimentação e bebidas (c315=7170)
- Transportes (c315=7625)

Fonte: API SIDRA (apisidra.ibge.gov.br), tabela 7060, variável 63.
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
SOURCE_LABEL = "IBGE_IPCA"

REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3

IPCA_GRUPOS_TABLE = "7060"
IPCA_MOM_VAR = "63"

# C315 (Geral, grupo, subgrupo, item e subitem)
C315_ALIMENTACAO_BEBIDAS = "7170"
C315_TRANSPORTES = "7625"
C315_VESTUARIO = "7169"  # Vestuário


def _month_to_period(ym: str) -> str:
    """Converte 'YYYY-MM' em 'AAAAMM' (SIDRA)."""
    if not ym or len(ym) < 7:
        return ""
    return ym.strip()[:7].replace("-", "")


def _period_to_date(period_code: str) -> datetime | None:
    """Converte 'AAAAMM' em datetime no dia 01 do mês."""
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


def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


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


def fetch_ipca_grupo_mom(
    *,
    c315: str,
    start_month: str,
    end_month: str,
    indicator: str,
    source: str = SOURCE_LABEL,
) -> pd.DataFrame:
    """
    Busca IPCA variação mensal (%) para um recorte C315 (grupo) na tabela 7060.
    """
    start_ym = (start_month or "").strip() or "2020-01"
    end_ym = (end_month or "").strip() or DEFAULT_END_MONTH
    if not end_ym:
        end_ym = datetime.now().strftime("%Y-%m")

    start_p = _month_to_period(start_ym)
    end_p = _month_to_period(end_ym)
    if not start_p or not end_p:
        raise ValueError(f"Datas devem estar em YYYY-MM. start={start_month!r} end={end_month!r}")

    url = f"{SIDRA_BASE_URL}/t/{IPCA_GRUPOS_TABLE}/n1/1/v/{IPCA_MOM_VAR}/p/{start_p}-{end_p}/c315/{c315}"
    params = {"formato": "json"}
    print(f"[IBGE] Requisitando {indicator} (tabela {IPCA_GRUPOS_TABLE}, c315={c315}) de {start_ym} a {end_ym}.")

    for attempt in range(REQUEST_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            break
        except RequestException as e:
            if attempt < REQUEST_RETRIES - 1:
                print(f"[IBGE] Erro de conexão (tentativa {attempt + 1}/{REQUEST_RETRIES}), aguardando {RETRY_SLEEP_SECONDS}s...")
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                raise

    if response.status_code != 200:
        response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError(f"Resposta SIDRA inesperada: {type(data)}")

    rows = [r for r in data if isinstance(r, dict) and "V" in r and "D3C" in r]
    if not rows:
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    out: list[dict[str, object]] = []
    for r in rows:
        dt = _period_to_date(str(r.get("D3C", "")).strip())
        if dt is None:
            continue
        sval = str(r.get("V", "")).strip()
        if sval in ("..", "...", "X", "-", ""):
            continue
        try:
            v = float(sval.replace(",", "."))
        except (TypeError, ValueError):
            continue
        out.append({"date": dt, "value": v, "indicator": indicator, "source": source})

    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


def get_ipca_alimentacao_mom(start_month: str | None = None, end_month: str | None = None) -> pd.DataFrame:
    return fetch_ipca_grupo_mom(
        c315=C315_ALIMENTACAO_BEBIDAS,
        start_month=start_month or "2020-01",
        end_month=end_month or DEFAULT_END_MONTH,
        indicator="IPCA_ALIMENTACAO_MOM",
    )


def get_ipca_transportes_mom(start_month: str | None = None, end_month: str | None = None) -> pd.DataFrame:
    return fetch_ipca_grupo_mom(
        c315=C315_TRANSPORTES,
        start_month=start_month or "2020-01",
        end_month=end_month or DEFAULT_END_MONTH,
        indicator="IPCA_TRANSPORTES_MOM",
    )


def get_ipca_vestuario_mom(start_month: str | None = None, end_month: str | None = None) -> pd.DataFrame:
    return fetch_ipca_grupo_mom(
        c315=C315_VESTUARIO,
        start_month=start_month or "2020-01",
        end_month=end_month or DEFAULT_END_MONTH,
        indicator="IPCA_VESTUARIO_MOM",
    )


def get_ipca_alimentacao_mom_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "ipca_alimentacao")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            return _ensure_date_column(pd.read_parquet(gold / "ipca_alimentacao.parquet"))
        new_df = get_ipca_alimentacao_mom(start_month=start_new, end_month=end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "ipca_alimentacao.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "ipca_alimentacao.parquet"))
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return combined
    return get_ipca_alimentacao_mom(start_month="2020-01", end_month=end)


def get_ipca_transportes_mom_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "ipca_transportes")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            return _ensure_date_column(pd.read_parquet(gold / "ipca_transportes.parquet"))
        new_df = get_ipca_transportes_mom(start_month=start_new, end_month=end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "ipca_transportes.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "ipca_transportes.parquet"))
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return combined
    return get_ipca_transportes_mom(start_month="2020-01", end_month=end)


def get_ipca_vestuario_mom_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "ipca_vestuario")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            return _ensure_date_column(pd.read_parquet(gold / "ipca_vestuario.parquet"))
        new_df = get_ipca_vestuario_mom(start_month=start_new, end_month=end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "ipca_vestuario.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "ipca_vestuario.parquet"))
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return combined
    return get_ipca_vestuario_mom(start_month="2020-01", end_month=end)

