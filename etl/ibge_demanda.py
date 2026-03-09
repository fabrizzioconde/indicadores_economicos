"""
ETL de indicadores de demanda doméstica (IBGE): PMC (varejo) e PMS (serviços).

Extrai séries mensais via API SIDRA (apisidra.ibge.gov.br) e normaliza para o
schema padrão do projeto:
    date (datetime), value (float), indicator (str), source (str)

Séries implementadas (Brasil, volume, com ajuste sazonal quando aplicável):
- PMC varejo restrito: variação M/M-1 com ajuste sazonal (%)
- PMC varejo ampliado: variação M/M-1 com ajuste sazonal (%)
- PMS serviços: variação M/M-1 com ajuste sazonal (%)
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
    settings,
)


SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"
SOURCE_LABEL = "IBGE_PMC_PMS"
REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3


def _month_to_period(ym: str) -> str:
    """Converte 'YYYY-MM' para o código de período SIDRA (AAAAMM)."""
    if not ym or len(ym) < 7:
        return ""
    return ym.strip()[:7].replace("-", "")


def _period_to_date(period_code: str) -> datetime | None:
    """Converte código SIDRA (AAAAMM) em datetime no dia 01 do mês."""
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
    """Garante date em datetime; aceita 'data' como alias."""
    if df is None or df.empty:
        return df
    df = df.copy()
    if "date" not in df.columns and "data" in df.columns:
        df = df.rename(columns={"data": "date"})
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _max_month_from_parquet(gold_dir: Path, filename: str) -> str | None:
    """Retorna o mês (YYYY-MM) da última data no parquet, ou None."""
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
    """Retorna o mês seguinte a YYYY-MM."""
    if not ym or len(ym) < 7:
        return ym
    year, month = int(ym[:4]), int(ym[5:7])
    if month == 12:
        return f"{year + 1}-01"
    return f"{year}-{month + 1:02d}"


def fetch_sidra_mensal(
    *,
    table: str,
    variable: str,
    classifications: dict[str, str] | None = None,
    start_month: str,
    end_month: str,
    indicator: str,
    source: str = SOURCE_LABEL,
) -> pd.DataFrame:
    """
    Busca uma série mensal na API SIDRA.

    A resposta é normalizada para colunas: date, value, indicator, source.
    """
    start_ym = (start_month or "").strip() or DEFAULT_START_MONTH
    end_ym = (end_month or "").strip() or DEFAULT_END_MONTH
    if not end_ym:
        end_ym = datetime.now().strftime("%Y-%m")

    start_p = _month_to_period(start_ym)
    end_p = _month_to_period(end_ym)
    if not start_p or not end_p:
        raise ValueError(f"Datas devem estar em YYYY-MM. start={start_month!r} end={end_month!r}")

    class_path = ""
    if classifications:
        # A API SIDRA é sensível à ausência de categoria "Total" em algumas classificações
        # (ex.: C11046 em PMC/PMS). Sem informar a classificação, a API retorna "..".
        parts = []
        for c, cat in classifications.items():
            c_norm = str(c).strip().lower()
            if not c_norm.startswith("c"):
                c_norm = f"c{c_norm}"
            parts.append((c_norm, str(cat).strip()))
        for c_norm, cat in sorted(parts):
            class_path += f"/{c_norm}/{cat}"

    url = f"{SIDRA_BASE_URL}/t/{table}/n1/1/v/{variable}/p/{start_p}-{end_p}{class_path}"
    params = {"formato": "json"}

    print(f"[IBGE] Requisitando {indicator} (tabela {table}, variável {variable}) de {start_ym} a {end_ym}.")
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

    rows = [r for r in data if isinstance(r, dict) and "V" in r and str(r.get("D3C", "")).isdigit()]
    if not rows:
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])

    out: list[dict[str, object]] = []
    for r in rows:
        dt = _period_to_date(str(r.get("D3C", "")).strip())
        if dt is None:
            continue
        val = r.get("V")
        if val is None:
            continue
        sval = str(val).strip()
        # Símbolos SIDRA: ".." (não disponível), "-" (zero absoluto), etc.
        if sval in ("..", "...", "X", "-", ""):
            continue
        try:
            v_float = float(sval.replace(",", "."))
        except (TypeError, ValueError):
            continue
        out.append({"date": dt, "value": v_float, "indicator": indicator, "source": source})

    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame(columns=["date", "value", "indicator", "source"])
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df


def _get_cfg(key: str) -> tuple[str, str, dict[str, str] | None]:
    cfg = getattr(settings, "ibge_demanda_config", {}).get(key)
    if not cfg:
        raise ValueError(f"Config IBGE demanda ausente para {key}.")
    table = str(cfg["table"])
    variable = str(cfg["variable"])
    classifications = cfg.get("classifications") if isinstance(cfg, dict) else None
    if classifications is not None and not isinstance(classifications, dict):
        classifications = None
    classifications = {str(k): str(v) for k, v in (classifications or {}).items()} or None
    return (table, variable, classifications)


def get_varejo_restrito_mom_sa(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    table, var, cls = _get_cfg("PMC_VAREJO_RESTRITO_MOM_SA")
    return fetch_sidra_mensal(
        table=table,
        variable=var,
        classifications=cls,
        start_month=start_month or DEFAULT_START_MONTH,
        end_month=end_month or DEFAULT_END_MONTH,
        indicator="VAREJO_RESTRITO_MOM_SA",
    )


def get_varejo_ampliado_mom_sa(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    table, var, cls = _get_cfg("PMC_VAREJO_AMPLIADO_MOM_SA")
    return fetch_sidra_mensal(
        table=table,
        variable=var,
        classifications=cls,
        start_month=start_month or DEFAULT_START_MONTH,
        end_month=end_month or DEFAULT_END_MONTH,
        indicator="VAREJO_AMPLIADO_MOM_SA",
    )


def get_servicos_mom_sa(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    table, var, cls = _get_cfg("PMS_SERVICOS_MOM_SA")
    return fetch_sidra_mensal(
        table=table,
        variable=var,
        classifications=cls,
        start_month=start_month or DEFAULT_START_MONTH,
        end_month=end_month or DEFAULT_END_MONTH,
        indicator="SERVICOS_MOM_SA",
    )


def get_varejo_restrito_mom_sa_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "varejo_restrito")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            return _ensure_date_column(pd.read_parquet(gold / "varejo_restrito.parquet"))
        new_df = get_varejo_restrito_mom_sa(start_month=start_new, end_month=end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "varejo_restrito.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "varejo_restrito.parquet"))
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return combined
    return get_varejo_restrito_mom_sa(start_month=DEFAULT_START_MONTH, end_month=end)


def get_varejo_ampliado_mom_sa_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "varejo_ampliado")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            return _ensure_date_column(pd.read_parquet(gold / "varejo_ampliado.parquet"))
        new_df = get_varejo_ampliado_mom_sa(start_month=start_new, end_month=end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "varejo_ampliado.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "varejo_ampliado.parquet"))
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return combined
    return get_varejo_ampliado_mom_sa(start_month=DEFAULT_START_MONTH, end_month=end)


def get_servicos_mom_sa_incremental(
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, "servicos")
    if max_ym:
        start_new = _next_month(max_ym)
        if start_new > end:
            return _ensure_date_column(pd.read_parquet(gold / "servicos.parquet"))
        new_df = get_servicos_mom_sa(start_month=start_new, end_month=end)
        if new_df.empty:
            return _ensure_date_column(pd.read_parquet(gold / "servicos.parquet"))
        existing = _ensure_date_column(pd.read_parquet(gold / "servicos.parquet"))
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return combined
    return get_servicos_mom_sa(start_month=DEFAULT_START_MONTH, end_month=end)

