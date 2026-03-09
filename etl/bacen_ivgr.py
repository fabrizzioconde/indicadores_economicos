"""
ETL do IVG-R (Índice de Valores de Garantia de Imóveis Residenciais Financiados) via BACEN/SGS.

Normaliza para o schema padrão:
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


def get_ivgr(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    start = (start_date or DEFAULT_START_DATE).strip()[:10]
    end = (end_date or DEFAULT_END_DATE).strip() or datetime.now().strftime("%Y-%m-%d")
    end = end[:10]
    code = BACEN_RE_SERIES.get("ivgr") or "21340"
    return fetch_bcb_series(code, start, end)


def get_ivgr_incremental(
    end_date: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_date or "").strip() or DEFAULT_END_DATE or datetime.now().strftime("%Y-%m-%d")
    code = BACEN_RE_SERIES.get("ivgr") or "21340"
    # série mensal, mas o incremental por "última data + 1 dia" funciona bem
    return _fetch_series_incremental(code, "ivgr", DEFAULT_START_DATE, end, gold)

