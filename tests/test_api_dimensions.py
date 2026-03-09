from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from api.main import get_indicator, list_indicator_dimensions


def test_get_indicator_educacao_defaults_para_br_total() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-01", "2021-01-01"]),
            "value": [100.0, 200.0, 300.0],
            "indicator": ["X", "X", "X"],
            "source": ["SRC", "SRC", "SRC"],
            "uf": ["BR", "SP", "BR"],
            "rede": ["TOTAL", "TOTAL", "TOTAL"],
            "modalidade": ["TOTAL", "TOTAL", "TOTAL"],
            "area": ["TOTAL", "TOTAL", "TOTAL"],
        }
    )
    with patch("api.main.PARQUET_FILES", ["edu_sup_matriculas"]), patch("api.main.load_parquet", return_value=df):
        res = get_indicator("edu_sup_matriculas", start=None, end=None, city=None, uf=None, rede=None, modalidade=None, area=None)
    assert res["key"] == "edu_sup_matriculas"
    # deve retornar apenas BR (2 pontos)
    assert len(res["data"]) == 2
    assert res["data"][0]["date"] == "2020-01-01"


def test_list_indicator_dimensions_retorna_dimensoes() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01"]),
            "value": [1.0],
            "indicator": ["X"],
            "source": ["SRC"],
            "uf": ["BR"],
            "rede": ["TOTAL"],
            "modalidade": ["TOTAL"],
            "area": ["TOTAL"],
        }
    )
    with patch("api.main.PARQUET_FILES", ["edu_sup_matriculas"]), patch("api.main.load_parquet", return_value=df):
        res = list_indicator_dimensions("edu_sup_matriculas")
    assert res["key"] == "edu_sup_matriculas"
    dims = res["dimensions"]
    assert "uf" in dims and dims["uf"] == ["BR"]
    assert "rede" in dims and dims["rede"] == ["TOTAL"]

