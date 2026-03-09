from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from etl.ibge_ipca_grupos import fetch_ipca_grupo_mom


def test_fetch_ipca_grupo_mom_retorna_colunas_e_datetime() -> None:
    mock_response = [
        {"NC": "1", "V": "0.27", "D3C": "202512", "D3N": "dezembro 2025"},
        {"NC": "1", "V": "0.23", "D3C": "202601", "D3N": "janeiro 2026"},
    ]

    with patch("etl.ibge_ipca_grupos.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        df = fetch_ipca_grupo_mom(
            c315="7170",
            start_month="2025-12",
            end_month="2026-01",
            indicator="IPCA_ALIMENTACAO_MOM",
        )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(df.columns) == {"date", "value", "indicator", "source"}
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert df["indicator"].unique().tolist() == ["IPCA_ALIMENTACAO_MOM"]


def test_fetch_ipca_grupo_mom_descarta_nao_disponivel() -> None:
    mock_response = [
        {"NC": "1", "V": "..", "D3C": "202512", "D3N": "dezembro 2025"},
        {"NC": "1", "V": "0.23", "D3C": "202601", "D3N": "janeiro 2026"},
    ]

    with patch("etl.ibge_ipca_grupos.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        df = fetch_ipca_grupo_mom(
            c315="7625",
            start_month="2025-12",
            end_month="2026-01",
            indicator="IPCA_TRANSPORTES_MOM",
        )

    assert len(df) == 1
    assert df.iloc[0]["date"] == pd.Timestamp("2026-01-01")
    assert float(df.iloc[0]["value"]) == 0.23

