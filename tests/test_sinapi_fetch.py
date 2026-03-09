from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from etl.ibge_sinapi import fetch_sinapi_uf


def test_fetch_sinapi_uf_retorna_colunas_e_filtra_uf() -> None:
    mock_response = [
        # Cabeçalho
        {"NC": "3", "V": "Valor", "D1C": "UF", "D2C": "Variável", "D3C": "Mês"},
        # SP (35)
        {"NC": "3", "V": "1893.04", "D1C": "35", "D2C": "48", "D3C": "202501"},
        # Código desconhecido (deve ser descartado)
        {"NC": "3", "V": "1000.00", "D1C": "99", "D2C": "48", "D3C": "202501"},
    ]
    with patch("etl.ibge_sinapi.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        df = fetch_sinapi_uf(variable="48", start_month="2025-01", end_month="2025-01")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(df.columns) == {"date", "uf", "value"}
    assert df.iloc[0]["uf"] == "SP"
    assert float(df.iloc[0]["value"]) == 1893.04
    assert pd.api.types.is_datetime64_any_dtype(df["date"])

