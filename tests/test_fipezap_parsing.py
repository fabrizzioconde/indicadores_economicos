from __future__ import annotations

import pandas as pd

from etl.fipezap import _parse_locacao_table_text, _parse_venda_table_text


def test_parse_locacao_table_text_extrai_campos_basicos() -> None:
    # Linha realista (texto "letra-a-letra") + uma linha do índice que termina com "A P"
    text = "\n".join(
        [
            "Í n d i c e F i p e Z A P + 0 ,5 9 % + 0 ,6 0 % + 8 ,7 0 % + 9 ,7 1 % 5 0 ,6 2 5 ,9 4 %",
            "V i t ó r i a E S + 1 ,1 8 % + 0 ,7 4 % + 1 3 ,2 4 % + 1 2 ,7 6 % 5 1 ,1 0 4 ,2 5 %",
        ]
    )
    rows = _parse_locacao_table_text(text)
    assert len(rows) == 1
    r = rows[0]
    assert r.uf == "ES"
    assert r.city == "Vitória"
    assert r.mom_pct == 1.18
    assert r.price_m2 == 51.10


def test_parse_venda_table_text_extrai_campos_basicos() -> None:
    text = "\n".join(
        [
            "Índice FipeZAP +0,20% +0,28% +0,20% +6,12% 9.642",
            "São Paulo SP +0,15% +0,15% +0,15% +4,30% 11.915",
            "Belo Horizonte MG -0,24% -0,22% -0,24% +10,63% 10.640",
        ]
    )
    rows = _parse_venda_table_text(text)
    assert len(rows) == 2
    sp = [r for r in rows if r.uf == "SP"][0]
    assert sp.city == "São Paulo"
    assert sp.mom_pct == 0.15
    assert sp.price_m2 == 11915.0

