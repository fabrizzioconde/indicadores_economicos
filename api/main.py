"""
API REST para indicadores macroeconômicos.

Expõe séries e KPIs a partir de data/gold para o frontend React.
Execute com: uvicorn api.main:app --reload (a partir de macro_insights_mvp).
"""
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_loader import (
    PARQUET_FILES,
    filter_by_date_range,
    load_all_data,
    load_parquet,
)

from api.varejo import get_varejo_dashboard

app = FastAPI(
    title="Macro Insights API",
    description="API de indicadores macroeconômicos (SELIC, câmbio, IPCA, etc.)",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/indicators")
def list_indicators() -> dict[str, list[str]]:
    """Lista as chaves de indicadores disponíveis."""
    return {"indicators": list(PARQUET_FILES)}


@app.get("/api/indicators/{key}")
def get_indicator(
    key: str,
    start: date | None = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end: date | None = Query(None, description="Data final (YYYY-MM-DD)"),
    city: str | None = Query(None, description="Cidade (quando aplicável)"),
    uf: str | None = Query(None, description="UF (quando aplicável)"),
    rede: str | None = Query(None, description="Rede (quando aplicável)"),
    modalidade: str | None = Query(None, description="Modalidade (quando aplicável)"),
    area: str | None = Query(None, description="Área (quando aplicável)"),
) -> dict[str, Any]:
    """Retorna a série temporal do indicador. Query params start e end opcionais."""
    if key not in PARQUET_FILES:
        raise HTTPException(status_code=404, detail=f"Indicador desconhecido: {key}")
    df = load_parquet(key)
    if df is None or df.empty:
        return {"key": key, "data": []}
    df = df.sort_values("date")

    # Filtros dimensionais (séries por cidade/UF ou apenas UF)
    if "city" in df.columns:
        if not city or not uf:
            raise HTTPException(
                status_code=400,
                detail="Este indicador requer filtros ?city=...&uf=... (use /api/indicators/{key}/locations para listar opções).",
            )
        df = df[(df["city"].astype(str) == str(city)) & (df["uf"].astype(str).str.upper() == str(uf).upper())]
        if df.empty:
            return {"key": key, "data": []}
    else:
        # Quando há apenas UF como dimensão (ex.: SINAPI), exige ?uf=...
        dim_cols = [c for c in ["uf", "rede", "modalidade", "area"] if c in df.columns]
        if dim_cols == ["uf"] and not uf:
            raise HTTPException(
                status_code=400,
                detail="Este indicador requer filtro ?uf=... (use /api/indicators/{key}/locations para listar opções).",
            )

        # Aplicar filtros (ou defaults TOTAL/BR quando aplicável)
        if "uf" in df.columns:
            if uf:
                df = df[df["uf"].astype(str).str.upper() == str(uf).upper()]
            else:
                # default amigável: Brasil (quando existir)
                uvals = set(df["uf"].dropna().astype(str).str.upper())
                if "BR" in uvals:
                    df = df[df["uf"].astype(str).str.upper() == "BR"]
        if "rede" in df.columns:
            if rede:
                df = df[df["rede"].astype(str) == str(rede)]
            else:
                rvals = set(df["rede"].dropna().astype(str))
                if "TOTAL" in rvals:
                    df = df[df["rede"].astype(str) == "TOTAL"]
        if "modalidade" in df.columns:
            if modalidade:
                df = df[df["modalidade"].astype(str) == str(modalidade)]
            else:
                mvals = set(df["modalidade"].dropna().astype(str))
                if "TOTAL" in mvals:
                    df = df[df["modalidade"].astype(str) == "TOTAL"]
        if "area" in df.columns:
            if area:
                df = df[df["area"].astype(str) == str(area)]
            else:
                avals = set(df["area"].dropna().astype(str))
                if "TOTAL" in avals:
                    df = df[df["area"].astype(str) == "TOTAL"]
        if df.empty:
            return {"key": key, "data": []}

    if start is not None or end is not None:
        start_ts = pd.Timestamp(start) if start else df["date"].min()
        end_ts = pd.Timestamp(end) if end else df["date"].max()
        df = filter_by_date_range(df, start_ts, end_ts)
        if df is None or df.empty:
            return {"key": key, "data": []}
    # Serializar: date como ISO string, value como float
    data = [
        {"date": d.isoformat()[:10], "value": float(v)}
        for d, v in zip(df["date"], df["value"], strict=True)
    ]
    return {"key": key, "data": data}


@app.get("/api/indicators/{key}/latest")
def get_indicator_latest(
    key: str,
    city: str | None = Query(None, description="Cidade (quando aplicável)"),
    uf: str | None = Query(None, description="UF (quando aplicável)"),
    rede: str | None = Query(None, description="Rede (quando aplicável)"),
    modalidade: str | None = Query(None, description="Modalidade (quando aplicável)"),
    area: str | None = Query(None, description="Área (quando aplicável)"),
) -> dict[str, Any]:
    """Retorna o último ponto (date/value) do indicador, com filtros city/uf quando aplicável."""
    if key not in PARQUET_FILES:
        raise HTTPException(status_code=404, detail=f"Indicador desconhecido: {key}")
    df = load_parquet(key)
    if df is None or df.empty:
        return {"key": key, "date": None, "value": None}
    if "date" not in df.columns or "value" not in df.columns:
        return {"key": key, "date": None, "value": None}
    df = df.sort_values("date")

    if "city" in df.columns:
        if not city or not uf:
            raise HTTPException(
                status_code=400,
                detail="Este indicador requer filtros ?city=...&uf=... (use /api/indicators/{key}/locations).",
            )
        df = df[(df["city"].astype(str) == str(city)) & (df["uf"].astype(str).str.upper() == str(uf).upper())]
    else:
        dim_cols = [c for c in ["uf", "rede", "modalidade", "area"] if c in df.columns]
        if dim_cols == ["uf"] and not uf:
            raise HTTPException(
                status_code=400,
                detail="Este indicador requer filtro ?uf=... (use /api/indicators/{key}/locations).",
            )
        if "uf" in df.columns:
            if uf:
                df = df[df["uf"].astype(str).str.upper() == str(uf).upper()]
            else:
                uvals = set(df["uf"].dropna().astype(str).str.upper())
                if "BR" in uvals:
                    df = df[df["uf"].astype(str).str.upper() == "BR"]
        if "rede" in df.columns:
            if rede:
                df = df[df["rede"].astype(str) == str(rede)]
            else:
                rvals = set(df["rede"].dropna().astype(str))
                if "TOTAL" in rvals:
                    df = df[df["rede"].astype(str) == "TOTAL"]
        if "modalidade" in df.columns:
            if modalidade:
                df = df[df["modalidade"].astype(str) == str(modalidade)]
            else:
                mvals = set(df["modalidade"].dropna().astype(str))
                if "TOTAL" in mvals:
                    df = df[df["modalidade"].astype(str) == "TOTAL"]
        if "area" in df.columns:
            if area:
                df = df[df["area"].astype(str) == str(area)]
            else:
                avals = set(df["area"].dropna().astype(str))
                if "TOTAL" in avals:
                    df = df[df["area"].astype(str) == "TOTAL"]

    if df is None or df.empty:
        return {"key": key, "date": None, "value": None}

    last = df.sort_values("date").iloc[-1]
    d = last["date"]
    v = last["value"]
    try:
        d_iso = pd.Timestamp(d).isoformat()[:10]
    except Exception:
        d_iso = str(d)[:10]
    return {"key": key, "date": d_iso, "value": float(v)}


@app.get("/api/indicators/{key}/locations")
def list_indicator_locations(key: str) -> dict[str, Any]:
    """
    Lista dimensões disponíveis para o indicador (cidades/UF ou apenas UF).
    Útil para o frontend montar seletores.
    """
    if key not in PARQUET_FILES:
        raise HTTPException(status_code=404, detail=f"Indicador desconhecido: {key}")
    df = load_parquet(key)
    if df is None or df.empty:
        return {"key": key, "type": "none", "locations": []}

    if "city" in df.columns:
        loc = (
            df[["city", "uf"]]
            .dropna()
            .astype({"city": str, "uf": str})
            .drop_duplicates()
            .sort_values(["uf", "city"])
        )
        return {"key": key, "type": "city_uf", "locations": loc.to_dict(orient="records")}
    if "uf" in df.columns:
        loc = (
            df[["uf"]]
            .dropna()
            .astype({"uf": str})
            .drop_duplicates()
            .sort_values(["uf"])
        )
        return {"key": key, "type": "uf", "locations": loc["uf"].tolist()}
    return {"key": key, "type": "none", "locations": []}


@app.get("/api/indicators/{key}/dimensions")
def list_indicator_dimensions(key: str, max_values: int = 5000) -> dict[str, Any]:
    """
    Lista valores únicos por dimensão (além de city/uf), para montar seletores no frontend.

    Retorna algo como:
      { key, dimensions: { uf: [...], rede: [...], modalidade: [...], area: [...] } }
    """
    if key not in PARQUET_FILES:
        raise HTTPException(status_code=404, detail=f"Indicador desconhecido: {key}")
    df = load_parquet(key)
    if df is None or df.empty:
        return {"key": key, "dimensions": {}}

    base_cols = {"date", "value", "indicator", "source"}
    dims = [c for c in df.columns if c not in base_cols]
    out: dict[str, list[str]] = {}
    for c in dims:
        try:
            vals = df[c].dropna().astype(str).drop_duplicates().tolist()
            vals_sorted = sorted(vals)
            if len(vals_sorted) > max_values:
                vals_sorted = vals_sorted[:max_values]
            out[c] = vals_sorted
        except Exception:
            continue
    return {"key": key, "dimensions": out}


@app.get("/api/kpis")
def get_kpis() -> dict[str, Any]:
    """Retorna KPIs agregados (última SELIC, IPCA acum 12m, variação câmbio, FOCUS, reservas)."""
    data = load_all_data()
    result: dict[str, Any] = {
        "selic": None,
        "ipca_acum_12m": None,
        "cambio_var_pct": None,
        "focus_ipca12": None,
        "focus_selic": None,
        "reservas_bi": None,
        "desocupacao": None,
    }

    df_selic = data.get("selic")
    if df_selic is not None and not df_selic.empty:
        result["selic"] = float(df_selic.sort_values("date")["value"].iloc[-1])

    df_ipca = data.get("ipca")
    if df_ipca is not None and len(df_ipca) >= 12:
        last12 = df_ipca.sort_values("date")["value"].tail(12)
        acum = (1 + last12 / 100).prod() - 1
        result["ipca_acum_12m"] = round(float(acum * 100), 2)

    df_cambio = data.get("usdbrl")
    if df_cambio is not None and len(df_cambio) >= 2:
        df_c = df_cambio.sort_values("date")
        v_ini = df_c["value"].iloc[0]
        v_fim = df_c["value"].iloc[-1]
        if v_ini:
            result["cambio_var_pct"] = round(float((v_fim / v_ini - 1) * 100), 2)

    df_focus = data.get("focus_ipca12")
    if df_focus is not None and not df_focus.empty:
        ultima = df_focus.sort_values("date")["value"].iloc[-1]
        result["focus_ipca12"] = round(float(ultima / 100.0), 2)

    df_focus_selic = data.get("focus_selic")
    if df_focus_selic is not None and not df_focus_selic.empty:
        ultima = df_focus_selic.sort_values("date")["value"].iloc[-1]
        result["focus_selic"] = round(float(ultima / 100.0), 2)

    df_desocupacao = data.get("desocupacao")
    if df_desocupacao is not None and not df_desocupacao.empty:
        ultima = df_desocupacao.sort_values("date")["value"].iloc[-1]
        result["desocupacao"] = round(float(ultima), 2)

    df_reservas = data.get("reservas")
    if df_reservas is not None and not df_reservas.empty:
        ultima = df_reservas.sort_values("date")["value"].iloc[-1]
        result["reservas_bi"] = round(float(ultima / 1e3), 2)

    return result


@app.get("/api/varejo-pme/dashboard")
def varejo_pme_dashboard() -> dict[str, Any]:
    """
    Dashboard agregado para PMEs do varejo.
    Retorna séries, índice de demanda, radar econômico e alertas.
    """
    return get_varejo_dashboard()


@app.get("/health")
def health() -> dict[str, str]:
    """Health check para deploy."""
    return {"status": "ok"}
