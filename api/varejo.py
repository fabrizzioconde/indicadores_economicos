"""
API para o dashboard Varejo PME.

Expõe endpoint agregado com séries, índice de demanda, radar e alertas.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from data_loader import filter_by_date_range, load_parquet

# Indicadores do dashboard varejo PME
VAREJO_INDICATORS = [
    "salario_real",
    "desocupacao",
    "varejo_restrito",
    "varejo_ampliado",
    "credito_consumo_saldo_pf",
    "credito_consumo_juros_pf",
    "ipca",
    "ipca_alimentacao",
    "ipca_vestuario",
]

# Frequência de cada série (PNAD = trimestral; PMC, BACEN, IPCA = mensal)
SERIES_FREQUENCY: dict[str, str] = {
    "salario_real": "trimestral",
    "desocupacao": "trimestral",
    "varejo_restrito": "mensal",
    "varejo_ampliado": "mensal",
    "credito_consumo_saldo_pf": "mensal",
    "credito_consumo_juros_pf": "mensal",
    "ipca": "mensal",
    "ipca_alimentacao": "mensal",
    "ipca_vestuario": "mensal",
}

# Pesos do índice de demanda (w1, w2, w3)
W_SALARIO = 0.4
W_CREDITO = 0.35
W_DESEMPREGO = 0.25

MONTHS_LOOKBACK = 24


def _to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Garante dados mensais (último dia do mês ou primeiro)."""
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["ym"] = df["date"].dt.to_period("M")
    agg = df.groupby("ym", as_index=False).agg({"value": "last", "date": "last"})
    agg["date"] = agg["ym"].dt.to_timestamp()
    return agg[["date", "value"]].sort_values("date").reset_index(drop=True)


def _base100(series: pd.Series) -> pd.Series:
    """Normaliza série para base 100 no primeiro ponto."""
    if series.empty or series.iloc[0] == 0:
        return series
    return (series / series.iloc[0]) * 100


def _compute_indice_demanda(
    salario: pd.DataFrame | None,
    credito: pd.DataFrame | None,
    desemprego: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    """
    Calcula índice de demanda: w1*norm(salario) + w2*norm(credito) - w3*norm(desemprego).
    Retorna lista de {date, value}.
    """
    dfs: list[pd.DataFrame] = []
    for name, df in [("salario", salario), ("credito", credito), ("desemprego", desemprego)]:
        if df is not None and not df.empty:
            m = _to_monthly(df)
            if not m.empty:
                m = m.rename(columns={"value": name})
                dfs.append(m)

    if len(dfs) < 2:
        return []

    merged = dfs[0]
    for d in dfs[1:]:
        merged = pd.merge(merged, d, on="date", how="outer")
    merged = merged.sort_values("date").reset_index(drop=True).ffill().bfill()

    # Exige pelo menos crédito e um dos outros para calcular
    has_sal = "salario" in merged.columns and merged["salario"].notna().any()
    has_cred = "credito" in merged.columns and merged["credito"].notna().any()
    has_des = "desemprego" in merged.columns and merged["desemprego"].notna().any()
    if not has_cred or (not has_sal and not has_des):
        return []

    merged["norm_sal"] = _base100(merged["salario"].ffill().bfill()) if has_sal else 100.0
    merged["norm_cred"] = _base100(merged["credito"].ffill().bfill())
    merged["norm_des"] = _base100(merged["desemprego"].ffill().bfill()) if has_des else 100.0

    merged["indice"] = (
        W_SALARIO * merged["norm_sal"]
        + W_CREDITO * merged["norm_cred"]
        - W_DESEMPREGO * merged["norm_des"]
    )

    return [
        {
            "date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"),
            "value": round(float(row["indice"]), 2),
        }
        for _, row in merged.iterrows()
    ]


def _compute_radar(
    indice_series: list[dict],
    salario: pd.DataFrame | None,
    credito: pd.DataFrame | None,
    juros: pd.DataFrame | None,
    ipca: pd.DataFrame | None,
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Gera radar econômico com classificação e motivos por dimensão."""
    radar: dict[str, str] = {
        "demanda": "moderada",
        "credito": "estável",
        "inflacao": "estável",
    }
    motivos: dict[str, list[str]] = {
        "demanda": [],
        "credito": [],
        "inflacao": [],
    }

    # Demanda: salário e tendência do índice
    if salario is not None and not salario.empty:
        s = _to_monthly(salario)
        if len(s) >= 3:
            delta_sal = s["value"].iloc[-1] - s["value"].iloc[-3]
            if delta_sal > 0:
                motivos["demanda"].append("salário real em alta recente")
            elif delta_sal < 0:
                motivos["demanda"].append("salário real em queda recente")
            else:
                motivos["demanda"].append("salário real estável")

    if not indice_series or len(indice_series) < 3:
        for eixo in ("demanda", "credito", "inflacao"):
            if not motivos[eixo]:
                motivos[eixo].append("dados insuficientes para detalhar o motivo")
        return radar, motivos

    # Tendência do índice (últimos 3 meses)
    vals = [p["value"] for p in indice_series[-6:]]
    if len(vals) >= 3:
        trend = (vals[-1] - vals[-3]) / 3 if vals[-3] else 0
        if trend > 1:
            radar["demanda"] = "forte"
            motivos["demanda"].append("índice de demanda acelerando nos últimos meses")
        elif trend < -1:
            radar["demanda"] = "fraca"
            motivos["demanda"].append("índice de demanda perdendo força nos últimos meses")
        else:
            motivos["demanda"].append("índice de demanda sem aceleração forte")

    # Crédito: variação do saldo e nível de juros
    if credito is not None and not credito.empty:
        c = _to_monthly(credito)
        if len(c) >= 3:
            var_cred = (c["value"].iloc[-1] / c["value"].iloc[-3] - 1) * 100
            if var_cred > 5:
                radar["credito"] = "expansivo"
                motivos["credito"].append("saldo de crédito em expansão")
            elif var_cred < -2:
                radar["credito"] = "restrito"
                motivos["credito"].append("saldo de crédito em retração")
            else:
                motivos["credito"].append("saldo de crédito com variação moderada")

    if juros is not None and not juros.empty:
        j = _to_monthly(juros)
        if not j.empty and j["value"].iloc[-1] > 4:
            if radar["credito"] == "expansivo":
                radar["credito"] = "estável"
                motivos["credito"].append("juros altos limitam expansão do crédito")
            else:
                radar["credito"] = "restrito"
                motivos["credito"].append("juros ainda elevados restringem o crédito")
        elif not j.empty:
            motivos["credito"].append("juros em patamar mais benigno")

    # Inflação: IPCA acum 12m
    if ipca is not None and len(ipca) >= 12:
        last12 = ipca.sort_values("date")["value"].tail(12)
        acum = (1 + last12 / 100).prod() - 1
        ipca12 = acum * 100
        if ipca12 > 6:
            radar["inflacao"] = "acelerando"
            motivos["inflacao"].append(f"IPCA acumulado em 12 meses em {ipca12:.1f}%")
        elif ipca12 < 4:
            radar["inflacao"] = "desacelerando"
            motivos["inflacao"].append(f"IPCA acumulado em 12 meses em {ipca12:.1f}%")
        else:
            motivos["inflacao"].append(f"IPCA em faixa intermediária ({ipca12:.1f}% em 12 meses)")
    else:
        motivos["inflacao"].append("dados insuficientes de IPCA para leitura de tendência")

    if not motivos["demanda"]:
        motivos["demanda"].append("dados insuficientes para detalhar o motivo")
    if not motivos["credito"]:
        motivos["credito"].append("dados insuficientes para detalhar o motivo")
    if not motivos["inflacao"]:
        motivos["inflacao"].append("dados insuficientes para detalhar o motivo")

    return radar, motivos


def _compute_alertas(
    salario: pd.DataFrame | None,
    credito: pd.DataFrame | None,
    desemprego: pd.DataFrame | None,
    juros: pd.DataFrame | None,
    varejo: pd.DataFrame | None,
) -> list[dict[str, str]]:
    """Gera lista de alertas econômicos estruturados para ação."""
    alertas: list[dict[str, str]] = []

    severity_rank = {"baixa": 0, "media": 1, "alta": 2}

    def _add_alert(
        *,
        alert_id: str,
        titulo: str,
        mensagem: str,
        impacto: str,
        severidade: str,
    ) -> None:
        alertas.append(
            {
                "id": alert_id,
                "titulo": titulo,
                "mensagem": mensagem,
                "impacto": impacto,
                "severidade": severidade,
            }
        )

    # Crédito ao consumidor caiu por 3+ meses consecutivos
    if credito is not None and len(credito) >= 4:
        c = _to_monthly(credito)
        if len(c) >= 4:
            v = c["value"].tolist()
            consec = 0
            for i in range(len(v) - 1, 0, -1):
                if v[i] < v[i - 1]:
                    consec += 1
                else:
                    break
            if consec >= 3:
                _add_alert(
                    alert_id="credito_queda_consecutiva",
                    titulo="Alerta econômico",
                    mensagem=f"Crédito ao consumidor caiu pelo {consec}º mês consecutivo.",
                    impacto="Risco de desaceleração do consumo.",
                    severidade="alta",
                )

    # Salário real caiu 2+ meses consecutivos
    if salario is not None and len(salario) >= 3:
        s = _to_monthly(salario)
        if len(s) >= 3:
            v = s["value"].tolist()
            consec = 0
            for i in range(len(v) - 1, 0, -1):
                if v[i] < v[i - 1]:
                    consec += 1
                else:
                    break
            if consec >= 2:
                _add_alert(
                    alert_id="salario_queda",
                    titulo="Alerta econômico",
                    mensagem="Salário real caiu por dois meses consecutivos.",
                    impacto="Risco de desaceleração do consumo das famílias.",
                    severidade="media",
                )

    # Desemprego subiu 1+ ponto em 3 meses
    if desemprego is not None and len(desemprego) >= 3:
        d = _to_monthly(desemprego)
        if len(d) >= 3:
            delta = d["value"].iloc[-1] - d["value"].iloc[-3]
            if delta >= 1:
                _add_alert(
                    alert_id="desemprego_alta",
                    titulo="Alerta econômico",
                    mensagem=f"Taxa de desemprego subiu {delta:.1f} ponto(s) nos últimos 3 meses.",
                    impacto="Mercado de trabalho mais fraco pode reduzir demanda no varejo.",
                    severidade="alta",
                )

    # Juros do crédito subiram
    if juros is not None and len(juros) >= 3:
        j = _to_monthly(juros)
        if len(j) >= 3:
            delta = j["value"].iloc[-1] - j["value"].iloc[-3]
            if delta >= 0.5:
                _add_alert(
                    alert_id="juros_credito_alta",
                    titulo="Alerta econômico",
                    mensagem=(
                        "Juros do crédito ao consumidor subiram "
                        f"{delta:.1f} ponto(s) percentual(is) nos últimos 3 meses."
                    ),
                    impacto="Condições de financiamento mais caras podem frear compras.",
                    severidade="media",
                )

    # Varejo desacelerou (MoM negativo 2+ meses)
    if varejo is not None and len(varejo) >= 3:
        v = _to_monthly(varejo)
        if len(v) >= 3:
            neg = sum(1 for i in range(len(v) - 2, len(v)) if v["value"].iloc[i] < 0)
            if neg >= 2:
                _add_alert(
                    alert_id="varejo_retracao",
                    titulo="Alerta econômico",
                    mensagem="Vendas do varejo em ritmo negativo nos últimos meses.",
                    impacto="Sinal de demanda enfraquecida para PMEs do setor.",
                    severidade="media",
                )

    alertas = sorted(
        alertas,
        key=lambda item: severity_rank.get(item.get("severidade", "baixa"), 0),
        reverse=True,
    )
    return alertas[:3]


def get_varejo_dashboard() -> dict[str, Any]:
    """
    Retorna dashboard agregado para Varejo PME:
    - series: dict de indicador -> [{date, value}]
    - series_meta: dict de indicador -> {frequency, date_min, date_max}
    - indice_demanda: [{date, value}]
    - radar: {demanda, credito, inflacao}
    - radar_motivos: {demanda: [], credito: [], inflacao: []}
    - alertas: [{id, titulo, mensagem, impacto, severidade}]
    """
    data: dict[str, list[dict[str, Any]]] = {}
    series_meta: dict[str, dict[str, str]] = {}
    now = pd.Timestamp.now()
    start = now - pd.DateOffset(months=MONTHS_LOOKBACK)
    start_ts = start
    end_ts = now

    for key in VAREJO_INDICATORS:
        df = load_parquet(key)
        if df is not None and not df.empty:
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"])
            date_min = df["date"].min().strftime("%Y-%m-%d")
            date_max = df["date"].max().strftime("%Y-%m-%d")
            series_meta[key] = {
                "frequency": SERIES_FREQUENCY.get(key, "mensal"),
                "date_min": date_min,
                "date_max": date_max,
            }
            df = filter_by_date_range(df, start_ts, end_ts)
            if df is not None and not df.empty:
                df = df.sort_values("date")
                data[key] = [
                    {"date": pd.Timestamp(d).strftime("%Y-%m-%d"), "value": float(v)}
                    for d, v in zip(df["date"], df["value"], strict=True)
                ]

    # Índice de demanda
    salario_df = load_parquet("salario_real")
    credito_df = load_parquet("credito_consumo_saldo_pf")
    desemprego_df = load_parquet("desocupacao")
    if salario_df is not None:
        salario_df = filter_by_date_range(salario_df, start_ts, end_ts)
    if credito_df is not None:
        credito_df = filter_by_date_range(credito_df, start_ts, end_ts)
    if desemprego_df is not None:
        desemprego_df = filter_by_date_range(desemprego_df, start_ts, end_ts)

    indice_demanda = _compute_indice_demanda(
        salario_df, credito_df, desemprego_df
    )

    # Radar
    juros_df = load_parquet("credito_consumo_juros_pf")
    ipca_df = load_parquet("ipca")
    if juros_df is not None:
        juros_df = filter_by_date_range(juros_df, start_ts, end_ts)
    if ipca_df is not None:
        ipca_df = filter_by_date_range(ipca_df, start_ts, end_ts)

    radar, radar_motivos = _compute_radar(
        indice_demanda,
        salario_df,
        credito_df,
        juros_df,
        ipca_df,
    )

    # Alertas
    varejo_df = load_parquet("varejo_restrito")
    if varejo_df is not None:
        varejo_df = filter_by_date_range(varejo_df, start_ts, end_ts)
    alertas = _compute_alertas(
        salario_df, credito_df, desemprego_df, juros_df, varejo_df
    )

    return {
        "series": data,
        "series_meta": series_meta,
        "indice_demanda": indice_demanda,
        "radar": radar,
        "radar_motivos": radar_motivos,
        "alertas": alertas,
    }
