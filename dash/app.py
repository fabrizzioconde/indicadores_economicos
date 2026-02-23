"""
Aplicação Streamlit do macro_insights_mvp.

Dashboard para visualizar indicadores macroeconômicos (SELIC, câmbio,
IBC-Br, IPCA, IPCA-15) a partir dos parquets gerados pelo ETL em data/gold.
Execute com: streamlit run dash/app.py (a partir da raiz do projeto).
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Pasta data/gold (relativa ao diretório do app: dash/ -> parent = raiz do projeto)
_APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _APP_DIR.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

# Indicadores e nomes de arquivo aceitos (primeiro que existir em data/gold será usado)
# Permite compatibilidade com nomes do pipeline atual e com arquivos antigos/alternativos
PARQUET_CANDIDATES: dict[str, list[str]] = {
    "selic": ["selic", "selic_m"],
    "usdbrl": ["usdbrl", "cambio_usdbrl"],
    "ibcbr": ["ibcbr", "ibc_br", "ibcb"],
    "ipca": ["ipca", "ipc"],
    "ipca15": ["ipca15"],
}
PARQUET_FILES = list(PARQUET_CANDIDATES.keys())

# Configuração para exibir gráficos Plotly no Streamlit (evita gráficos em branco)
PLOTLY_LAYOUT = dict(
    template="plotly_white",
    height=420,
    margin=dict(t=50, b=50, l=50, r=30),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(240,240,240,0.8)",
)
PLOTLY_CHART_CONFIG = {"displayModeBar": True, "responsive": True}

st.set_page_config(page_title="Macro Insights MVP", layout="wide")


def _apply_plotly_layout(fig: go.Figure) -> None:
    """Aplica layout padrão para exibição no Streamlit (evita gráfico em branco)."""
    fig.update_layout(**PLOTLY_LAYOUT)


def _read_parquet_file(path: Path) -> pd.DataFrame | None:
    """Lê um parquet e normaliza a coluna date para datetime. Retorna None em erro."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_parquet(indicator_key: str) -> pd.DataFrame | None:
    """
    Carrega os dados de um indicador a partir de data/gold, com cache.

    Tenta cada nome de arquivo em PARQUET_CANDIDATES para esse indicador
    (ex.: para 'selic' tenta selic.parquet e selic_m.parquet). Garante
    que a coluna 'date' seja datetime.

    Args:
        indicator_key: Chave do indicador ('selic', 'usdbrl', 'ibcbr', 'ipca', 'ipca15').

    Returns:
        DataFrame com date e value, ou None se nenhum arquivo for encontrado.
    """
    for filename in PARQUET_CANDIDATES.get(indicator_key, [indicator_key]):
        path = GOLD_DIR / f"{filename}.parquet"
        df = _read_parquet_file(path)
        if df is not None and not df.empty:
            return df
    return None


def load_all_data() -> dict[str, pd.DataFrame | None]:
    """
    Carrega todos os parquets de data/gold (um DataFrame por indicador).

    Usa os nomes alternativos definidos em PARQUET_CANDIDATES para encontrar
    os arquivos existentes.
    """
    return {key: load_parquet(key) for key in PARQUET_FILES}


def filter_by_date_range(
    df: pd.DataFrame | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    """Filtra o DataFrame pelo intervalo [start, end] na coluna date."""
    if df is None or df.empty or "date" not in df.columns:
        return df
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].copy()


def render_sidebar(data: dict[str, pd.DataFrame | None]) -> tuple[pd.Timestamp, pd.Timestamp, bool]:
    """
    Renderiza a barra lateral: seletor de datas e checkbox.

    Returns:
        (data_inicial, data_final, mostrar_valores_reais)
    """
    st.sidebar.header("Filtros")

    # Intervalo de datas: usar min/max dos dados disponíveis
    all_dates: list[pd.Timestamp] = []
    for df in data.values():
        if df is not None and not df.empty and "date" in df.columns:
            all_dates.extend(df["date"].dropna().tolist())
    if not all_dates:
        min_date = pd.Timestamp("2010-01-01")
        max_date = pd.Timestamp.now()
    else:
        min_date = min(all_dates)
        max_date = max(all_dates)

    start = st.sidebar.date_input(
        "Data inicial",
        value=min_date.date() if hasattr(min_date, "date") else min_date,
        min_value=min_date.date() if hasattr(min_date, "date") else min_date,
        max_value=max_date.date() if hasattr(max_date, "date") else max_date,
    )
    end = st.sidebar.date_input(
        "Data final",
        value=max_date.date() if hasattr(max_date, "date") else max_date,
        min_value=min_date.date() if hasattr(min_date, "date") else min_date,
        max_value=max_date.date() if hasattr(max_date, "date") else max_date,
    )
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts > end_ts:
        start_ts, end_ts = end_ts, start_ts

    # Checkbox para exibição nominal vs real (usado em gráficos onde fizer sentido)
    show_real = st.sidebar.checkbox(
        "Mostrar variações percentuais / índices (quando aplicável)",
        value=True,
        help="Ex.: variação % do câmbio em janela móvel, inflação acumulada.",
    )

    return start_ts, end_ts, show_real


def tab_resumo(
    data: dict[str, pd.DataFrame | None],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> None:
    """Aba Resumo: KPIs e texto explicativo."""
    st.subheader("Resumo")
    st.markdown(
        "Visão rápida dos principais indicadores. Os valores abaixo consideram "
        "o período selecionado na barra lateral."
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        df_selic = data.get("selic")
        if df_selic is not None and not df_selic.empty:
            selic_f = filter_by_date_range(df_selic, start, end)
            if selic_f is not None and not selic_f.empty:
                ultima_selic = selic_f.sort_values("date")["value"].iloc[-1]
                st.metric("Última SELIC (% a.a.)", f"{ultima_selic:.2f}%")
            else:
                st.metric("Última SELIC (% a.a.)", "—")
        else:
            st.metric("Última SELIC (% a.a.)", "—")

    with col2:
        df_ipca = data.get("ipca")
        if df_ipca is not None and not df_ipca.empty:
            ipca_f = filter_by_date_range(df_ipca, start, end)
            if ipca_f is not None and len(ipca_f) >= 12:
                # Acumulado 12 meses: (1+v1/100)*...*(1+v12/100) - 1
                last12 = ipca_f.sort_values("date")["value"].tail(12)
                acum = (1 + last12 / 100).prod() - 1
                st.metric("IPCA acum. 12 meses (%)", f"{acum * 100:.2f}%")
            else:
                st.metric("IPCA acum. 12 meses (%)", "—")
        else:
            st.metric("IPCA acum. 12 meses (%)", "—")

    with col3:
        df_cambio = data.get("usdbrl")
        if df_cambio is not None and not df_cambio.empty:
            cam_f = filter_by_date_range(df_cambio, start, end)
            if cam_f is not None and len(cam_f) >= 2:
                cam_f = cam_f.sort_values("date")
                v_ini = cam_f["value"].iloc[0]
                v_fim = cam_f["value"].iloc[-1]
                var_12m = ((v_fim / v_ini) - 1) * 100 if v_ini else 0
                st.metric("Variação USD/BRL no período (%)", f"{var_12m:.2f}%")
            else:
                st.metric("Variação USD/BRL no período (%)", "—")
        else:
            st.metric("Variação USD/BRL no período (%)", "—")

    st.info(
        "Este dashboard usa dados do BACEN (SELIC, câmbio, IBC-Br) e do IBGE (IPCA, IPCA-15) "
        "gerados pelo ETL do projeto. Atualize os dados com: `python run_etl.py --mode full`."
    )


def tab_juros(
    data: dict[str, pd.DataFrame | None],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> None:
    """Aba Juros: gráfico de linha da SELIC (Plotly)."""
    st.subheader("Taxa SELIC")
    df = data.get("selic")
    if df is None or df.empty:
        st.warning("Dados da SELIC não disponíveis. Execute o ETL.")
        return
    df = filter_by_date_range(df, start, end)
    if df is None or df.empty:
        st.warning("Nenhum dado no intervalo selecionado.")
        return
    df = df.sort_values("date")
    fig = px.line(df, x="date", y="value", title="SELIC (% a.a.)", render_mode="svg")
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="SELIC (%)",
        hovermode="x unified",
    )
    _apply_plotly_layout(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CHART_CONFIG)


def tab_inflacao(
    data: dict[str, pd.DataFrame | None],
    start: pd.Timestamp,
    end: pd.Timestamp,
    show_variations: bool,
) -> None:
    """Aba Inflação: IPCA e IPCA-15; opção de inflação acumulada 12 meses."""
    st.subheader("Inflação (IPCA e IPCA-15)")
    df_ipca = data.get("ipca")
    df_ipca15 = data.get("ipca15")
    if (df_ipca is None or df_ipca.empty) and (df_ipca15 is None or df_ipca15.empty):
        st.warning("Dados de inflação não disponíveis. Execute o ETL.")
        return

    fig = go.Figure()
    if df_ipca is not None and not df_ipca.empty:
        d = filter_by_date_range(df_ipca, start, end)
        if d is not None and not d.empty:
            d = d.sort_values("date")
            fig.add_trace(
                go.Scatter(
                    x=d["date"], y=d["value"], name="IPCA (var. mensal %)",
                    mode="lines+markers", line=dict(width=2),
                )
            )
    if df_ipca15 is not None and not df_ipca15.empty:
        d = filter_by_date_range(df_ipca15, start, end)
        if d is not None and not d.empty:
            d = d.sort_values("date")
            fig.add_trace(
                go.Scatter(
                    x=d["date"], y=d["value"], name="IPCA-15 (var. mensal %)",
                    mode="lines+markers", line=dict(width=2),
                )
            )
    fig.update_layout(
        title="IPCA e IPCA-15 — Variação mensal (%)",
        xaxis_title="Data",
        yaxis_title="Variação (%)",
        hovermode="x unified",
    )
    _apply_plotly_layout(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CHART_CONFIG)

    if show_variations and df_ipca is not None and not df_ipca.empty:
        st.subheader("IPCA acumulado em 12 meses")
        df = filter_by_date_range(df_ipca, start, end)
        if df is not None and len(df) >= 12:
            df = df.sort_values("date").reset_index(drop=True)
            df["acum_12m"] = df["value"].rolling(12).apply(
                lambda x: ((1 + x / 100).prod() - 1) * 100 if len(x) == 12 else None
            )
            fig2 = px.line(
                df.dropna(subset=["acum_12m"]), x="date", y="acum_12m", render_mode="svg"
            )
            fig2.update_layout(
                xaxis_title="Data",
                yaxis_title="IPCA acum. 12 meses (%)",
                hovermode="x unified",
            )
            _apply_plotly_layout(fig2)
            st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CHART_CONFIG)


def tab_cambio(
    data: dict[str, pd.DataFrame | None],
    start: pd.Timestamp,
    end: pd.Timestamp,
    show_variations: bool,
) -> None:
    """Aba Câmbio: USD/BRL e opcional variação % em janela móvel (30 dias)."""
    st.subheader("Câmbio USD/BRL")
    df = data.get("usdbrl")
    if df is None or df.empty:
        st.warning("Dados de câmbio não disponíveis. Execute o ETL.")
        return
    df = filter_by_date_range(df, start, end)
    if df is None or df.empty:
        st.warning("Nenhum dado no intervalo selecionado.")
        return
    df = df.sort_values("date").copy()
    fig = px.line(df, x="date", y="value", title="Cotação USD/BRL", render_mode="svg")
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="R$ / US$",
        hovermode="x unified",
    )
    _apply_plotly_layout(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CHART_CONFIG)

    if show_variations and len(df) >= 30:
        df["var_30d"] = df["value"].pct_change(30) * 100
        df_plot = df.dropna(subset=["var_30d"])
        if not df_plot.empty:
            st.subheader("Variação percentual (janela 30 dias)")
            fig2 = px.line(df_plot, x="date", y="var_30d", render_mode="svg")
            fig2.update_layout(
                xaxis_title="Data",
                yaxis_title="Var. % (30 dias)",
                hovermode="x unified",
            )
            _apply_plotly_layout(fig2)
            st.plotly_chart(fig2, use_container_width=True, config=PLOTLY_CHART_CONFIG)


def tab_atividade(
    data: dict[str, pd.DataFrame | None],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> None:
    """Aba Atividade: IBC-Br e métrica simples de crescimento ano contra ano."""
    st.subheader("IBC-Br (Índice de Atividade Econômica)")
    df = data.get("ibcbr")
    if df is None or df.empty:
        st.warning("Dados do IBC-Br não disponíveis. Execute o ETL.")
        return
    df = filter_by_date_range(df, start, end)
    if df is None or df.empty:
        st.warning("Nenhum dado no intervalo selecionado.")
        return
    df = df.sort_values("date").copy()
    fig = px.line(df, x="date", y="value", title="IBC-Br (com ajuste sazonal)", render_mode="svg")
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="IBC-Br",
        hovermode="x unified",
    )
    _apply_plotly_layout(fig)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CHART_CONFIG)

    # Crescimento ano contra ano (último valor vs. mesmo período há 1 ano)
    if len(df) >= 2:
        ultimo = df["value"].iloc[-1]
        df["date_only"] = df["date"].dt.date
        um_ano_atras = df["date"].iloc[-1] - pd.DateOffset(years=1)
        df_ant = df[df["date"] <= um_ano_atras].tail(1)
        if not df_ant.empty:
            valor_ant = df_ant["value"].iloc[0]
            if valor_ant and valor_ant != 0:
                cresc_aa = ((ultimo / valor_ant) - 1) * 100
                st.metric("Crescimento aproximado (ano contra ano, %)", f"{cresc_aa:.2f}%")


def main() -> None:
    """Ponto de entrada: carrega dados, renderiza sidebar e abas."""
    st.title("Macro Insights MVP")

    data = load_all_data()
    missing = [k for k in PARQUET_FILES if data.get(k) is None or data[k].empty]
    if missing:
        nomes = {"selic": "SELIC", "usdbrl": "Câmbio USD/BRL", "ibcbr": "IBC-Br", "ipca": "IPCA", "ipca15": "IPCA-15"}
        faltando = ", ".join(nomes.get(k, k) for k in missing)
        st.warning(
            f"**Dados não encontrados:** {faltando}. "
            "Gere os arquivos em **data/gold** executando na raiz do projeto: "
            "`python run_etl.py --mode full`"
        )

    start_ts, end_ts, show_real = render_sidebar(data)

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Resumo", "Juros", "Inflação", "Câmbio", "Atividade"]
    )

    with tab1:
        tab_resumo(data, start_ts, end_ts)
    with tab2:
        tab_juros(data, start_ts, end_ts)
    with tab3:
        tab_inflacao(data, start_ts, end_ts, show_real)
    with tab4:
        tab_cambio(data, start_ts, end_ts, show_real)
    with tab5:
        tab_atividade(data, start_ts, end_ts)


if __name__ == "__main__":
    # Ao rodar com "streamlit run dash/app.py", o Streamlit executa o script
    # e __name__ é "__main__" — é preciso chamar main() para exibir o dashboard.
    main()
