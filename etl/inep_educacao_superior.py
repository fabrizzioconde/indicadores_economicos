"""
ETL — Educação Superior (INEP).

Fontes (públicas e gratuitas):
- Microdados do Censo da Educação Superior (INEP) — cursos/IES (CSV em ZIP)
- Índice Geral de Cursos (IGC) — resultados por IES (XLSX)

Este módulo produz séries anuais com `date = YYYY-01-01` e dimensões:
- uf: UF (sigla)
- rede: Publica | Privada
- modalidade: Presencial | EAD | TOTAL
- area: "<codigo> - <nome>" | TOTAL

Observação: docentes e IGC são do nível IES e não têm modalidade/área no dado bruto;
nesses casos, modalidade/area ficam como "TOTAL".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import io
import re
import unicodedata
from pathlib import Path

import pandas as pd
import requests


_UA = {"User-Agent": "Mozilla/5.0 (MacroInsightsBot)"}
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_RAW_INEP_DIR = _PROJECT_ROOT / "data" / "raw" / "inep"


def _as_year_date(y: int) -> pd.Timestamp:
    return pd.Timestamp(date(y, 1, 1))


def _strip_accents(s: str) -> str:
    s2 = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s2 if not unicodedata.combining(ch))


def _norm_col(s: str) -> str:
    return re.sub(r"\s+", " ", _strip_accents(str(s)).strip().lower())


def _download_bytes(url: str, timeout_s: int = 120) -> bytes:
    r = requests.get(url, headers=_UA, timeout=timeout_s)
    r.raise_for_status()
    return r.content


def _download_zip_censo_superior(year: int) -> bytes:
    """
    Baixa o ZIP do Censo Superior e faz cache em disco para evitar downloads repetidos.
    """
    _RAW_INEP_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = _RAW_INEP_DIR / f"microdados_censo_da_educacao_superior_{year}.zip"
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path.read_bytes()
    url = f"https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{year}.zip"
    b = _download_bytes(url, timeout_s=240)
    try:
        cache_path.write_bytes(b)
    except Exception:
        # se falhar o cache, segue sem interromper
        pass
    return b


def _read_member_csv_from_zip(zip_bytes: bytes, member_name_predicate, usecols: list[str] | None = None) -> pd.DataFrame:
    import zipfile

    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    members = zf.namelist()
    target = None
    for m in members:
        if member_name_predicate(m):
            target = m
            break
    if not target:
        raise FileNotFoundError("CSV alvo não encontrado dentro do ZIP.")
    with zf.open(target) as f:
        # INEP tipicamente usa ';' e latin-1
        return pd.read_csv(f, sep=";", encoding="latin-1", low_memory=False, usecols=usecols)


def _map_rede_from_tp_rede(tp: object) -> str:
    try:
        v = int(tp)
    except Exception:
        return "TOTAL"
    if v == 1:
        return "Publica"
    if v == 2:
        return "Privada"
    return "TOTAL"


def _map_rede_from_categoria_admin(tp_cat: object) -> str:
    """
    TP_CATEGORIA_ADMINISTRATIVA (INEP) costuma codificar:
    1 Federal, 2 Estadual, 3 Municipal, demais = privada (com/sem fins lucrativos etc.)
    """
    try:
        v = int(tp_cat)
    except Exception:
        return "TOTAL"
    if v in (1, 2, 3):
        return "Publica"
    return "Privada"


def _map_modalidade(tp_mod: object) -> str:
    try:
        v = int(tp_mod)
    except Exception:
        return "TOTAL"
    if v == 1:
        return "Presencial"
    if v == 2:
        return "EAD"
    return "TOTAL"


def _format_area(co: object, no: object) -> str:
    co_s = "" if pd.isna(co) else str(int(co)) if str(co).strip().isdigit() else str(co).strip()
    no_s = "" if pd.isna(no) else str(no).strip()
    if not co_s and not no_s:
        return "TOTAL"
    if co_s and no_s:
        return f"{co_s} - {no_s}"
    return co_s or no_s


@dataclass(frozen=True)
class CursosMetricSpec:
    indicator: str
    metric_col: str  # ex.: QT_MAT, QT_ING, QT_CONC
    source: str


def _agg_from_cursos_year(year: int, spec: CursosMetricSpec) -> pd.DataFrame:
    z = _download_zip_censo_superior(year)

    def is_cursos_csv(name: str) -> bool:
        up = name.upper()
        return up.endswith(f"MICRODADOS_CADASTRO_CURSOS_{year}.CSV")

    required = [
        "NU_ANO_CENSO",
        "SG_UF",
        "TP_REDE",
        "TP_MODALIDADE_ENSINO",
        "CO_CINE_AREA_GERAL",
        "NO_CINE_AREA_GERAL",
        spec.metric_col,
    ]
    df = _read_member_csv_from_zip(z, is_cursos_csv, usecols=required)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colunas ausentes em cursos {year}: {missing}")

    # Normalização das dimensões
    out = pd.DataFrame(
        {
            "year": pd.to_numeric(df["NU_ANO_CENSO"], errors="coerce").astype("Int64"),
            "uf": df["SG_UF"].astype(str).str.upper().str.strip(),
            "rede": df["TP_REDE"].map(_map_rede_from_tp_rede),
            "modalidade": df["TP_MODALIDADE_ENSINO"].map(_map_modalidade),
            "area": [
                _format_area(co, no)
                for co, no in zip(df["CO_CINE_AREA_GERAL"], df["NO_CINE_AREA_GERAL"], strict=False)
            ],
            "value": pd.to_numeric(df[spec.metric_col], errors="coerce").fillna(0.0),
        }
    )
    out = out.dropna(subset=["year"])

    # Agregações anuais:
    # - granular (UF/rede/modalidade/área)
    # - totais por UF, por UF+rede e Brasil (para defaults/filtros no frontend)
    g_full = out.groupby(["year", "uf", "rede", "modalidade", "area"], dropna=False, as_index=False)["value"].sum()

    g_uf_rede = out.groupby(["year", "uf", "rede"], dropna=False, as_index=False)["value"].sum()
    g_uf_rede["modalidade"] = "TOTAL"
    g_uf_rede["area"] = "TOTAL"

    g_uf = out.groupby(["year", "uf"], dropna=False, as_index=False)["value"].sum()
    g_uf["rede"] = "TOTAL"
    g_uf["modalidade"] = "TOTAL"
    g_uf["area"] = "TOTAL"

    g_br_rede = out.groupby(["year", "rede"], dropna=False, as_index=False)["value"].sum()
    g_br_rede["uf"] = "BR"
    g_br_rede["modalidade"] = "TOTAL"
    g_br_rede["area"] = "TOTAL"

    g_br = out.groupby(["year"], dropna=False, as_index=False)["value"].sum()
    g_br["uf"] = "BR"
    g_br["rede"] = "TOTAL"
    g_br["modalidade"] = "TOTAL"
    g_br["area"] = "TOTAL"

    g = pd.concat([g_full, g_uf_rede, g_uf, g_br_rede, g_br], ignore_index=True)
    g["date"] = g["year"].astype(int).map(_as_year_date)
    g["indicator"] = spec.indicator
    g["source"] = spec.source
    g = g.drop(columns=["year"])
    return g[["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"]]


def _cursos_metric(
    spec: CursosMetricSpec,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for y in range(start_year, end_year + 1):
        try:
            frames.append(_agg_from_cursos_year(y, spec))
        except Exception:
            # Mantém robustez do pipeline (anos podem não existir / mudar layout)
            continue
    if not frames:
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "uf", "rede", "modalidade", "area"]).reset_index(drop=True)


def get_edu_sup_matriculas(start_year: int, end_year: int) -> pd.DataFrame:
    return _cursos_metric(
        CursosMetricSpec(
            indicator="edu_sup_matriculas",
            metric_col="QT_MAT",
            source="INEP — Microdados do Censo da Educação Superior (Cursos)",
        ),
        start_year,
        end_year,
    )


def get_edu_sup_ingressantes(start_year: int, end_year: int) -> pd.DataFrame:
    return _cursos_metric(
        CursosMetricSpec(
            indicator="edu_sup_ingressantes",
            metric_col="QT_ING",
            source="INEP — Microdados do Censo da Educação Superior (Cursos)",
        ),
        start_year,
        end_year,
    )


def get_edu_sup_concluintes(start_year: int, end_year: int) -> pd.DataFrame:
    return _cursos_metric(
        CursosMetricSpec(
            indicator="edu_sup_concluintes",
            metric_col="QT_CONC",
            source="INEP — Microdados do Censo da Educação Superior (Cursos)",
        ),
        start_year,
        end_year,
    )


def get_edu_sup_docentes_exercicio(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Docentes em exercício (QT_DOC_EXE) no nível de IES.
    Modalidade/área não existem nesse módulo -> "TOTAL".
    """
    frames: list[pd.DataFrame] = []
    for y in range(start_year, end_year + 1):
        try:
            z = _download_zip_censo_superior(y)

            def is_ies_csv(name: str) -> bool:
                up = name.upper()
                return up.endswith(f"MICRODADOS_CADASTRO_IES_{y}.CSV") or up.endswith(f"MICRODADOS_ED_SUP_IES_{y}.CSV")

            required = ["NU_ANO_CENSO", "SG_UF_IES", "TP_CATEGORIA_ADMINISTRATIVA", "QT_DOC_EXE"]
            df = _read_member_csv_from_zip(z, is_ies_csv, usecols=required)
            missing = [c for c in required if c not in df.columns]
            if missing:
                continue
            tmp = pd.DataFrame(
                {
                    "year": pd.to_numeric(df["NU_ANO_CENSO"], errors="coerce").astype("Int64"),
                    "uf": df["SG_UF_IES"].astype(str).str.upper().str.strip(),
                    "rede": df["TP_CATEGORIA_ADMINISTRATIVA"].map(_map_rede_from_categoria_admin),
                    "value": pd.to_numeric(df["QT_DOC_EXE"], errors="coerce").fillna(0.0),
                }
            ).dropna(subset=["year"])
            g_full = tmp.groupby(["year", "uf", "rede"], as_index=False)["value"].sum()
            g_uf = tmp.groupby(["year", "uf"], as_index=False)["value"].sum()
            g_uf["rede"] = "TOTAL"
            g_br_rede = tmp.groupby(["year", "rede"], as_index=False)["value"].sum()
            g_br_rede["uf"] = "BR"
            g_br = tmp.groupby(["year"], as_index=False)["value"].sum()
            g_br["uf"] = "BR"
            g_br["rede"] = "TOTAL"

            g = pd.concat([g_full, g_uf, g_br_rede, g_br], ignore_index=True)
            g["date"] = g["year"].astype(int).map(_as_year_date)
            g["indicator"] = "edu_sup_docentes_exercicio"
            g["source"] = "INEP — Microdados do Censo da Educação Superior (IES)"
            g["modalidade"] = "TOTAL"
            g["area"] = "TOTAL"
            frames.append(g.drop(columns=["year"])[["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"]])
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values(["date", "uf", "rede"]).reset_index(drop=True)


def _combine_existing(existing: pd.DataFrame | None, new_df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    if existing is None or existing.empty:
        combined = new_df.copy()
    elif new_df is None or new_df.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, new_df], ignore_index=True)
    if "date" in combined.columns:
        combined["date"] = pd.to_datetime(combined["date"])
    combined = combined.drop_duplicates(subset=subset).sort_values(subset).reset_index(drop=True)
    return combined


def get_edu_sup_matriculas_incremental(end_year: int, existing: pd.DataFrame | None) -> pd.DataFrame:
    start = end_year
    if existing is not None and not existing.empty and "date" in existing.columns:
        try:
            last_year = int(pd.to_datetime(existing["date"]).max().year)
            start = max(last_year + 1, end_year)
        except Exception:
            start = end_year
    if start > end_year:
        return existing if existing is not None else pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    new_df = get_edu_sup_matriculas(start_year=start, end_year=end_year)
    return _combine_existing(existing, new_df, subset=["date", "uf", "rede", "modalidade", "area"])


def get_edu_sup_ingressantes_incremental(end_year: int, existing: pd.DataFrame | None) -> pd.DataFrame:
    start = end_year
    if existing is not None and not existing.empty and "date" in existing.columns:
        try:
            last_year = int(pd.to_datetime(existing["date"]).max().year)
            start = max(last_year + 1, end_year)
        except Exception:
            start = end_year
    if start > end_year:
        return existing if existing is not None else pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    new_df = get_edu_sup_ingressantes(start_year=start, end_year=end_year)
    return _combine_existing(existing, new_df, subset=["date", "uf", "rede", "modalidade", "area"])


def get_edu_sup_concluintes_incremental(end_year: int, existing: pd.DataFrame | None) -> pd.DataFrame:
    start = end_year
    if existing is not None and not existing.empty and "date" in existing.columns:
        try:
            last_year = int(pd.to_datetime(existing["date"]).max().year)
            start = max(last_year + 1, end_year)
        except Exception:
            start = end_year
    if start > end_year:
        return existing if existing is not None else pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    new_df = get_edu_sup_concluintes(start_year=start, end_year=end_year)
    return _combine_existing(existing, new_df, subset=["date", "uf", "rede", "modalidade", "area"])


def get_edu_sup_docentes_exercicio_incremental(end_year: int, existing: pd.DataFrame | None) -> pd.DataFrame:
    start = end_year
    if existing is not None and not existing.empty and "date" in existing.columns:
        try:
            last_year = int(pd.to_datetime(existing["date"]).max().year)
            start = max(last_year + 1, end_year)
        except Exception:
            start = end_year
    if start > end_year:
        return existing if existing is not None else pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    new_df = get_edu_sup_docentes_exercicio(start_year=start, end_year=end_year)
    return _combine_existing(existing, new_df, subset=["date", "uf", "rede", "modalidade", "area"])


def _candidate_igc_urls(year: int) -> list[str]:
    base = f"https://download.inep.gov.br/educacao_superior/indicadores/resultados/{year}/"
    return [
        f"{base}igc_{year}.xlsx",
        f"{base}IGC_{year}.xlsx",
        f"{base}resultado_igc_{year}.xlsx",
        f"{base}resultados_igc_{year}.xlsx",
        f"{base}resultado_IGC_{year}.xlsx",
        f"{base}resultados_IGC_{year}.xlsx",
    ]


def _download_first_ok(urls: list[str]) -> bytes | None:
    for u in urls:
        try:
            r = requests.get(u, headers=_UA, timeout=60)
            if r.status_code == 200 and r.content:
                return r.content
        except Exception:
            continue
    return None

def get_edu_sup_igc_medio(start_year: int, end_year: int) -> pd.DataFrame:
    """
    IGC contínuo médio por UF/rede (quando disponível).
    Série anual (nem todos os anos têm arquivo publicado).
    """
    frames: list[pd.DataFrame] = []
    for y in range(start_year, end_year + 1):
        try:
            b = _download_first_ok(_candidate_igc_urls(y))
            if not b:
                continue
            df = pd.read_excel(io.BytesIO(b))
            df.columns = [str(c).strip() for c in df.columns]

            # Robustez para nomes com variações/acentos
            cols = list(df.columns)
            norm = {_norm_col(c): c for c in cols}

            def pick_one(cands: list[str]) -> str | None:
                for c in cands:
                    if c in norm:
                        return norm[c]
                return None

            col_uf = pick_one(["sigla da uf", "sigla uf", "uf"])
            col_igc = pick_one(["igc (continuo)", "igc (contínuo)", "igc continuo", "igc contínuo"])
            col_cat = pick_one(["categoria administrativa"])

            if not col_uf or not col_igc:
                continue

            tmp = pd.DataFrame(
                {
                    "uf": df[col_uf].astype(str).str.upper().str.strip(),
                    "igc": pd.to_numeric(df[col_igc], errors="coerce"),
                    "rede": df[col_cat].astype(str).map(
                        lambda s: "Publica" if _strip_accents(s).strip().lower().startswith("publica") else "Privada"
                    )
                    if col_cat
                    else "TOTAL",
                }
            ).dropna(subset=["igc"])

            g_full = tmp.groupby(["uf", "rede"], as_index=False)["igc"].mean()
            g_uf = tmp.groupby(["uf"], as_index=False)["igc"].mean()
            g_uf["rede"] = "TOTAL"
            g_br_rede = tmp.groupby(["rede"], as_index=False)["igc"].mean()
            g_br_rede["uf"] = "BR"
            g_br = pd.DataFrame({"igc": [tmp["igc"].mean()]})
            g_br["uf"] = "BR"
            g_br["rede"] = "TOTAL"

            g = pd.concat([g_full, g_uf, g_br_rede, g_br], ignore_index=True)
            g["date"] = _as_year_date(y)
            g["value"] = g["igc"].astype(float)
            g["indicator"] = "edu_sup_igc_medio"
            g["source"] = "INEP — Indicadores de Qualidade (IGC) — resultados por IES (XLSX)"
            g["modalidade"] = "TOTAL"
            g["area"] = "TOTAL"
            frames.append(g[["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"]])
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values(["date", "uf", "rede"]).reset_index(drop=True)


def get_edu_sup_igc_medio_incremental(end_year: int, existing: pd.DataFrame | None) -> pd.DataFrame:
    start = end_year
    if existing is not None and not existing.empty and "date" in existing.columns:
        try:
            last_year = int(pd.to_datetime(existing["date"]).max().year)
            start = max(last_year + 1, end_year)
        except Exception:
            start = end_year
    if start > end_year:
        return existing if existing is not None else pd.DataFrame(columns=["date", "value", "indicator", "source", "uf", "rede", "modalidade", "area"])
    new_df = get_edu_sup_igc_medio(start_year=start, end_year=end_year)
    return _combine_existing(existing, new_df, subset=["date", "uf", "rede"])

