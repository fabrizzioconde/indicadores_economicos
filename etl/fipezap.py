"""
ETL do Índice FipeZAP (preços de venda e locação por cidade).

Objetivo: disponibilizar séries mensais públicas por cidade/UF para consumo no dashboard
e na API, com schema compatível com o projeto:

- date (datetime): primeiro dia do mês
- city (str)
- uf (str)
- value (float)
- indicator (str)
- source (str)

Fontes (PDFs públicos):
- Locação residencial: downloads.fipe.org.br (FIPE)
- Venda residencial: datazap.com.br (DataZAP)
"""

from __future__ import annotations

import calendar
import io
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
import requests
from requests.exceptions import RequestException

if __name__ == "__main__":
    _project_root = Path(__file__).resolve().parent.parent
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

from config.settings import DEFAULT_END_MONTH, DEFAULT_START_MONTH, GOLD_DIR


SOURCE_LABEL = "FIPEZAP_PDF"
REQUEST_TIMEOUT = 90
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 3
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/pdf,*/*",
}

UF_CODES = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI",
    "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}


@dataclass(frozen=True)
class FipezapRow:
    city: str
    uf: str
    mom_pct: float | None
    price_m2: float | None


def _month_iter(start_ym: str, end_ym: str) -> list[str]:
    """Gera lista de meses YYYY-MM (inclusive) de start até end."""
    start_ym = start_ym.strip()[:7]
    end_ym = end_ym.strip()[:7]
    y1, m1 = int(start_ym[:4]), int(start_ym[5:7])
    y2, m2 = int(end_ym[:4]), int(end_ym[5:7])
    out: list[str] = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        out.append(f"{y}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out


def _ym_to_date(ym: str) -> datetime:
    y = int(ym[:4])
    m = int(ym[5:7])
    return datetime(y, m, 1)


def _ym_to_url_locacao(ym: str) -> list[str]:
    """Retorna lista de URLs candidatas (locação) para um mês YYYY-MM."""
    yyyymm = ym.replace("-", "")
    return [
        f"https://downloads.fipe.org.br/indices/fipezap/fipezap-{yyyymm}-residencial-locacao-publico.pdf",
        f"https://downloads.fipe.org.br/indices/fipezap/fipezap-{yyyymm}-residencial-locacao.pdf",
    ]


def _ym_to_url_venda(ym: str) -> list[str]:
    """Retorna lista de URLs candidatas (venda) para um mês YYYY-MM."""
    yyyymm = ym.replace("-", "")
    # No DataZAP, o PDF do mês de referência (AAAAMM) costuma ser publicado no mês seguinte
    # (e portanto hospedado em /uploads/AAAA/MM_publicacao/). Para robustez, tentamos o mês
    # de referência e os 2 meses subsequentes.
    y = int(ym[:4])
    m = int(ym[5:7])
    candidates: list[tuple[int, int]] = []
    for add in (0, 1, 2):
        yy = y
        mm = m + add
        while mm > 12:
            yy += 1
            mm -= 12
        candidates.append((yy, mm))

    urls: list[str] = []
    for yy, mm in candidates:
        urls.append(f"https://www.datazap.com.br/wp-content/uploads/{yy}/{mm:02d}/fipezap-{yyyymm}-residencial-venda-.pdf")
        urls.append(f"https://www.datazap.com.br/wp-content/uploads/{yy}/{mm:02d}/fipezap-{yyyymm}-residencial-venda.pdf")
    return urls


def _download_pdf(urls: list[str]) -> bytes:
    last_err: Exception | None = None
    for url in urls:
        for attempt in range(REQUEST_RETRIES):
            try:
                r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=DEFAULT_HEADERS)
                if r.status_code == 200 and (r.headers.get("content-type") or "").lower().startswith("application/pdf"):
                    return r.content
                last_err = RuntimeError(f"HTTP {r.status_code} {url}")
            except RequestException as e:
                last_err = e
            if attempt < REQUEST_RETRIES - 1:
                time.sleep(RETRY_SLEEP_SECONDS)
    raise RuntimeError(f"Falha ao baixar PDF FipeZAP. Último erro: {last_err}")


def _parse_venda_table_text(text: str) -> list[FipezapRow]:
    """
    Parser para PDF de VENDA (DataZAP): tabelas vêm em texto 'normal' (sem espaçamento letra-a-letra).
    Espera linhas como: 'São Paulo SP +0,15% ... 11.915'
    """
    rows: list[FipezapRow] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        # Cidade UF ... var_mensal ... preço
        m = re.match(r"^(?P<city>.+?)\s+(?P<uf>[A-Z]{2})\s+(?P<rest>.+)$", ln)
        if not m:
            continue
        uf = m.group("uf").strip()
        if uf not in UF_CODES:
            continue
        city = m.group("city").strip()
        rest = m.group("rest")
        # Primeiro percentual na linha = variação mensal
        pct_m = re.search(r"([+\-−]?\d+,\d+)%", rest)
        mom = None
        if pct_m:
            mom = float(pct_m.group(1).replace("−", "-").replace(",", "."))
        # Último número inteiro na linha = preço médio (R$/m²), com separador de milhar '.'
        price = None
        nums = re.findall(r"\b\d{1,3}(?:\.\d{3})*\b", rest)
        if nums:
            price = float(nums[-1].replace(".", ""))
        rows.append(FipezapRow(city=city, uf=uf, mom_pct=mom, price_m2=price))
    return rows


def _parse_locacao_table_text(text: str) -> list[FipezapRow]:
    """
    Parser para PDF de LOCAÇÃO (FIPE downloads): texto vem com nomes/UF espaçados letra-a-letra.
    Ex.: 'V i t ó r i a E S + 1 ,1 8 % ... 5 1 ,1 0 4 ,2 5 %'
    """
    rows: list[FipezapRow] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        m = re.match(r"^(?P<name>(?:[A-Za-zÀ-ÿ]\s)+)(?P<u1>[A-Z])\s(?P<u2>[A-Z])\s(?P<rest>.*)$", ln)
        if not m:
            continue
        uf = f"{m.group('u1')}{m.group('u2')}"
        if uf not in UF_CODES:
            continue

        name_raw = m.group("name")
        # Compactar letras: 'C a m p o G r a n d e' -> 'CampoGrande'
        city_compact = re.sub(r"\s+", "", name_raw).strip()
        # A linha do índice (Índice FipeZAP) pode terminar com "A P" e ser confundida com UF=AP.
        # Filtrar explicitamente para evitar registro espúrio.
        if city_compact.lower().startswith("índicefipez") or city_compact.lower().startswith("indicefipez"):
            continue

        # Re-hidratar nomes comuns com espaço usando um dicionário (mantém canônico)
        # Lista focada nas cidades monitoradas no FipeZAP locação.
        city_map = {
            "RioDeJaneiro": "Rio de Janeiro",
            "BeloHorizonte": "Belo Horizonte",
            "PortoAlegre": "Porto Alegre",
            "CampoGrande": "Campo Grande",
            "JoaoPessoa": "João Pessoa",
            "SaoLuis": "São Luís",
            "SaoPaulo": "São Paulo",
            "SaoBernardodoCampo": "São Bernardo do Campo",
            "SaoJosedoRioPreto": "São José do Rio Preto",
            "SaoJosedosCampos": "São José dos Campos",
            "PraiaGrande": "Praia Grande",
            "RibeiraoPreto": "Ribeirão Preto",
            "SantoAndre": "Santo André",
            "SaoJose": "São José",
        }
        # Normalizar acentos em algumas palavras compactadas
        city = city_map.get(city_compact, city_compact)

        rest = m.group("rest")
        # Pcts com sinal: 4 colunas (var mensal, var mês anterior, var ano, var 12m)
        pcts = re.findall(r"([+\-−])\s*([\d\s]+,\s*[\d\s]+)\s*%", rest)
        mom = None
        if pcts:
            sign, num = pcts[0]
            v = float(num.replace(" ", "").replace(",", "."))
            mom = -v if sign in ("-", "−") else v

        # Preço médio (R$/m²) aparece como penúltimo número com vírgula antes do yield
        price = None
        m_price = re.search(r"([\d\s]+,\s*[\d\s]+)\s+([\d\s]+,\s*[\d\s]+)\s*%\s*$", rest)
        if m_price:
            price = float(m_price.group(1).replace(" ", "").replace(",", "."))
        rows.append(FipezapRow(city=city, uf=uf, mom_pct=mom, price_m2=price))
    return rows


def _extract_table_texts(pdf_bytes: bytes, *, kind: str) -> list[str]:
    """
    Retorna os textos das tabelas de 'ÚLTIMOS RESULTADOS' (capitais e demais cidades).
    """
    texts: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if "ÚLTIMOS RESULTADOS" not in t:
                continue
            tbl = page.extract_table()
            if not tbl or not tbl[0] or not tbl[0][0]:
                continue
            cell = tbl[0][0]
            # Heurística: apenas tabelas que de fato listam cidades
            if kind == "locacao" and "preços de locação" in cell:
                texts.append(cell)
            if kind == "venda" and "preços de venda" in cell:
                texts.append(cell)
    return texts


def fetch_fipezap_month(ym: str, *, kind: str) -> pd.DataFrame:
    """
    Baixa e parseia o PDF FipeZAP para um mês.

    kind: 'locacao' | 'venda'
    Retorna DataFrame com colunas: city, uf, mom_pct, price_m2
    """
    if kind not in ("locacao", "venda"):
        raise ValueError("kind deve ser 'locacao' ou 'venda'")
    urls = _ym_to_url_locacao(ym) if kind == "locacao" else _ym_to_url_venda(ym)
    pdf_bytes = _download_pdf(urls)
    table_texts = _extract_table_texts(pdf_bytes, kind=kind)
    if not table_texts:
        return pd.DataFrame(columns=["city", "uf", "mom_pct", "price_m2"])

    rows: list[FipezapRow] = []
    for txt in table_texts:
        rows.extend(_parse_locacao_table_text(txt) if kind == "locacao" else _parse_venda_table_text(txt))

    if not rows:
        return pd.DataFrame(columns=["city", "uf", "mom_pct", "price_m2"])

    df = pd.DataFrame([r.__dict__ for r in rows])
    df["city"] = df["city"].astype(str)
    df["uf"] = df["uf"].astype(str)
    df = df.dropna(subset=["city", "uf"]).drop_duplicates(subset=["city", "uf"]).reset_index(drop=True)
    return df


def _max_month_from_parquet(gold_dir: Path, filename: str) -> str | None:
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
    if not ym or len(ym) < 7:
        return ym
    y = int(ym[:4])
    m = int(ym[5:7])
    if m == 12:
        return f"{y + 1}-01"
    return f"{y}-{m + 1:02d}"


def fetch_fipezap_range(
    *,
    kind: str,
    start_month: str,
    end_month: str,
) -> pd.DataFrame:
    """
    Retorna DataFrame longo com colunas:
      date, city, uf, mom_pct, price_m2
    """
    months = _month_iter(start_month, end_month)
    out: list[pd.DataFrame] = []
    for ym in months:
        dfm = fetch_fipezap_month(ym, kind=kind)
        if dfm is None or dfm.empty:
            continue
        dfm = dfm.copy()
        dfm["date"] = _ym_to_date(ym)
        out.append(dfm)
    if not out:
        return pd.DataFrame(columns=["date", "city", "uf", "mom_pct", "price_m2"])
    df = pd.concat(out, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["city", "uf", "date"]).reset_index(drop=True)
    return df


def get_fipezap_locacao_preco_m2(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    start = (start_month or "").strip() or DEFAULT_START_MONTH
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    df = fetch_fipezap_range(kind="locacao", start_month=start, end_month=end)
    if df.empty:
        return pd.DataFrame(columns=["date", "city", "uf", "value", "indicator", "source"])
    out = df.dropna(subset=["price_m2"]).copy()
    out = out.rename(columns={"price_m2": "value"})
    out["indicator"] = "FIPEZAP_LOCACAO_PRECO_M2"
    out["source"] = SOURCE_LABEL
    return out[["date", "city", "uf", "value", "indicator", "source"]]


def get_fipezap_locacao_mom_pct(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    start = (start_month or "").strip() or DEFAULT_START_MONTH
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    df = fetch_fipezap_range(kind="locacao", start_month=start, end_month=end)
    if df.empty:
        return pd.DataFrame(columns=["date", "city", "uf", "value", "indicator", "source"])
    out = df.dropna(subset=["mom_pct"]).copy()
    out = out.rename(columns={"mom_pct": "value"})
    out["indicator"] = "FIPEZAP_LOCACAO_MOM_PCT"
    out["source"] = SOURCE_LABEL
    return out[["date", "city", "uf", "value", "indicator", "source"]]


def get_fipezap_venda_preco_m2(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    start = (start_month or "").strip() or DEFAULT_START_MONTH
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    df = fetch_fipezap_range(kind="venda", start_month=start, end_month=end)
    if df.empty:
        return pd.DataFrame(columns=["date", "city", "uf", "value", "indicator", "source"])
    out = df.dropna(subset=["price_m2"]).copy()
    out = out.rename(columns={"price_m2": "value"})
    out["indicator"] = "FIPEZAP_VENDA_PRECO_M2"
    out["source"] = SOURCE_LABEL
    return out[["date", "city", "uf", "value", "indicator", "source"]]


def get_fipezap_venda_mom_pct(
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    start = (start_month or "").strip() or DEFAULT_START_MONTH
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    df = fetch_fipezap_range(kind="venda", start_month=start, end_month=end)
    if df.empty:
        return pd.DataFrame(columns=["date", "city", "uf", "value", "indicator", "source"])
    out = df.dropna(subset=["mom_pct"]).copy()
    out = out.rename(columns={"mom_pct": "value"})
    out["indicator"] = "FIPEZAP_VENDA_MOM_PCT"
    out["source"] = SOURCE_LABEL
    return out[["date", "city", "uf", "value", "indicator", "source"]]


def _incremental(
    *,
    filename: str,
    kind: str,
    metric: str,
    end_month: str | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    gold = gold_dir or GOLD_DIR
    end = (end_month or "").strip() or DEFAULT_END_MONTH or datetime.now().strftime("%Y-%m")
    max_ym = _max_month_from_parquet(gold, filename)
    if not max_ym:
        # carga completa
        if kind == "locacao" and metric == "price":
            return get_fipezap_locacao_preco_m2(DEFAULT_START_MONTH, end)
        if kind == "locacao" and metric == "mom":
            return get_fipezap_locacao_mom_pct(DEFAULT_START_MONTH, end)
        if kind == "venda" and metric == "price":
            return get_fipezap_venda_preco_m2(DEFAULT_START_MONTH, end)
        return get_fipezap_venda_mom_pct(DEFAULT_START_MONTH, end)

    start_new = _next_month(max_ym)
    if start_new > end:
        return pd.read_parquet(gold / f"{filename}.parquet")

    # Buscar apenas meses novos
    if kind == "locacao":
        new_df = get_fipezap_locacao_preco_m2(start_new, end) if metric == "price" else get_fipezap_locacao_mom_pct(start_new, end)
    else:
        new_df = get_fipezap_venda_preco_m2(start_new, end) if metric == "price" else get_fipezap_venda_mom_pct(start_new, end)

    if new_df is None or new_df.empty:
        return pd.read_parquet(gold / f"{filename}.parquet")

    existing = pd.read_parquet(gold / f"{filename}.parquet")
    if "date" in existing.columns:
        existing["date"] = pd.to_datetime(existing["date"])
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"])
    combined = (
        combined.drop_duplicates(subset=["date", "city", "uf"])
        .sort_values(["city", "uf", "date"])
        .reset_index(drop=True)
    )
    return combined


def get_fipezap_locacao_preco_m2_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental(filename="fipezap_locacao_preco_m2", kind="locacao", metric="price", end_month=end_month, gold_dir=gold_dir)


def get_fipezap_locacao_mom_pct_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental(filename="fipezap_locacao_mom_pct", kind="locacao", metric="mom", end_month=end_month, gold_dir=gold_dir)


def get_fipezap_venda_preco_m2_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental(filename="fipezap_venda_preco_m2", kind="venda", metric="price", end_month=end_month, gold_dir=gold_dir)


def get_fipezap_venda_mom_pct_incremental(end_month: str | None = None, gold_dir: Path | None = None) -> pd.DataFrame:
    return _incremental(filename="fipezap_venda_mom_pct", kind="venda", metric="mom", end_month=end_month, gold_dir=gold_dir)

