# Esse script é uma versão alternativa para o baixar_150pdfs_GUI.py que foca em baixar os pdfs com GUI. O script presente nesse arquivo faz buscas pelo nome do pdf em outras fontes,
# para achar pdfs alternativos, deixando ele aqui apenas como alternativa para quem baixar o repo **não testei esse script, por falta de tempo**

#pip install pandas requests beautifulsoup4 lxml tqdm


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import re
import sys
import time
import random
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# =========================
# Configuração
# =========================

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

SEARCH_ENDPOINTS = [
    "https://html.duckduckgo.com/html/",
]

REQUEST_TIMEOUT = 45
MIN_DELAY = 1.2
MAX_DELAY = 2.4
TOP_K_PER_DOC = 5

TRUST_DOMAIN_WEIGHTS = {
    "gov.br/aneel": 3.0,
    "www.gov.br/aneel": 3.0,
    "aneel.gov.br": 2.8,
    "gov.br/mme": 2.6,
    "www.gov.br/mme": 2.6,
    "planalto.gov.br": 2.4,
    "www.planalto.gov.br": 2.4,
    "leis.org": 2.0,
}

SIGLA_HINTS = {
    "DSP": "despacho",
    "PRT": "portaria",
    "REA": "resolução autorizativa",
    "REH": "resolução homologatória",
    "REN": "resolução normativa",
    "ECT": "extrato de contrato",
    "ACP": "aviso de consulta pública",
    "AVS": "aviso",
    "COM": "comunicado",
    "OFC": "ofício",
}

ALLOWED_EXTENSIONS = (".pdf", ".html", ".htm", ".doc", ".docx")
IGNORE_URL_PATTERNS = [
    "javascript:",
    "mailto:",
    "webcache.googleusercontent",
]

RE_WHITESPACE = re.compile(r"\s+")
RE_NUM_ANO = re.compile(r"(\d{1,5})(?:/(\d{4}))?")


# =========================
# Modelos
# =========================

@dataclass
class Candidate:
    registro_uid: str
    rank: int
    query: str
    candidate_title: str
    candidate_url: str
    candidate_snippet: str
    source_domain: str
    candidate_score: float
    confidence: str


@dataclass
class ResolutionResult:
    registro_uid: str
    ano: str
    titulo: str
    sigla_titulo: str
    tipo_ato_titulo: str
    assunto_normalizado: str
    pdf_tipo: str
    url_original_json: str
    arquivo_original: str
    status_resolucao: str
    fonte_encontrada: str
    url_resolvida: str
    titulo_resolvido: str
    score_match: float
    observacao: str


# =========================
# Utilidades
# =========================

def normalize_spaces(text: str) -> str:
    return RE_WHITESPACE.sub(" ", str(text or "")).strip()


def safe_str(v: Any) -> str:
    if pd.isna(v):
        return ""
    return normalize_spaces(str(v))


def detect_domain(url: str) -> str:
    u = url.lower()
    for d in TRUST_DOMAIN_WEIGHTS:
        if d in u:
            return d
    return "other"


def domain_score(url: str) -> float:
    return TRUST_DOMAIN_WEIGHTS.get(detect_domain(url), 0.3)


def extension_score(url: str) -> float:
    u = url.lower()
    if u.endswith(".pdf"):
        return 1.5
    if u.endswith(".docx") or u.endswith(".doc"):
        return 1.1
    if u.endswith(".html") or u.endswith(".htm"):
        return 0.8
    if "/view" in u:
        return 0.5
    return 0.0


def is_candidate_url_usable(url: str) -> bool:
    if not url:
        return False
    u = url.lower().strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return False
    if any(p in u for p in IGNORE_URL_PATTERNS):
        return False
    return True


def extract_num_ano_from_titulo(titulo: str) -> tuple[Optional[str], Optional[str]]:
    match = RE_NUM_ANO.search(titulo or "")
    if not match:
        return None, None
    return match.group(1), match.group(2)


def build_queries(row: pd.Series) -> List[str]:
    titulo = safe_str(row.get("titulo"))
    sigla = safe_str(row.get("sigla_titulo"))
    tipo = safe_str(row.get("tipo_ato_titulo"))
    ano = safe_str(row.get("ano"))

    numero, ano_titulo = extract_num_ano_from_titulo(titulo)
    hint = SIGLA_HINTS.get(sigla, tipo.lower())

    queries = []

    if titulo:
        queries.append(f'"{titulo}" site:gov.br')
        queries.append(f'"{titulo}" site:aneel.gov.br')
        queries.append(f'"{titulo}" site:planalto.gov.br')
        queries.append(f'"{titulo}" site:leis.org')

    if numero and ano:
        queries.append(f'ANEEL "{hint}" "{numero}" "{ano}" site:gov.br')
        queries.append(f'ANEEL "{hint}" "{numero}" "{ano}" site:aneel.gov.br')
        queries.append(f'ANEEL "{hint}" "{numero}" "{ano}" site:leis.org')

    if tipo and numero and ano:
        queries.append(f'ANEEL "{tipo}" "{numero}" "{ano}"')
        queries.append(f'"{tipo}" "{numero}" "{ano}" site:gov.br')

    if numero and ano_titulo:
        queries.append(f'ANEEL "{numero}/{ano_titulo}"')
        queries.append(f'"{numero}/{ano_titulo}" site:gov.br')

    # remove duplicadas preservando ordem
    seen = set()
    deduped = []
    for q in queries:
        if q not in seen:
            deduped.append(q)
            seen.add(q)

    return deduped[:8]


def overlap_score(row: pd.Series, title: str, snippet: str, url: str) -> float:
    hay = f"{title} {snippet} {url}".lower()

    titulo = safe_str(row.get("titulo")).lower()
    sigla = safe_str(row.get("sigla_titulo")).lower()
    tipo = safe_str(row.get("tipo_ato_titulo")).lower()
    assunto = safe_str(row.get("assunto_normalizado")).lower()
    ano = safe_str(row.get("ano"))

    numero, ano_titulo = extract_num_ano_from_titulo(titulo)

    score = 0.0

    if titulo and titulo.lower() in hay:
        score += 3.0
    if numero and numero in hay:
        score += 1.8
    if ano and ano in hay:
        score += 0.8
    if ano_titulo and ano_titulo in hay:
        score += 0.7
    if sigla and sigla in hay:
        score += 0.3
    if tipo and tipo in hay:
        score += 1.2
    if assunto and assunto in hay:
        score += 0.4

    hint = SIGLA_HINTS.get(safe_str(row.get("sigla_titulo")), "").lower()
    if hint and hint in hay:
        score += 0.8

    return score


def compute_candidate_score(row: pd.Series, title: str, snippet: str, url: str) -> float:
    score = 0.0
    score += domain_score(url)
    score += extension_score(url)
    score += overlap_score(row, title, snippet, url)

    # leve penalização para URLs muito genéricas
    if len(url) < 20:
        score -= 0.4

    return round(score, 4)


def confidence_from_score(score: float) -> str:
    if score >= 6.0:
        return "high"
    if score >= 4.0:
        return "medium"
    if score >= 2.5:
        return "low"
    return "very_low"


# =========================
# Busca HTML
# =========================

def polite_sleep():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def search_duckduckgo_html(session: requests.Session, query: str) -> List[Dict[str, str]]:
    polite_sleep()
    response = session.post(
        SEARCH_ENDPOINTS[0],
        data={"q": query},
        headers={
            "User-Agent": USER_AGENT,
            "Referer": "https://duckduckgo.com/",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    results = []

    for result in soup.select(".result"):
        title_el = result.select_one(".result__title a")
        snippet_el = result.select_one(".result__snippet")

        if not title_el:
            continue

        title = normalize_spaces(title_el.get_text(" ", strip=True))
        url = title_el.get("href", "").strip()
        snippet = normalize_spaces(snippet_el.get_text(" ", strip=True)) if snippet_el else ""

        if is_candidate_url_usable(url):
            results.append({
                "title": title,
                "url": url,
                "snippet": snippet,
            })

    return results


# =========================
# Resolução
# =========================

def resolve_row(session: requests.Session, row: pd.Series, log_f) -> tuple[Optional[ResolutionResult], List[Candidate]]:
    registro_uid = safe_str(row.get("registro_uid"))
    queries = build_queries(row)

    best_by_url: Dict[str, Candidate] = {}

    for query in queries:
        try:
            results = search_duckduckgo_html(session, query)
            log_f.write(json.dumps({
                "registro_uid": registro_uid,
                "event": "query_ok",
                "query": query,
                "num_results": len(results),
            }, ensure_ascii=False) + "\n")

        except Exception as exc:
            log_f.write(json.dumps({
                "registro_uid": registro_uid,
                "event": "query_error",
                "query": query,
                "error": repr(exc),
            }, ensure_ascii=False) + "\n")
            continue

        for r in results:
            title = r["title"]
            url = r["url"]
            snippet = r["snippet"]

            score = compute_candidate_score(row, title, snippet, url)
            confidence = confidence_from_score(score)

            candidate = Candidate(
                registro_uid=registro_uid,
                rank=0,  # preenchido depois
                query=query,
                candidate_title=title,
                candidate_url=url,
                candidate_snippet=snippet,
                source_domain=detect_domain(url),
                candidate_score=score,
                confidence=confidence,
            )

            prev = best_by_url.get(url)
            if prev is None or candidate.candidate_score > prev.candidate_score:
                best_by_url[url] = candidate

    ordered = sorted(
        best_by_url.values(),
        key=lambda c: c.candidate_score,
        reverse=True,
    )

    ordered = ordered[:TOP_K_PER_DOC]

    for idx, cand in enumerate(ordered, start=1):
        cand.rank = idx

    if not ordered:
        return (
            ResolutionResult(
                registro_uid=registro_uid,
                ano=safe_str(row.get("ano")),
                titulo=safe_str(row.get("titulo")),
                sigla_titulo=safe_str(row.get("sigla_titulo")),
                tipo_ato_titulo=safe_str(row.get("tipo_ato_titulo")),
                assunto_normalizado=safe_str(row.get("assunto_normalizado")),
                pdf_tipo=safe_str(row.get("pdf_tipo")),
                url_original_json=safe_str(row.get("url")),
                arquivo_original=safe_str(row.get("arquivo")),
                status_resolucao="not_found",
                fonte_encontrada="",
                url_resolvida="",
                titulo_resolvido="",
                score_match=0.0,
                observacao="Nenhum candidato encontrado",
            ),
            [],
        )

    best = ordered[0]
    status = {
        "high": "high_confidence",
        "medium": "medium_confidence",
        "low": "low_confidence",
        "very_low": "weak_match",
    }[best.confidence]

    result = ResolutionResult(
        registro_uid=registro_uid,
        ano=safe_str(row.get("ano")),
        titulo=safe_str(row.get("titulo")),
        sigla_titulo=safe_str(row.get("sigla_titulo")),
        tipo_ato_titulo=safe_str(row.get("tipo_ato_titulo")),
        assunto_normalizado=safe_str(row.get("assunto_normalizado")),
        pdf_tipo=safe_str(row.get("pdf_tipo")),
        url_original_json=safe_str(row.get("url")),
        arquivo_original=safe_str(row.get("arquivo")),
        status_resolucao=status,
        fonte_encontrada=best.source_domain,
        url_resolvida=best.candidate_url,
        titulo_resolvido=best.candidate_title,
        score_match=best.candidate_score,
        observacao=f"Resolvido via busca alternativa; confiança={best.confidence}",
    )

    return result, ordered


# =========================
# Pipeline principal
# =========================

def main():
    if len(sys.argv) != 3:
        print(
            "Uso: python src/resolver/resolver_fontes_alternativas.py "
            "<csv_amostra> <output_dir>"
        )
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    if not csv_path.exists():
        print(f"CSV não encontrado: {csv_path}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    resolved_path = output_dir / "resolved_links.csv"
    candidates_path = output_dir / "resolved_candidates_top5.csv"
    logs_path = output_dir / "resolver_logs.jsonl"

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        print(f"Erro ao ler CSV: {exc}")
        sys.exit(1)

    required_columns = {
        "registro_uid",
        "ano",
        "titulo",
        "sigla_titulo",
        "tipo_ato_titulo",
        "assunto_normalizado",
        "pdf_tipo",
        "url",
        "arquivo",
    }
    missing = required_columns - set(df.columns)
    if missing:
        print(f"CSV sem colunas obrigatórias: {sorted(missing)}")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    resolved_rows: List[Dict[str, Any]] = []
    candidate_rows: List[Dict[str, Any]] = []

    with logs_path.open("w", encoding="utf-8") as log_f:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Resolvendo fontes alternativas"):
            try:
                resolution, candidates = resolve_row(session, row, log_f)

                if resolution is not None:
                    resolved_rows.append(asdict(resolution))

                for cand in candidates:
                    candidate_rows.append(asdict(cand))

            except Exception as exc:
                # erro inesperado por linha nunca quebra o lote
                log_f.write(json.dumps({
                    "registro_uid": safe_str(row.get("registro_uid")),
                    "event": "row_fatal_error",
                    "error": repr(exc),
                }, ensure_ascii=False) + "\n")

                resolved_rows.append(asdict(ResolutionResult(
                    registro_uid=safe_str(row.get("registro_uid")),
                    ano=safe_str(row.get("ano")),
                    titulo=safe_str(row.get("titulo")),
                    sigla_titulo=safe_str(row.get("sigla_titulo")),
                    tipo_ato_titulo=safe_str(row.get("tipo_ato_titulo")),
                    assunto_normalizado=safe_str(row.get("assunto_normalizado")),
                    pdf_tipo=safe_str(row.get("pdf_tipo")),
                    url_original_json=safe_str(row.get("url")),
                    arquivo_original=safe_str(row.get("arquivo")),
                    status_resolucao="error",
                    fonte_encontrada="",
                    url_resolvida="",
                    titulo_resolvido="",
                    score_match=0.0,
                    observacao=f"Erro inesperado: {repr(exc)}",
                )))

    pd.DataFrame(resolved_rows).to_csv(resolved_path, index=False, encoding="utf-8")
    pd.DataFrame(candidate_rows).to_csv(candidates_path, index=False, encoding="utf-8")

    summary = pd.DataFrame(resolved_rows)["status_resolucao"].value_counts(dropna=False).to_dict()
    print("Concluído.")
    print(f"resolved_links.csv: {resolved_path}")
    print(f"resolved_candidates_top5.csv: {candidates_path}")
    print(f"resolver_logs.jsonl: {logs_path}")
    print("Resumo:", summary)


if __name__ == "__main__":
    main()

