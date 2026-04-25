
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field


ENABLE_YEARS = True
ENABLE_ACT_NUMBERS = True
ENABLE_ACT_TYPES = True
ENABLE_SIGLAS = True
ENABLE_NAMED_TERMS = True
ENABLE_INTERVALS = False 

ACT_TYPE_ALIASES = {
    "despacho": {"despacho", "dsp"},
    "resolucao": {"resolucao", "resolução", "res", "reh", "rea"},
    "portaria": {"portaria", "prt"},
    "consulta_publica": {"consulta publica", "consulta pública", "acp"},
    "nota_tecnica": {"nota tecnica", "nota técnica", "nt"},
}

SIGLA_EXPANSIONS = {
    "dsp": ["despacho"],
    "reh": ["resolucao", "homologatoria"],
    "rea": ["resolucao", "autorizativa"],
    "prt": ["portaria"],
    "acp": ["consulta", "publica"],
    "scg": ["superintendencia", "geracao"],
}

LIGHT_STOPWORDS = {
    "a", "o", "os", "as", "de", "do", "da", "dos", "das",
    "e", "em", "no", "na", "nos", "nas", "para", "por",
    "com", "sem", "sobre", "qual", "quais", "que", "um", "uma"
}

GENERIC_LEGAL_TERMS = {
    "art", "artigo", "inciso", "paragrafo", "parágrafo",
    "caput", "alinea", "alínea", "dispõe", "dispoe",
    "considera", "considerando"
}

CONTINUOUS_HINTS = {
    "mw", "kw", "kv", "%", "r$", "reais", "ano", "anos",
    "mes", "meses", "mês", "mêses"
}

TOKEN_PATTERN = re.compile(r"[a-zà-ÿ0-9]+(?:[/-][a-zà-ÿ0-9]+)*")
YEAR_PATTERN = re.compile(r"\b(?:19\d{2}|20\d{2})\b")
ACT_NUMBER_PATTERN = re.compile(r"\b\d{1,6}/(?:19\d{2}|20\d{2})\b")


@dataclass(slots=True)
class QuerySignals:
    years: list[str] = field(default_factory=list)
    act_numbers: list[str] = field(default_factory=list)
    act_types: list[str] = field(default_factory=list)
    siglas: list[str] = field(default_factory=list)
    named_terms: list[str] = field(default_factory=list)
    interval_expansions: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProcessedQuery:
    original: str
    normalized: str
    enriched: str
    base_tokens: list[str]
    enriched_tokens: list[str]
    signals: QuerySignals


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def tokenize_query(text: str) -> list[str]:
    text = (text or "").lower()
    raw_tokens = TOKEN_PATTERN.findall(text)

    tokens: list[str] = []
    for token in raw_tokens:
        tokens.append(token)
        if "/" in token or "-" in token:
            parts = re.split(r"[/-]", token)
            tokens.extend([p for p in parts if p])

    return dedupe_keep_order(tokens)


def normalize_query(text: str) -> str:
    q = (text or "").strip().lower()
    q = q.replace("–", "-").replace("—", "-")
    q = q.replace("“", '"').replace("”", '"')
    q = re.sub(r"[?!;,()\[\]{}\"]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q


def extract_years(query: str) -> list[str]:
    return dedupe_keep_order(YEAR_PATTERN.findall(query))


def extract_act_numbers(query: str) -> list[str]:
    return dedupe_keep_order(ACT_NUMBER_PATTERN.findall(query))


def extract_act_types(query: str) -> list[str]:
    found: list[str] = []
    q_no_acc = strip_accents(query)

    for canonical, variants in ACT_TYPE_ALIASES.items():
        for variant in variants:
            if variant in q_no_acc:
                found.append(canonical)
                break

    return dedupe_keep_order(found)


def extract_siglas(query: str) -> list[str]:
    q_tokens = set(tokenize_query(strip_accents(query)))
    known_siglas = {"dsp", "reh", "rea", "prt", "acp", "scg"}
    found = [sigla for sigla in known_siglas if sigla in q_tokens]
    return dedupe_keep_order(found)


def extract_named_terms(query: str) -> list[str]:
    q_no_acc = strip_accents(query)
    tokens = re.findall(r"[a-z0-9]+", q_no_acc)

    result: list[str] = []
    for token in tokens:
        if len(token) < 4:
            continue
        if token in LIGHT_STOPWORDS:
            continue
        if token in GENERIC_LEGAL_TERMS:
            continue
        result.append(token)

    return dedupe_keep_order(result)


def _looks_continuous_context(anchor: str, full_query: str) -> bool:
    if anchor in CONTINUOUS_HINTS:
        return True
    for hint in CONTINUOUS_HINTS:
        if f"{anchor} {hint}" in full_query:
            return True
    return False


def extract_interval_expansions(query: str, max_range_size: int = 30) -> list[str]:
    """Mantido aqui para futura transição para o Chunking/Ingest pipeline"""
    expansions: list[str] = []
    q_no_acc = strip_accents(query)

    patterns = [
        r"\b([a-z0-9]+)\s+(\d{1,4})\s+a\s+(\d{1,4})\b",
        r"\b([a-z0-9]+)\s+(\d{1,4})\s*-\s*(\d{1,4})\b",
        r"\b([a-z0-9]+)\s+(\d{1,4})\s+ate\s+(\d{1,4})\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, q_no_acc):
            anchor = match.group(1)
            start = int(match.group(2))
            end = int(match.group(3))

            if _looks_continuous_context(anchor, q_no_acc):
                continue

            if start > end:
                start, end = end, start

            range_size = end - start
            if range_size < 0 or range_size > max_range_size:
                continue

            for n in range(start, end + 1):
                expansions.append(f"{anchor} {n}")

    return dedupe_keep_order(expansions)


def build_enriched_query(normalized_query: str, signals: QuerySignals) -> str:
    base_tokens = set(tokenize_query(strip_accents(normalized_query)))
    terms_to_append = []

    for sigla in signals.siglas:
        expansions = SIGLA_EXPANSIONS.get(sigla, [])
        for exp in expansions:
            if exp not in base_tokens:
                terms_to_append.append(exp)

    for act_type in signals.act_types:
        if act_type not in base_tokens:
            terms_to_append.append(act_type)

    terms_to_append = dedupe_keep_order(terms_to_append)

    parts = [normalized_query]
    if terms_to_append:
        parts.append(" ".join(terms_to_append))

    return " ".join(parts).strip()


def extract_query_signals(normalized_query: str) -> QuerySignals:
    signals = QuerySignals()

    if ENABLE_YEARS:
        signals.years = extract_years(normalized_query)

    if ENABLE_ACT_NUMBERS:
        signals.act_numbers = extract_act_numbers(normalized_query)

    if ENABLE_ACT_TYPES:
        signals.act_types = extract_act_types(normalized_query)

    if ENABLE_SIGLAS:
        signals.siglas = extract_siglas(normalized_query)

    if ENABLE_NAMED_TERMS:
        signals.named_terms = extract_named_terms(normalized_query)

    if ENABLE_INTERVALS:
        signals.interval_expansions = extract_interval_expansions(normalized_query)

    return signals


def process_query(query: str) -> ProcessedQuery:
    normalized = normalize_query(query)
    signals = extract_query_signals(normalized)
    
    enriched = build_enriched_query(normalized, signals)

    return ProcessedQuery(
        original=query,
        normalized=normalized,
        enriched=enriched,
        base_tokens=tokenize_query(normalized),
        enriched_tokens=tokenize_query(enriched),
        signals=signals,
    )