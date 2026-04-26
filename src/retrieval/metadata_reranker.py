from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from src.common.schemas import PreparedChunk


# ============================================================
# Configuração
# ============================================================

@dataclass(frozen=True)
class MetadataRerankConfig:
    # boosts estruturais
    year_match_boost: float = 0.08
    act_number_match_boost: float = 0.12
    act_type_match_boost: float = 0.08
    sigla_match_boost: float = 0.06

    # boosts textuais
    assunto_overlap_weight: float = 0.10
    ementa_overlap_weight: float = 0.12
    pdf_tipo_overlap_weight: float = 0.04
    autor_overlap_weight: float = 0.03

    # controle
    max_total_boost_ratio: float = 0.35
    min_token_len_for_overlap: int = 4
    top_n_rerank: int = 20



@dataclass(slots=True)
class QueryMetadataSignals:
    years: list[str] = field(default_factory=list)
    act_numbers: list[str] = field(default_factory=list)
    act_types: list[str] = field(default_factory=list)
    siglas: list[str] = field(default_factory=list)
    lexical_terms: list[str] = field(default_factory=list)



TOKEN_PATTERN = re.compile(r"[a-zà-ÿ0-9]+(?:[/-][a-zà-ÿ0-9]+)*")
YEAR_PATTERN = re.compile(r"\b(?:19\d{2}|20\d{2})\b")
ACT_NUMBER_PATTERN = re.compile(r"\b\d{1,6}/(?:19\d{2}|20\d{2})\b")

ACT_TYPE_ALIASES = {
    "despacho": {"despacho", "dsp"},
    "resolucao": {"resolucao", "resolução", "res", "reh", "rea"},
    "portaria": {"portaria", "prt"},
    "consulta_publica": {"consulta publica", "consulta pública", "acp"},
    "nota_tecnica": {"nota tecnica", "nota técnica", "nt"},
}

SIGLA_SET = {"dsp", "reh", "prt", "acp", "aneel", "scg"}

LIGHT_STOPWORDS = {
    "a", "o", "os", "as", "de", "do", "da", "dos", "das",
    "e", "em", "no", "na", "nos", "nas", "para", "por",
    "com", "sem", "sobre", "qual", "quais", "que", "um", "uma",
    "ao", "aos", "à", "às"
}


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[?!;,()\[\]{}\"]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize_text(text: str) -> list[str]:
    text = normalize_text(text)
    raw_tokens = TOKEN_PATTERN.findall(text)

    tokens: list[str] = []
    for token in raw_tokens:
        tokens.append(token)

        if "/" in token or "-" in token:
            parts = re.split(r"[/-]", token)
            tokens.extend([p for p in parts if p])

    return dedupe_keep_order(tokens)


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    result: list[str] = []

    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)

    return result


def extract_query_metadata_signals(query: str) -> QueryMetadataSignals:
    normalized = normalize_text(query)
    normalized_no_acc = strip_accents(normalized)

    years = dedupe_keep_order(YEAR_PATTERN.findall(normalized))
    act_numbers = dedupe_keep_order(ACT_NUMBER_PATTERN.findall(normalized))

    act_types: list[str] = []
    for canonical, variants in ACT_TYPE_ALIASES.items():
        for variant in variants:
            if variant in normalized_no_acc:
                act_types.append(canonical)
                break

    query_tokens = tokenize_text(normalized_no_acc)
    siglas = [tok for tok in query_tokens if tok in SIGLA_SET]

    lexical_terms = [
        tok
        for tok in query_tokens
        if len(tok) >= 4 and tok not in LIGHT_STOPWORDS
    ]

    return QueryMetadataSignals(
        years=dedupe_keep_order(years),
        act_numbers=dedupe_keep_order(act_numbers),
        act_types=dedupe_keep_order(act_types),
        siglas=dedupe_keep_order(siglas),
        lexical_terms=dedupe_keep_order(lexical_terms),
    )



def normalize_metadata_value(value: Any) -> str:
    return normalize_text(str(value or ""))


def canonicalize_act_type(value: str) -> str:
    value_norm = strip_accents(normalize_text(value))

    for canonical, variants in ACT_TYPE_ALIASES.items():
        if value_norm in variants:
            return canonical

    for canonical, variants in ACT_TYPE_ALIASES.items():
        for variant in variants:
            if variant in value_norm:
                return canonical

    return value_norm


def token_overlap_ratio(
    query_terms: list[str],
    field_text: str,
    min_token_len: int = 4,
) -> float:
    if not query_terms or not field_text:
        return 0.0

    field_tokens = set(tokenize_text(strip_accents(field_text)))
    valid_query_terms = [t for t in query_terms if len(t) >= min_token_len]

    if not valid_query_terms:
        return 0.0

    matched = sum(1 for term in valid_query_terms if term in field_tokens)
    return matched / len(valid_query_terms)


def compute_metadata_boost_ratio(
    query: str,
    metadata: dict[str, Any],
    config: MetadataRerankConfig | None = None,
) -> tuple[float, dict[str, float]]:
    config = config or MetadataRerankConfig()
    signals = extract_query_metadata_signals(query)

    boost = 0.0
    reasons: dict[str, float] = {}

    ano = normalize_metadata_value(metadata.get("ano"))
    numero_titulo = normalize_metadata_value(metadata.get("numero_titulo"))
    tipo_ato_titulo = canonicalize_act_type(metadata.get("tipo_ato_titulo", ""))
    sigla_titulo = normalize_metadata_value(metadata.get("sigla_titulo"))
    assunto = normalize_metadata_value(metadata.get("assunto_normalizado"))
    ementa = normalize_metadata_value(metadata.get("ementa"))
    pdf_tipo = normalize_metadata_value(metadata.get("pdf_tipo"))
    autor = normalize_metadata_value(metadata.get("autor"))


    if signals.years and ano and any(year == ano for year in signals.years):
        boost += config.year_match_boost
        reasons["year_match"] = config.year_match_boost

    if signals.act_numbers and numero_titulo:
        for act_number in signals.act_numbers:
            if act_number == numero_titulo:
                boost += config.act_number_match_boost
                reasons["act_number_match"] = config.act_number_match_boost
                break

    if signals.act_types and tipo_ato_titulo:
        if tipo_ato_titulo in signals.act_types:
            boost += config.act_type_match_boost
            reasons["act_type_match"] = config.act_type_match_boost

    if signals.siglas and sigla_titulo:
        if sigla_titulo in signals.siglas:
            boost += config.sigla_match_boost
            reasons["sigla_match"] = config.sigla_match_boost


    assunto_overlap = token_overlap_ratio(
        signals.lexical_terms,
        assunto,
        min_token_len=config.min_token_len_for_overlap,
    )
    if assunto_overlap > 0:
        score = assunto_overlap * config.assunto_overlap_weight
        boost += score
        reasons["assunto_overlap"] = round(score, 6)

    ementa_overlap = token_overlap_ratio(
        signals.lexical_terms,
        ementa,
        min_token_len=config.min_token_len_for_overlap,
    )
    if ementa_overlap > 0:
        score = ementa_overlap * config.ementa_overlap_weight
        boost += score
        reasons["ementa_overlap"] = round(score, 6)

    pdf_tipo_overlap = token_overlap_ratio(
        signals.lexical_terms,
        pdf_tipo,
        min_token_len=config.min_token_len_for_overlap,
    )
    if pdf_tipo_overlap > 0:
        score = pdf_tipo_overlap * config.pdf_tipo_overlap_weight
        boost += score
        reasons["pdf_tipo_overlap"] = round(score, 6)

    autor_overlap = token_overlap_ratio(
        signals.lexical_terms,
        autor,
        min_token_len=config.min_token_len_for_overlap,
    )
    if autor_overlap > 0:
        score = autor_overlap * config.autor_overlap_weight
        boost += score
        reasons["autor_overlap"] = round(score, 6)

    boost = min(boost, config.max_total_boost_ratio)
    return boost, reasons



def rerank_results_with_metadata(
    results: list[dict],
    query: str,
    chunk_by_id: dict[str, PreparedChunk],
    config: MetadataRerankConfig | None = None,
) -> list[dict]:
    config = config or MetadataRerankConfig()

    if not results:
        return []

    reranked: list[dict] = []

    for rank_idx, result in enumerate(results):
        chunk_id = result.get("chunk_id")
        chunk = chunk_by_id.get(chunk_id)

        if chunk is None:
            reranked.append(result)
            continue

        base_score = float(result.get("score", 0.0))
        boost_ratio, reasons = compute_metadata_boost_ratio(
            query=query,
            metadata=chunk.metadata,
            config=config,
        )

        final_score = base_score * (1.0 + boost_ratio) if base_score > 0 else boost_ratio

        enriched_result = dict(result)
        enriched_result["score_bm25"] = base_score
        enriched_result["metadata_boost_ratio"] = round(boost_ratio, 6)
        enriched_result["score_final"] = round(final_score, 6)
        enriched_result["metadata_rerank_reasons"] = reasons
        enriched_result["_original_rank"] = rank_idx

        reranked.append(enriched_result)

    reranked.sort(
        key=lambda x: (
            x.get("score_final", x.get("score", 0.0)),
            x.get("score_bm25", x.get("score", 0.0)),
            -x.get("_original_rank", 0),
        ),
        reverse=True,
    )

    for item in reranked:
        item.pop("_original_rank", None)

    return reranked



def rerank_top_n_results_with_metadata(
    results: list[dict],
    query: str,
    chunk_by_id: dict[str, PreparedChunk],
    config: MetadataRerankConfig | None = None,
) -> list[dict]:
    config = config or MetadataRerankConfig()

    if not results:
        return []

    top_n = results[: config.top_n_rerank]
    tail = results[config.top_n_rerank :]

    reranked_top_n = rerank_results_with_metadata(
        results=top_n,
        query=query,
        chunk_by_id=chunk_by_id,
        config=config,
    )

    return reranked_top_n + tail