from __future__ import annotations

import re
import unicodedata
from typing import Optional


_WHITESPACE_RE = re.compile(r"\s+")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_whitespace(text: str) -> str:
    text = _CONTROL_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def normalize_for_retrieval(
    text: str,
    *,
    lowercase: bool = True,
    remove_accents: bool = False,
) -> str:
    
    if not text:
        return ""

    text = normalize_whitespace(text)

    if lowercase:
        text = text.lower()

    if remove_accents:
        text = strip_accents(text)

    return text


def build_retrieval_text(
    raw_chunk_text: str,
    *,
    lowercase: bool = True,
    remove_accents: bool = False,
) -> str:
    
    return normalize_for_retrieval(
        raw_chunk_text,
        lowercase=lowercase,
        remove_accents=remove_accents,
    )