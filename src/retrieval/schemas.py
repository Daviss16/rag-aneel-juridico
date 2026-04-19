from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict


@dataclass(slots=True)
class PreparedChunk:
    chunk_id: str
    registro_uid: str
    text_original: str
    text_retrieval: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CorpusStats:
    total_chunks: int
    total_documents: int
    avg_chunks_per_document: float
    min_chunks_per_document: int
    max_chunks_per_document: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)