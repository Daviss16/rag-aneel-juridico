from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
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
    

def load_prepared_chunks(path: Path) -> list[PreparedChunk]:
    chunks: list[PreparedChunk] = []

    with path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            chunk_id = obj.get("chunk_id")
            registro_uid = obj.get("registro_uid")
            text = obj.get("text_retrieval") or obj.get("text") or ""
            metadata = obj.get("metadata") or {}

            if not chunk_id or not registro_uid or not text:
                continue 

            chunks.append(
                PreparedChunk(
                    chunk_id=chunk_id,
                    registro_uid=registro_uid,
                    text_original=obj.get("text_original") or obj.get("text") or "",
                    text_retrieval=text,
                    metadata=metadata,
                )
            )

    return chunks