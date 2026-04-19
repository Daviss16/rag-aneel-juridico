from __future__ import annotations

import json
import logging
from collections import defaultdict, Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable

from src.retrieval.schemas import PreparedChunk, CorpusStats
from src.retrieval.text_normalization import build_retrieval_text


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetrievalPrepConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent

    input_chunks_jsonl: Path = base_dir / "data/processed/chunks/chunks.jsonl"

    output_prepared_dir: Path = base_dir / "data/retrieval/prepared"
    output_indexes_dir: Path = base_dir / "data/retrieval/indexes"
    log_file: Path = base_dir / "data/logs/retrieval_prepare.log"

    lowercase: bool = True
    remove_accents: bool = False


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def _validate_chunk_record(obj: Dict[str, Any], line_num: int) -> None:
    required_fields = ("chunk_id", "registro_uid", "text")
    missing = [field for field in required_fields if not obj.get(field)]

    if missing:
        raise ValueError(
            f"Linha {line_num}: campos obrigatórios ausentes ou vazios: {missing}"
        )

    metadata = obj.get("metadata", {})
    if metadata is not None and not isinstance(metadata, dict):
        raise ValueError(
            f"Linha {line_num}: campo 'metadata' deve ser dict quando presente."
        )


def iter_prepared_chunks(
    input_path: Path,
    *,
    lowercase: bool = True,
    remove_accents: bool = False,
) -> Iterable[PreparedChunk]:
    with input_path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Erro ao parsear JSON na linha {line_num}: {exc}"
                ) from exc

            _validate_chunk_record(obj, line_num)

            text_original = obj["text"]
            text_retrieval = build_retrieval_text(
                text_original,
                lowercase=lowercase,
                remove_accents=remove_accents,
            )

            yield PreparedChunk(
                chunk_id=obj["chunk_id"],
                registro_uid=obj["registro_uid"],
                text_original=text_original,
                text_retrieval=text_retrieval,
                metadata=obj.get("metadata", {}) or {},
            )


def load_prepared_chunks(
    input_path: Path,
    *,
    lowercase: bool = True,
    remove_accents: bool = False,
) -> list[PreparedChunk]:
    return list(
        iter_prepared_chunks(
            input_path,
            lowercase=lowercase,
            remove_accents=remove_accents,
        )
    )


def build_chunk_id_to_row(chunks: list[PreparedChunk]) -> dict[str, int]:
    mapping: dict[str, int] = {}

    for idx, chunk in enumerate(chunks):
        if chunk.chunk_id in mapping:
            raise ValueError(f"chunk_id duplicado encontrado: {chunk.chunk_id}")
        mapping[chunk.chunk_id] = idx

    return mapping


def build_doc_to_chunk_ids(chunks: list[PreparedChunk]) -> dict[str, list[str]]:
    doc_to_chunk_ids: dict[str, list[str]] = defaultdict(list)

    for chunk in chunks:
        doc_to_chunk_ids[chunk.registro_uid].append(chunk.chunk_id)

    return dict(doc_to_chunk_ids)


def build_corpus_stats(chunks: list[PreparedChunk]) -> CorpusStats:
    if not chunks:
        return CorpusStats(
            total_chunks=0,
            total_documents=0,
            avg_chunks_per_document=0.0,
            min_chunks_per_document=0,
            max_chunks_per_document=0,
        )

    counter = Counter(chunk.registro_uid for chunk in chunks)
    counts = list(counter.values())

    return CorpusStats(
        total_chunks=len(chunks),
        total_documents=len(counter),
        avg_chunks_per_document=round(len(chunks) / len(counter), 4),
        min_chunks_per_document=min(counts),
        max_chunks_per_document=max(counts),
    )


def preview_chunks(chunks: list[PreparedChunk], n: int = 3) -> None:
    logger.info("Prévia de %s chunk(s):", n)

    for i, chunk in enumerate(chunks[:n], start=1):
        snippet = chunk.text_original[:250].replace("\n", " ")
        logger.info(
            "[%s] chunk_id=%s | registro_uid=%s | texto=%s...",
            i,
            chunk.chunk_id,
            chunk.registro_uid,
            snippet,
        )