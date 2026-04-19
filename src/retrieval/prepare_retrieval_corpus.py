#rode no terminal:  python3 -m src.retrieval.prepare_retrieval_corpus

from __future__ import annotations

import json
from pathlib import Path

base_dir = Path(__file__).resolve().parent.parent.parent

from src.retrieval.data_loader import (
    RetrievalPrepConfig,
    setup_logging,
    load_prepared_chunks,
    build_chunk_id_to_row,
    build_doc_to_chunk_ids,
    build_corpus_stats,
    preview_chunks,
)
from src.retrieval.schemas import PreparedChunk


CONFIG = RetrievalPrepConfig()


def save_prepared_chunks(chunks: list[PreparedChunk], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk.to_dict(), ensure_ascii=False) + "\n")


def save_json(data: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> None:
    setup_logging(CONFIG.log_file)

    chunks = load_prepared_chunks(
        CONFIG.input_chunks_jsonl,
        lowercase=CONFIG.lowercase,
        remove_accents=CONFIG.remove_accents,
    )

    chunk_id_to_row = build_chunk_id_to_row(chunks)
    doc_to_chunk_ids = build_doc_to_chunk_ids(chunks)
    stats = build_corpus_stats(chunks)

    retrieval_dir = base_dir / "data" / "retrieval"

    prepared_chunks_path = retrieval_dir / "prepared" / "prepared_chunks.jsonl"
    chunk_id_to_row_path = retrieval_dir / "indexes" / "chunk_id_to_row.json"
    doc_to_chunk_ids_path = retrieval_dir / "indexes" / "doc_to_chunk_ids.json"
    corpus_stats_path = retrieval_dir / "indexes" / "corpus_stats.json"

    save_prepared_chunks(chunks, prepared_chunks_path)
    save_json(chunk_id_to_row, chunk_id_to_row_path)
    save_json(doc_to_chunk_ids, doc_to_chunk_ids_path)
    save_json(stats.to_dict(), corpus_stats_path)

    preview_chunks(chunks, n=3)

    print("\nCorpus de retrieval preparado com sucesso.")
    print(f"Prepared chunks: {prepared_chunks_path}")
    print(f"Chunk ID -> Row:  {chunk_id_to_row_path}")
    print(f"Doc -> Chunk IDs: {doc_to_chunk_ids_path}")
    print(f"Corpus stats:     {corpus_stats_path}")
    print()
    print(f"Total de chunks:      {stats.total_chunks}")
    print(f"Total de documentos:  {stats.total_documents}")
    print(f"Média chunks/doc:     {stats.avg_chunks_per_document}")
    print(f"Mín chunks/doc:       {stats.min_chunks_per_document}")
    print(f"Máx chunks/doc:       {stats.max_chunks_per_document}")


if __name__ == "__main__":
    main()