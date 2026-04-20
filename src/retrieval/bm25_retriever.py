#rode no terminal: python3 -m src.retrieval.bm25_retriever <"query"> --top-k N

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

from rank_bm25 import BM25Okapi
from src.retrieval.schemas import PreparedChunk, load_prepared_chunks

@dataclass(frozen=True)
class BM25Config:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    prepared_chunks_path: Path = base_dir / "data" / "retrieval" / "prepared" / "prepared_chunks.jsonl"


TOKEN_PATTERN = re.compile(r"[a-zà-ÿ0-9]+")


def tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall((text or "").lower())


class BM25Retriever:
    def __init__(self, chunks: list[PreparedChunk]) -> None:
        if not chunks:
            raise ValueError("O corpus de chunks está vazio.")

        self.chunks = chunks
        self.tokenized_corpus = [tokenize(chunk.text_retrieval) for chunk in chunks]
        self.index = BM25Okapi(self.tokenized_corpus)

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        if not query or not query.strip():
            return []

        query_tokens = tokenize(query)
        if not query_tokens:
            return []

        scores = self.index.get_scores(query_tokens)
        ranked_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True,
        )[:top_k]

        results = []
        for idx in ranked_indices:
            chunk = self.chunks[idx]
            results.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "registro_uid": chunk.registro_uid,
                    "score": float(scores[idx]),
                    "text_preview": chunk.text_original[:300],
                }
            )

        return results
    

def build_bm25_retriever(
    config: BM25Config | None = None,
    chunks: list[PreparedChunk] | None = None 
) -> BM25Retriever:
    
    config = config or BM25Config() 
    
    if chunks is None:
        chunks = load_prepared_chunks(config.prepared_chunks_path)
        
    return BM25Retriever(chunks)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", type=str)
    parser.add_argument("--top-k", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    retriever = build_bm25_retriever()
    results = retriever.search(args.query, top_k=args.top_k)

    for result in results:
        print(f"chunk_id: {result['chunk_id']}")
        print(f"registro_uid: {result['registro_uid']}")
        print(f"score: {result['score']:.4f}")
        print(f"text_preview: {result['text_preview']}")
        print()


if __name__ == "__main__":
    main()