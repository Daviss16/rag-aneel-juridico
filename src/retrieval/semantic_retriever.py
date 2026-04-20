#rode no terminal: python3 -m src.retrieval.semantic_retriever <"query"> --top-k N 

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from sentence_transformers import SentenceTransformer
from src.retrieval.schemas import PreparedChunk, load_prepared_chunks


@dataclass(frozen=True)
class SemanticConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    prepared_chunks_path: Path = base_dir / "data" / "retrieval" / "prepared" / "prepared_chunks.jsonl"
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

@dataclass(frozen=True)
class RetrievalChunk:
    chunk_id: str
    registro_uid: str
    text: str
    metadata: dict


class SemanticRetriever:
    def __init__(self, chunks: list[PreparedChunk], model_name: str):
        self.chunks = chunks
        self.model = SentenceTransformer(model_name)

        texts = [c.text_retrieval for c in chunks]
        self.embeddings = self.model.encode(texts, show_progress_bar=True, convert_to_numpy=True)

        self.embeddings = self._normalize(self.embeddings)


    def _normalize(self, vectors: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        return vectors / np.clip(norms, 1e-10, None)
    

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        query_vec = self.model.encode([query], convert_to_numpy=True)
        query_vec = self._normalize(query_vec)

        scores = np.dot(self.embeddings, query_vec.T).squeeze()

        top_indices = np.argsort(scores)[::-1][:top_k]

        results = []
        for idx in top_indices:
            chunk = self.chunks[idx]

            results.append({
                "chunk_id": chunk.chunk_id,
                "registro_uid": chunk.registro_uid,
                "score": float(scores[idx]),
                "text_preview": chunk.text_original[:300],
            })

        return results
    

def build_semantic_retriever(
    config: SemanticConfig | None = None, 
    chunks: list[PreparedChunk] | None = None 
) -> SemanticRetriever:
    
    config = config or SemanticConfig()
    
    if chunks is None:
        chunks = load_prepared_chunks(config.prepared_chunks_path)
        
    return SemanticRetriever(chunks, config.model_name)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", type=str)
    parser.add_argument("--top-k", type=int, default=5)
    return parser.parse_args()


def main():
    args = parse_args()
    retriever = build_semantic_retriever()
    results = retriever.search(args.query, top_k=args.top_k)

    for r in results:
        print(r)


if __name__ == "__main__":
    main()