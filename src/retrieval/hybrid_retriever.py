#rode no terminal: python3 -m src.retrieval.hybrid_retriever <"query"> --top-k N

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from src.retrieval.bm25_retriever import BM25Retriever, BM25Config, load_prepared_chunks, tokenize, build_bm25_retriever
from src.retrieval.semantic_retriever import SemanticRetriever, SemanticConfig, build_semantic_retriever
from src.retrieval.metadata_reranker import (
    MetadataRerankConfig,
    compute_metadata_boost_ratio,
)

@dataclass(frozen=True)
class HybridConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    candidate_k: int = 20
    weight_bm25: float = 0.65
    weight_semantic: float = 0.25
    weight_metadata: float = 0.10


def min_max_normalize(values: np.ndarray) -> np.ndarray:
    if values.size == 0:
        return values

    min_val = values.min()
    max_val = values.max()

    if max_val - min_val < 1e-10:
        return np.ones_like(values)

    return (values - min_val) / (max_val - min_val)


class HybridRetriever:
    def __init__(
        self,
        bm25_retriever: BM25Retriever,
        semantic_retriever: SemanticRetriever,
        candidate_k: int = 20,
        weight_bm25: float = 0.65,
        weight_semantic: float = 0.25,
        weight_metadata: float = 0.10,
    ) -> None:
        
        self.bm25 = bm25_retriever
        self.semantic = semantic_retriever
        self.candidate_k = candidate_k

        self.weight_bm25 = weight_bm25
        self.weight_semantic = weight_semantic
        self.weight_metadata = weight_metadata

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        query_tokens = tokenize(query)
        bm25_scores = np.array(self.bm25.index.get_scores(query_tokens))

        candidate_indices = np.argsort(bm25_scores)[::-1][: self.candidate_k]

        query_vec = self.semantic.model.encode([query], convert_to_numpy=True)
        query_vec = self.semantic._normalize(query_vec)

        candidate_embeddings = self.semantic.embeddings[candidate_indices]
        semantic_scores = np.dot(candidate_embeddings, query_vec.T).squeeze()

        candidate_bm25_scores = bm25_scores[candidate_indices]

        metadata_scores = []
        for corpus_idx in candidate_indices:
            chunk = self.bm25.chunks[corpus_idx]
            boost_ratio, _ = compute_metadata_boost_ratio(
                query=query,
                metadata=chunk.metadata,
                config=MetadataRerankConfig(),
            )
            metadata_scores.append(boost_ratio)

        metadata_scores = np.array(metadata_scores, dtype=float)

        norm_bm25 = min_max_normalize(candidate_bm25_scores)
        norm_semantic = min_max_normalize(np.array(semantic_scores))
        norm_metadata = min_max_normalize(metadata_scores)


        final_scores = (
            self.weight_bm25 * norm_bm25
            + self.weight_semantic * norm_semantic
            + self.weight_metadata * norm_metadata
        )

        reranked = np.argsort(final_scores)[::-1][:top_k]

        results = []
        for local_idx in reranked:
            corpus_idx = candidate_indices[local_idx]
            chunk = self.bm25.chunks[corpus_idx]

            results.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "registro_uid": chunk.registro_uid,
                    "score_bm25": float(candidate_bm25_scores[local_idx]),
                    "score_semantic": float(semantic_scores[local_idx]),
                    "score_metadata": float(metadata_scores[local_idx]),
                    "score_final": float(final_scores[local_idx]),
                    "text_preview": chunk.text_original[:300],
                }
            )

        return results
    

def build_hybrid_retriever(config: HybridConfig | None = None) -> HybridRetriever:
    config = config or HybridConfig()
    
    bm25_config = BM25Config()
    chunks_compartilhados = load_prepared_chunks(bm25_config.prepared_chunks_path)
    
    bm25_instance = build_bm25_retriever(chunks=chunks_compartilhados)
    semantic_instance = build_semantic_retriever(chunks=chunks_compartilhados)
    
    return HybridRetriever(
        bm25_retriever=bm25_instance,
        semantic_retriever=semantic_instance,
        candidate_k=config.candidate_k, 
        weight_bm25=config.weight_bm25,
        weight_semantic=config.weight_semantic,
        weight_metadata=config.weight_metadata,
    )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", type=str)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--candidate-k", type=int, default=20)
    parser.add_argument("--alpha", type=float, default=0.7)
    return parser.parse_args()

def main():
    args = parse_args()
    
    config = HybridConfig(candidate_k=args.candidate_k, alpha=args.alpha)
    
    retriever = build_hybrid_retriever(config)
    
    results = retriever.search(args.query, top_k=args.top_k)

    for result in results:
        print(result)

if __name__ == "__main__":
    main()