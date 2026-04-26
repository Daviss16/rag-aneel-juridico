#rode no terminal: python3 -m src.retrieval.hybrid_retriever <"query"> --top-k N

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import chromadb 

from src.retrieval.bm25_retriever import BM25Retriever, BM25Config, build_bm25_retriever
from src.retrieval.semantic_retriever import SemanticRetriever, SemanticConfig, build_semantic_retriever
from src.common.utils_retriever import tokenize 
from src.common.schemas import PreparedChunk 
from src.retrieval.metadata_reranker import (
    MetadataRerankConfig,
    compute_metadata_boost_ratio,
)

@dataclass(frozen=True)
class HybridConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    db_path: Path = base_dir / "data" / "retrieval" / "chroma_db"
    collection_name: str = "aneel_retrieval"
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
        config: HybridConfig, 
    ) -> None:
        
        self.bm25 = bm25_retriever
        self.semantic = semantic_retriever
        self.config = config

        self.client = chromadb.PersistentClient(path=str(self.config.db_path))
        self.collection = self.client.get_collection(name=self.config.collection_name)


    def _fetch_chunks_from_chroma(self, chunk_ids: list[str]) -> dict[str, PreparedChunk]:
        if not chunk_ids: return {}
        
        db_results = self.collection.get(ids=chunk_ids)
        chunk_by_id = {}
        for i, cid in enumerate(db_results['ids']):
            meta = db_results['metadatas'][i]
            chunk_by_id[cid] = PreparedChunk(
                chunk_id=cid,
                registro_uid=meta.get("registro_uid", ""),
                text_original=meta.get("text_original", ""),
                text_retrieval=db_results['documents'][i],
                metadata=meta
            )
        return chunk_by_id

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        query_tokens = tokenize(query)
        if not query_tokens:
            return []

        bm25_scores_all = np.array(self.bm25.index.get_scores(query_tokens))
        top_bm25_indices = np.argsort(bm25_scores_all)[::-1][: self.config.candidate_k]
        
        bm25_candidate_ids = [self.bm25.row_to_chunk_id[idx] for idx in top_bm25_indices]
        candidate_bm25_scores = bm25_scores_all[top_bm25_indices]

        semantic_results = self.semantic.search(query, top_k=self.config.candidate_k)

        all_candidate_ids = list(set(bm25_candidate_ids + [r["chunk_id"] for r in semantic_results]))
        chunk_by_id = self._fetch_chunks_from_chroma(all_candidate_ids)

        aligned_bm25 = []
        aligned_semantic = []
        metadata_scores = []
        valid_ids = []

        bm25_score_map = {cid: score for cid, score in zip(bm25_candidate_ids, candidate_bm25_scores)}
        semantic_score_map = {r["chunk_id"]: r["score"] for r in semantic_results}

        for cid in all_candidate_ids:
            chunk = chunk_by_id.get(cid)
            if not chunk: continue 

            valid_ids.append(cid)
            aligned_bm25.append(bm25_score_map.get(cid, 0.0))
            aligned_semantic.append(semantic_score_map.get(cid, 0.0))
            
            boost_ratio, _ = compute_metadata_boost_ratio(
                query=query,
                metadata=chunk.metadata,
                config=MetadataRerankConfig(),
            )
            metadata_scores.append(boost_ratio)

        if not valid_ids:
            return []

        metadata_scores = np.array(metadata_scores, dtype=float)

        norm_bm25 = min_max_normalize(np.array(aligned_bm25))
        norm_semantic = min_max_normalize(np.array(aligned_semantic))
        norm_metadata = min_max_normalize(metadata_scores)

        final_scores = (
            self.config.weight_bm25 * norm_bm25
            + self.config.weight_semantic * norm_semantic
            + self.config.weight_metadata * norm_metadata
        )

        reranked = np.argsort(final_scores)[::-1][:top_k]

        results = []
        for local_idx in reranked:
            cid = valid_ids[local_idx]
            chunk = chunk_by_id[cid]

            results.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "registro_uid": chunk.registro_uid,
                    "score_bm25": float(aligned_bm25[local_idx]),
                    "score_semantic": float(aligned_semantic[local_idx]),
                    "score_metadata": float(metadata_scores[local_idx]),
                    "score_final": float(final_scores[local_idx]),
                    "text_preview": chunk.text_original[:300],
                }
            )

        return results
    

def build_hybrid_retriever(config: HybridConfig | None = None) -> HybridRetriever:
    config = config or HybridConfig()
    
    bm25_instance = build_bm25_retriever()
    semantic_instance = build_semantic_retriever()
    
    return HybridRetriever(
        bm25_retriever=bm25_instance,
        semantic_retriever=semantic_instance,
        config=config,
    )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", type=str)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--candidate-k", type=int, default=20)
    parser.add_argument("--weight-bm25", type=float, default=0.65)
    parser.add_argument("--weight-semantic", type=float, default=0.25)
    parser.add_argument("--weight-metadata", type=float, default=0.10)
    return parser.parse_args()

def main():
    args = parse_args()
    
    config = HybridConfig(
        candidate_k=args.candidate_k, 
        weight_bm25=args.weight_bm25,
        weight_semantic=args.weight_semantic,
        weight_metadata=args.weight_metadata
    )
    
    retriever = build_hybrid_retriever(config)
    
    results = retriever.search(args.query, top_k=args.top_k)

    for result in results:
        print(f"chunk_id: {result['chunk_id']}")
        print(f"registro_uid: {result['registro_uid']}")
        print(f"score_final: {result['score_final']:.4f} (BM25: {result['score_bm25']:.2f} | Sem: {result['score_semantic']:.2f} | Meta: {result['score_metadata']:.2f})")
        print(f"text_preview: {result['text_preview']}")
        print("-" * 50)

if __name__ == "__main__":
    main()