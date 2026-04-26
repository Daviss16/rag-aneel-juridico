#rode no terminal: python3 -m src.retrieval.bm25_retriever <"query"> --top-k N

from __future__ import annotations

import argparse
import json
import pickle 
import sqlite3 
from dataclasses import dataclass
from pathlib import Path

from rank_bm25 import BM25Okapi
from src.common.utils_retriever import tokenize
from src.common.schemas import PreparedChunk
from retrieval.query_processing import process_query
from src.retrieval.metadata_reranker import (
    MetadataRerankConfig,
    rerank_top_n_results_with_metadata,
)

@dataclass(frozen=True)
class BM25Config:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    pkl_path: Path = base_dir / "data" / "retrieval" / "indexes" / "bm25_index.pkl"
    row_to_chunk_path: Path = base_dir / "data" / "retrieval" / "indexes" / "row_to_chunk_id.json"
    sqlite_path: Path = base_dir / "data" / "retrieval" / "indexes" / "retrieval_chunks.db" 


class BM25Retriever:
    def __init__(
        self, 
        config: BM25Config, 
        default_rerank_config: MetadataRerankConfig | None = None  
    ) -> None:
        self.config = config
        self.default_rerank_config = default_rerank_config

        with open(self.config.pkl_path, "rb") as f:
            self.index = pickle.load(f)
            
        with open(self.config.row_to_chunk_path, "r", encoding="utf-8") as f:
            self.row_to_chunk_id = json.load(f)

        self.conn = sqlite3.connect(self.config.sqlite_path, check_same_thread=False)

    def _fetch_chunks_from_sqlite(self, chunk_ids: list[str]) -> dict[str, PreparedChunk]:
        if not chunk_ids: return {}
        
        cursor = self.conn.cursor()
        
        placeholders = ",".join("?" * len(chunk_ids))
        query = f"SELECT chunk_id, registro_uid, text_original, text_retrieval, metadata FROM chunks WHERE chunk_id IN ({placeholders})"
        
        cursor.execute(query, chunk_ids)
        rows = cursor.fetchall()
        
        chunk_by_id = {}
        for row in rows:
            chunk_id, registro_uid, text_original, text_retrieval, metadata_str = row
            
            metadata = json.loads(metadata_str) if metadata_str else {}
            
            chunk_by_id[chunk_id] = PreparedChunk(
                chunk_id=chunk_id,
                registro_uid=registro_uid,
                text_original=text_original,
                text_retrieval=text_retrieval,
                metadata=metadata
            )
            
        return chunk_by_id

    def search(
        self,
        query: str,
        top_k: int = 5,
        candidate_k: int = 20,
        use_metadata_rerank: bool = True,
        use_query_processing: bool = True,
        metadata_rerank_config: MetadataRerankConfig | None = None,
    ) -> list[dict]:

        if not query or not query.strip():
            return []

        if use_query_processing:
            processed = process_query(query)
            query_tokens = processed.enriched_tokens
        else:
            query_tokens = tokenize(query)

        if not query_tokens:
            return []

        candidate_k = max(candidate_k, top_k)

        scores = self.index.get_scores(query_tokens)
        ranked_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True,
        )[:candidate_k]

        candidate_ids = [self.row_to_chunk_id[idx] for idx in ranked_indices]
        chunk_by_id = self._fetch_chunks_from_sqlite(candidate_ids)

        results = []
        for idx, chunk_id in zip(ranked_indices, candidate_ids):
            chunk = chunk_by_id.get(chunk_id)
            if chunk:
                res_dict = {
                    "chunk_id": chunk.chunk_id,
                    "registro_uid": chunk.registro_uid,
                    "score": float(scores[idx]),
                    "text_preview": chunk.text_original[:300],
                    "text_full": chunk.text_original,
                }
                
                if chunk.metadata:
                    res_dict.update(chunk.metadata)
                    
                results.append(res_dict)

        if use_metadata_rerank and results:
            config_to_use = metadata_rerank_config or self.default_rerank_config

            results = rerank_top_n_results_with_metadata(
                results=results,
                query=query,
                chunk_by_id=chunk_by_id, 
                config=config_to_use,
            )

        return results[:top_k]
        
    def __del__(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass

def build_bm25_retriever(
    config: BM25Config | None = None,
    rerank_config: MetadataRerankConfig | None = None  
) -> BM25Retriever:
    
    config = config or BM25Config() 
    
    return BM25Retriever(config=config, default_rerank_config=rerank_config) 

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
        print(f"score: {result.get('score_final', result.get('score', 0.0)):.4f}")
        print(f"text_preview: {result['text_preview']}")
        print()


if __name__ == "__main__":
    main()