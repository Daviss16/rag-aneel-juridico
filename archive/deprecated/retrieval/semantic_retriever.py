#rode no terminal: python3 -m src.retrieval.semantic_retriever <"query"> --top-k N 

from __future__ import annotations
import argparse
from dataclasses import dataclass
from pathlib import Path
import chromadb
from chromadb.utils import embedding_functions

@dataclass(frozen=True)
class SemanticConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    db_path: Path = base_dir / "data" / "retrieval" / "chroma_db"
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    collection_name: str = "aneel_retrieval"

class SemanticRetriever:
    def __init__(self, config: SemanticConfig):
        self.client = chromadb.PersistentClient(path=str(config.db_path))
        self.emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=config.model_name
        )
        self.collection = self.client.get_collection(
            name=config.collection_name, 
            embedding_function=self.emb_fn
        )

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        results = self.collection.query(
            query_texts=[query],
            n_results=top_k
        )
        
        formatted_results = []
        if results['ids']:
            for i in range(len(results['ids'][0])):
                chunk_id = results['ids'][0][i]
                metadata = results['metadatas'][0][i]
                
                distance = results['distances'][0][i] if results['distances'] else 0.0
                
                formatted_results.append({
                    "chunk_id": chunk_id,
                    "registro_uid": metadata.get("registro_uid"),
                    "score": 1.0 - distance,
                    "text_preview": metadata.get("text_original", "")[:300]
                })
                
        return formatted_results

def build_semantic_retriever(config: SemanticConfig | None = None) -> SemanticRetriever:
    config = config or SemanticConfig()
    return SemanticRetriever(config)

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