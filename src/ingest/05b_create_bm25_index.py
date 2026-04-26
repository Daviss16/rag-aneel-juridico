# rode no terminal: python3 -m src.ingest.05b_create_bm25_index

import pickle
import json
import re
from pathlib import Path
from rank_bm25 import BM25Okapi
from src.common.utils_retriever import tokenize
from common.data_loader import load_prepared_chunks, RetrievalPrepConfig

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def main():
    config = RetrievalPrepConfig()
    prepared_file = config.output_prepared_dir / "prepared_chunks.jsonl"
    
    print(f"Lendo corpus de: {prepared_file}")
    chunks = load_prepared_chunks(prepared_file, lowercase=False, remove_accents=False)

    print("Treinando modelo léxico BM25...")
    tokenized_corpus = [tokenize(chunk.text_retrieval) for chunk in chunks]
    bm25_index = BM25Okapi(tokenized_corpus)

    indexes_dir = BASE_DIR / "data/retrieval/indexes"
    indexes_dir.mkdir(parents=True, exist_ok=True)
    
    pkl_path = indexes_dir / "bm25_index.pkl"
    row_to_chunk_path = indexes_dir / "row_to_chunk_id.json"


    with open(pkl_path, "wb") as f:
        pickle.dump(bm25_index, f)

    row_to_chunk_id = [chunk.chunk_id for chunk in chunks]
    with open(row_to_chunk_path, "w", encoding="utf-8") as f:
        json.dump(row_to_chunk_id, f, ensure_ascii=False)

    print("Índice BM25 gerado e salvo com sucesso!")

if __name__ == "__main__":
    main()