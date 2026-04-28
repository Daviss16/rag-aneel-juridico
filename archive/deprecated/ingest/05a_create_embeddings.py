#rode no terminal: python3 -m src.ingest.05a_create_embeddings

import logging
import json
from pathlib import Path
import chromadb
from chromadb.utils import embedding_functions
from src.common.data_loader import load_prepared_chunks, RetrievalPrepConfig

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_DIR = BASE_DIR / "data/retrieval/chroma_db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def ingest_to_chroma():
    client = chromadb.PersistentClient(path=str(DB_DIR))
    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="sentence-transformers/all-MiniLM-L6-v2"
    )
    
    collection = client.get_or_create_collection(
        name="aneel_retrieval",
        embedding_function=emb_fn,
        metadata={"hnsw:space": "cosine"}
    )

    logging.info("Consultando IDs já existentes no banco para evitar reprocessamento...")
    existing_ids_data = collection.get(include=[])
    existing_ids = set(existing_ids_data['ids'])
    logging.info(f"O banco já possui {len(existing_ids)} chunks processados.")

    config = RetrievalPrepConfig()
    prepared_file = config.output_prepared_dir / "prepared_chunks.jsonl"
    
    chunks = load_prepared_chunks(prepared_file, lowercase=True, remove_accents=False) 
    
    batch_size = 150
    ids, documents, metadatas = [], [], []
    skipped_count = 0

    for idx, chunk in enumerate(chunks):
        if chunk.chunk_id in existing_ids:
            skipped_count += 1
            continue

        ids.append(chunk.chunk_id)
        documents.append(chunk.text_retrieval)
        
        meta = chunk.metadata.copy()
        meta["text_original"] = chunk.text_original
        meta["registro_uid"] = chunk.registro_uid
        clean_meta = {k: str(v) for k, v in meta.items() if v is not None}
        metadatas.append(clean_meta)

        if len(ids) >= batch_size:
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
            ids, documents, metadatas = [], [], []
            logging.info(f"Vetorizados e salvos: {idx + 1} / {len(chunks)} (Saltados: {skipped_count})")

    if ids:
        collection.upsert(ids=ids, documents=documents, metadatas=metadatas)

    logging.info(f"Ingestão concluída. Processados novos: {len(chunks) - skipped_count} | Saltados: {skipped_count}")

if __name__ == "__main__":
    ingest_to_chroma()