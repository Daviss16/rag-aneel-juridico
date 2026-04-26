# rode no terminal: python3 -m src.ingest.05a_import_to_sqlite

import sqlite3
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_JSONL = BASE_DIR / "data" / "retrieval" / "prepared" / "prepared_chunks.jsonl"
OUTPUT_DB = BASE_DIR / "data" / "retrieval" / "indexes" / "retrieval_chunks.db"

def create_db():
    print(f"Criando banco de dados em: {OUTPUT_DB}")
    OUTPUT_DB.parent.mkdir(parents=True, exist_ok=True)
    
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()
        
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE chunks (
            chunk_id TEXT PRIMARY KEY,
            registro_uid TEXT,
            text_original TEXT,
            text_retrieval TEXT,
            metadata TEXT
        )
    """)
    
    count = 0
    print(f"Lendo dados de {INPUT_JSONL}...")
    
    with open(INPUT_JSONL, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            
            metadata_str = json.dumps(data.get("metadata", {}), ensure_ascii=False)
            
            cursor.execute("""
                INSERT INTO chunks (chunk_id, registro_uid, text_original, text_retrieval, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, (
                data.get("chunk_id"),
                data.get("registro_uid"),
                data.get("text_original"),
                data.get("text_retrieval"),
                metadata_str
            ))
            count += 1
            
            if count % 5000 == 0:
                conn.commit()
                print(f"{count} chunks inseridos...")
                
    conn.commit()
    
    print("Criando índices para acelerar a busca...")
    cursor.execute("CREATE INDEX idx_registro_uid ON chunks(registro_uid);")
    
    conn.close()
    
    print("\n" + "="*50)
    print("IMPORTAÇÃO PARA SQLITE CONCLUÍDA!")
    print(f"Total de chunks: {count}")
    print(f"Arquivo gerado: {OUTPUT_DB}")
    print("="*50 + "\n")

if __name__ == "__main__":
    create_db()