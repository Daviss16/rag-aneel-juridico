#rode no terminal: python3 -m src.ingest.03_create_chunks

import os
import gc
import sqlite3
import json
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List

# ============================================================
# CONFIGURAÇÃO DE CHUNKING
# ============================================================

@dataclass(frozen=True)
class ChunkConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    
    input_jsonl: Path = base_dir / "data/interim/parsed/parsed_documents.jsonl"
    output_jsonl: Path = base_dir / "data/processed/chunks/chunks.jsonl"
    catalog_db: Path = base_dir / "data/interim/metadata/metadata_catalog.db"
    log_file: Path = base_dir / "data/logs/chunking.log"
    
    chunk_size: int = 2500
    chunk_overlap: int = 350

CONFIG = ChunkConfig()

def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def get_metadata_from_db(uid: str, cursor: sqlite3.Cursor) -> dict:
    cursor.execute("SELECT * FROM metadata WHERE registro_uid = ?", (uid,))
    row = cursor.fetchone()
    if not row:
        return {}
    
    return {
        "ano": row[1],
        "titulo": row[2],
        "sigla_titulo": row[3],
        "tipo_ato_titulo": row[4],
        "numero_titulo": row[5],
        "autor": row[6],
        "assunto_normalizado": row[7],
        "situacao_normalizada": row[8],
        "revogada_flag": row[9],
        "ementa": row[10],
        "pdf_tipo": row[11]
    }

def build_enriched_document(meta: dict, raw_text: str) -> str:
    tipo_ato = meta.get("tipo_ato_titulo", "Documento")
    sigla = meta.get("sigla_titulo", "")
    numero = meta.get("numero_titulo", "")
    ano = meta.get("ano", "")
    assunto = meta.get("assunto_normalizado", "Sem Assunto")
    autor = meta.get("autor", "ANEEL")
    ementa = meta.get("ementa", "Não disponível")
    
    revogada = meta.get("revogada_flag", 0)
    alerta = "\n[ALERTA: DOCUMENTO REVOGADO. NÃO UTILIZAR COMO REGRA VIGENTE.]" if str(revogada) == "1" else ""
    
    header = (
        f"DOCUMENTO: {tipo_ato} {numero} ({sigla})\n"
        f"AUTOR: {autor}\n"
        f"ANO: {ano}{alerta}\n"
        f"ASSUNTO: {assunto}\n"
        f"EMENTA: {ementa}\n"
        f"---\n"
    )
    return header + raw_text

def chunk_text_with_header(enriched_text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    marker = "\n---\n"
    if marker in enriched_text:
        parts = enriched_text.split(marker, 1)
        header = parts[0] + marker
        body = parts[1].lstrip()
    else:
        header, body = "", enriched_text

    if not body: return [enriched_text]

    effective_body_size = chunk_size - len(header)
    if effective_body_size <= 100:
        effective_body_size = 500 

    chunks = []
    start = 0
    body_length = len(body)
    
    loop_safeguard = 0 

    while start < body_length:
        loop_safeguard += 1
        if loop_safeguard > 10000:
            logging.error("ERRO CRÍTICO: Loop infinito evitado no chunking! Documento ignorado parcialmente.")
            break

        end = start + effective_body_size
        
        if end < body_length:
            ultimo_espaco = body.rfind(" ", start, end)
            ultima_quebra = body.rfind("\n", start, end)
            corte_ideal = max(ultimo_espaco, ultima_quebra)
            
            if corte_ideal > start + (effective_body_size // 2):
                end = corte_ideal

        body_chunk = body[start:end].strip()
        
        if body_chunk:
            chunks.append(f"{header}\n{body_chunk}".strip())

        if end >= body_length: break
        
        novo_start = end - chunk_overlap
        if novo_start <= start:
            start = start + 100  
        else:
            start = novo_start

    return chunks

def process_chunks():
    setup_logging(CONFIG.log_file)
    CONFIG.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    
    if not CONFIG.catalog_db.exists():
        logging.error(f"Catálogo SQLite não encontrado em: {CONFIG.catalog_db}. Rode o script 02 primeiro.")
        return
        
    conn = sqlite3.connect(CONFIG.catalog_db)
    cursor = conn.cursor()
    
    total_docs = 0
    total_chunks = 0
    docs_pulados = 0
    
    print("\nIniciando geração de Chunks...")
    
    with open(CONFIG.input_jsonl, "r", encoding="utf-8") as fin, \
         open(CONFIG.output_jsonl, "w", encoding="utf-8") as fout:
        
        for idx, line in enumerate(fin):
            if not line.strip(): continue
            
            try:
                record = json.loads(line)
                uid = record["registro_uid"]
                raw_text = record.get("raw_text", "")
                
                if len(raw_text) > 2500000:
                    logging.warning(f"\n[{uid}] IGNORADO: Documento muito grande ({len(raw_text)} caracteres).")
                    docs_pulados += 1
                    del record, raw_text
                    continue
                
                meta = get_metadata_from_db(uid, cursor)
                
                enriched_text = build_enriched_document(meta, raw_text)
                chunks = chunk_text_with_header(enriched_text, CONFIG.chunk_size, CONFIG.chunk_overlap)
                
                for i, chunk_text in enumerate(chunks):
                    chunk_record = {
                        "chunk_id": f"{uid}_{i}",
                        "registro_uid": uid,
                        "text": chunk_text,
                        "metadata": meta  
                    }
                    fout.write(json.dumps(chunk_record, ensure_ascii=False) + "\n")
                
                total_docs += 1
                total_chunks += len(chunks)
                
                fout.flush()
                os.fsync(fout.fileno())
                
                del record, raw_text, enriched_text, chunks
                
                if total_docs % 100 == 0:
                    gc.collect()
                    print(f"\n{total_docs} documentos salvos seguros... Chunks totais: {total_chunks}")
                    
            except MemoryError:
                print(f"\nESTOURO DE MEMÓRIA CAPTURADO no documento. Pulando para sobreviver...")
                docs_pulados += 1
                gc.collect()
                continue
            except Exception as e:
                print(f"\nErro no documento {uid}: {e}")
                continue

    conn.close()
    
    print("\n" + "="*50)
    print("CHUNKING CONCLUÍDO COM SUCESSO!")
    print(f"Total de Documentos Processados: {total_docs}")
    print(f"Total de Chunks Gerados: {total_chunks}")
    if docs_pulados > 0:
        print(f"Documentos Monstros Pulados: {docs_pulados}")
    print("="*50 + "\n")
    
    logging.info(f"Processo finalizado: {total_docs} documentos renderam {total_chunks} chunks.")

if __name__ == "__main__":
    process_chunks()