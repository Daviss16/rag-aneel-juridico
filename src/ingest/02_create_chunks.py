#rode no terminal: python3 -m src.ingest.02_create_chunks.py

import json
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any

# ============================================================
# CONFIGURAÇÃO DE CHUNKING
# ============================================================

@dataclass(frozen=True)
class ChunkConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    
    input_jsonl: Path = base_dir / "data/interim/parsed/parsed_documents.jsonl"
    output_jsonl: Path = base_dir / "data/processed/chunks/chunks.jsonl"
    log_file: Path = base_dir / "data/logs/chunking.log"
    
    chunk_size: int = 1200
    chunk_overlap: int = 200

CONFIG = ChunkConfig()

def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def build_enriched_document(metadata: Dict[str, Any], raw_text: str) -> str:
    tipo_ato = metadata.get("tipo_ato_titulo", "")
    sigla = metadata.get("sigla_titulo", "")
    ano = metadata.get("ano", "")
    assunto = metadata.get("assunto_normalizado", "")
    
    revogada = metadata.get("revogada_flag", 0)
    alerta = "\n[ALERTA: DOCUMENTO REVOGADO. NÃO UTILIZAR COMO REGRA VIGENTE.]" if revogada == 1 else ""
    
    header = (
        f"DOCUMENTO: {tipo_ato} {sigla}\n"
        f"ANO: {ano}{alerta}\n"
        f"ASSUNTO: {assunto}\n"
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
        raise ValueError("chunk_size muito pequeno para manter cabeçalho.")

    chunks = []
    start = 0
    body_length = len(body)

    while start < body_length:
        end = start + effective_body_size
        body_chunk = body[start:end].strip()
        
        
        if body_chunk:
            chunks.append(f"{header}\n{body_chunk}".strip())

        if end >= body_length: break
        start += max(1, effective_body_size - chunk_overlap)

    return chunks

def process_chunks():
    setup_logging(CONFIG.log_file)
    CONFIG.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    
    total_docs = 0
    total_chunks = 0
    
    with open(CONFIG.input_jsonl, "r", encoding="utf-8") as fin, \
         open(CONFIG.output_jsonl, "w", encoding="utf-8") as fout:
        
        for line in fin:
            record = json.loads(line)
            uid = record["registro_uid"]
            metadata = record["metadata"]
            raw_text = record["raw_text"]
            
            enriched_text = build_enriched_document(metadata, raw_text)
            chunks = chunk_text_with_header(enriched_text, CONFIG.chunk_size, CONFIG.chunk_overlap)
            
            for i, chunk_text in enumerate(chunks):
                chunk_record = {
                    "chunk_id": f"{uid}_{i}",
                    "registro_uid": uid,
                    "text": chunk_text,
                    "metadata": metadata 
                }
                fout.write(json.dumps(chunk_record, ensure_ascii=False) + "\n")
            
            total_docs += 1
            total_chunks += len(chunks)
            
    logging.info(f"Processo finalizado: {total_docs} documentos renderam {total_chunks} chunks.")

if __name__ == "__main__":
    process_chunks()