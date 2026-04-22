#rode no terminal: python3 -m src.ingest.03_create_chunks

import gc
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
    catalog_json: Path = base_dir / "data/interim/metadata/metadata_catalog.json"
    log_file: Path = base_dir / "data/logs/chunking.log"
    
    chunk_size: int = 1200
    chunk_overlap: int = 200

CONFIG = ChunkConfig()

def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def build_enriched_document(uid: str, raw_text: str, catalog: dict) -> str:
    meta = catalog.get(uid, {})
    
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
        raise ValueError("chunk_size muito pequeno para manter cabeçalho.")

    chunks = []
    start = 0
    body_length = len(body)

    while start < body_length:
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
        start = end - chunk_overlap

    return chunks

def process_chunks():
    setup_logging(CONFIG.log_file)
    CONFIG.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    
    if not CONFIG.catalog_json.exists():
        logging.error("Catálogo de metadados não encontrado!")
        return
        
    print("⏳ Carregando catálogo para a memória...")
    with open(CONFIG.catalog_json, "r", encoding="utf-8") as f:
        metadata_catalog = json.load(f)
        logging.info(f"Catálogo carregado: {len(metadata_catalog)} registros.")
    
    total_docs = 0
    total_chunks = 0
    
    print("\nIniciando geração de Chunks Enriquecidos (Modo Econômico de RAM)...")
    
    with open(CONFIG.input_jsonl, "r", encoding="utf-8") as fin, \
         open(CONFIG.output_jsonl, "w", encoding="utf-8") as fout:
        
        for idx, line in enumerate(fin):
            if not line.strip(): continue
            
            record = json.loads(line)
            uid = record["registro_uid"]
            raw_text = record["raw_text"]
            
            enriched_text = build_enriched_document(uid, raw_text, metadata_catalog)
            chunks = chunk_text_with_header(enriched_text, CONFIG.chunk_size, CONFIG.chunk_overlap)
            
            for i, chunk_text in enumerate(chunks):
                chunk_record = {
                    "chunk_id": f"{uid}_{i}",
                    "registro_uid": uid,
                    "text": chunk_text,
                    "metadata": metadata_catalog.get(uid, {})
                }
                fout.write(json.dumps(chunk_record, ensure_ascii=False) + "\n")
            
            total_docs += 1
            total_chunks += len(chunks)
            
            fout.flush() 
            
            del record, raw_text, enriched_text, chunks
            
            if idx % 100 == 0 and idx > 0:
                gc.collect() 
                print(f"Processados {idx} documentos... (RAM limpa)")

    print("\n" + "="*50)
    print("CHUNKING CONCLUÍDO COM SUCESSO!")
    print(f"Total de Documentos: {total_docs}")
    print(f"Total de Chunks Gerados: {total_chunks}")
    print("="*50 + "\n")
            
    logging.info(f"Processo finalizado: {total_docs} documentos renderam {total_chunks} chunks.")

if __name__ == "__main__":
    process_chunks()