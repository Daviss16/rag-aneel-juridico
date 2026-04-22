#rode no terminal: python3 -m src.ingest.02_build_metadata_catalog

import os
import json
import logging
import re
from pathlib import Path
from typing import Dict, Any

# ============================================================
# CONFIGURAÇÃO
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_JSON_DIR = BASE_DIR / "data/raw/json"
OUTPUT_CATALOG = BASE_DIR / "data/interim/metadata/metadata_catalog.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def clean_prefix(value: Any, prefix: str) -> str:
    text = str(value or "")
    if text.startswith(prefix):
        return text[len(prefix):].strip()
    return text.strip()

def parse_titulo(titulo: str):
    titulo = titulo or ""
    parts = titulo.split(" - ")
    sigla = parts[0].strip() if len(parts) > 0 else ""
    resto = parts[1].strip() if len(parts) > 1 else titulo
    
    tipo_ato = ""
    numero_ato = ""
    if resto:
        subparts = resto.split(" ")
        tipo_ato = subparts[0].strip()
        numero_ato = " ".join(subparts[1:]).strip() if len(subparts) > 1 else ""
        
    return sigla, tipo_ato, numero_ato

def build_catalog():
    OUTPUT_CATALOG.parent.mkdir(parents=True, exist_ok=True)
    catalog: Dict[str, Dict[str, Any]] = {}
    
    total_jsons = 0
    total_uids = 0

    json_files = sorted(RAW_JSON_DIR.glob("*.json"))

    for json_path in json_files:
        filename = json_path.stem
        
        match = re.search(r'(20\d{2})', filename)
        ano = match.group(1) if match else "0000"
        
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            record_idx = 0
            
            for date_key, date_content in data.items():
                registros = date_content.get("registros", []) if isinstance(date_content, dict) else []
                if not isinstance(registros, list):
                    continue
                
                for item in registros:
                    if not isinstance(item, dict):
                        continue
                        
                    record_idx += 1
                    
                    titulo_bruto = item.get("titulo", "")
                    sigla, tipo_ato, numero_ato = parse_titulo(titulo_bruto)
                    
                    autor = item.get("autor", "ANEEL")
                    assunto = clean_prefix(item.get("assunto", ""), "Assunto:")
                    situacao = clean_prefix(item.get("situacao", ""), "Situação:")
                    
                    ementa_bruta = item.get("ementa")
                    if ementa_bruta is None or str(ementa_bruta).strip() == "None":
                        ementa_limpa = "Não disponível"
                    else:
                        ementa_limpa = " ".join(str(ementa_bruta).split()).replace(" Imprimir", "").strip()
                    
                    revogada_flag = 1 if "REVOGADA" in situacao.upper() else 0
                    
                    pdfs = item.get("pdfs", [])
                    if not isinstance(pdfs, list):
                        pdfs = []
                        
                    for pdf_idx, pdf_info in enumerate(pdfs, start=1):
                        if not isinstance(pdf_info, dict):
                            continue
                            
                        registro_uid = f"{ano}_{record_idx:05d}_pdf{pdf_idx}"
                        
                        catalog[registro_uid] = {
                            "ano": ano,
                            "titulo": titulo_bruto,
                            "sigla_titulo": sigla,
                            "tipo_ato_titulo": tipo_ato,
                            "numero_titulo": numero_ato,
                            "autor": autor,
                            "assunto_normalizado": assunto,
                            "situacao_normalizada": situacao,
                            "revogada_flag": revogada_flag,
                            "ementa": ementa_limpa,
                            "pdf_tipo": clean_prefix(pdf_info.get("tipo", ""), ":").strip()
                        }
                        total_uids += 1
                        
            total_jsons += 1
            logging.info(f"Processado: {filename} ({record_idx} registros) -> Ano extraído: {ano}")
            
        except Exception as e:
            logging.error(f"Erro em {filename}: {e}")

    with open(OUTPUT_CATALOG, "w", encoding="utf-8") as fout:
        json.dump(catalog, fout, ensure_ascii=False, indent=2)
    
    print("\n" + "="*50)
    print(f"CATÁLOGO DE METADADOS CONCLUÍDO")
    print(f"Total de registros_uid mapeados: {total_uids}")
    print("="*50 + "\n")

if __name__ == "__main__":
    build_catalog()