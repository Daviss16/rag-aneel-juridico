#rode no terminal: python3 -m src.ingest.01_extract_text
#rode no terminal: python3 -m src.ingest.01_extract_text --reset PARA RESETAR O PROGRESSO E COMEÇAR DO INICIO

import os
import re
import gc
import json
import logging
import pandas as pd
import sys
import time
import fitz  
import pdfplumber
from bs4 import BeautifulSoup
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

# ============================================================
# CONFIGURAÇÃO DE EXTRAÇÃO
# ============================================================

@dataclass(frozen=True)
class ExtractConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    
    manifest_csv: Path = base_dir / "data/raw/selected/manifesto_3_unified_pdfs.csv"
    downloads_dir: Path = base_dir / "data/raw/documents/downloads/full_dataset"
    output_jsonl: Path = base_dir / "data/interim/parsed/parsed_documents.jsonl"
    log_file: Path = base_dir / "data/logs/extract.log"
    tracker_file: Path = base_dir / "data/logs/processed_uids.txt" 
    
    min_text_length: int = 80
    batch_size: int = 150 

CONFIG = ExtractConfig()

def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]
    )

def normalize_text(text: str) -> str:
    if not text: return ""
    text = text.replace("\x00", " ").replace("\r", "\n")
    text = re.sub(r'\|\s*\|\s*\|', '|', text) 
    text = re.sub(r'\|\s*\|', '|', text)
    linhas = text.split('\n')
    text = '\n'.join([linha for linha in linhas if linha.strip() != '|'])
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()

def carregar_uids_processados(tracker_path: Path) -> set:
    processados = set()
    if tracker_path.exists():
        with open(tracker_path, "r", encoding="utf-8") as f:
            for linha in f:
                uid = linha.strip()
                if uid:
                    processados.add(uid)
    return processados

# ============================================================
# EXTRATORES
# ============================================================

class BaseExtractor:
    def extract(self, path: Path) -> Optional[str]:
        raise NotImplementedError

class PDFExtractor(BaseExtractor):
    def extract(self, path: Path) -> Optional[str]:
        try:
            doc_fitz = fitz.open(path)
            if doc_fitz.page_count == 0:
                doc_fitz.close()
                return None
            
            custom_table_settings = {
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "intersection_x_tolerance": 15,
                "intersection_y_tolerance": 15,
                "snap_tolerance": 5,
                "join_tolerance": 5
            }

            pages: List[str] = []
            with pdfplumber.open(path) as pdf_plumb:
                for page_num in range(doc_fitz.page_count):
                    page_fitz = doc_fitz[page_num]
                    page_plumb = pdf_plumb.pages[page_num]
                    
                    tables = page_plumb.find_tables(table_settings=custom_table_settings)
                    if tables:
                        page_text = page_plumb.extract_text() or ""
                        
                        extracted_tables = page_plumb.extract_tables(table_settings=custom_table_settings)
                        
                        for table in extracted_tables:
                            if not table: continue
                            md_table = []
                            for i, row in enumerate(table):
                                clean_row = [str(cell).replace('\n', ' ').strip() if cell else "" for cell in row]
                                md_table.append("| " + " | ".join(clean_row) + " |")
                                if i == 0:
                                    md_table.append("|" + "|".join(["---"] * len(clean_row)) + "|")
                            
                            page_text += "\n\n" + "\n".join(md_table) + "\n\n"
                        pages.append(page_text)
                    else:
                        text_dict = page_fitz.get_text("dict")
                        clean_text_blocks = []
                        
                        for block in text_dict.get("blocks", []):
                            if block.get("type") == 0:  
                                for line in block.get("lines", []):
                                    line_text = ""
                                    for span in line.get("spans", []):
                                        if not (span.get("flags", 0) & 16):
                                            line_text += span.get("text", "")
                                            
                                    if line_text.strip():
                                        clean_text_blocks.append(line_text)
                                        
                        text = "\n".join(clean_text_blocks)
                        if text: 
                            pages.append(text)

            doc_fitz.close()
            return normalize_text("\n\n".join(pages))
        except Exception as e:
            logging.exception(f"Erro no PDF {path}: {e}")
            return None

class HTMLExtractor(BaseExtractor):
    INVALID_HTML_SIGNATURES = [
        "attention required",
        "cloudflare",
        "checking your browser",
        "please enable cookies",
        "about:blank",
        "nova guia",
        "new tab",
        "chrome://new-tab-page",
    ]

    def is_invalid_html(self, raw_html: str, text: str) -> bool:
        raw_html_lower = raw_html.lower()
        text_lower = text.lower()
        for sig in self.INVALID_HTML_SIGNATURES:
            if sig in raw_html_lower or sig in text_lower:
                return True
        if len(text.strip()) < 30:
            return True
        return False

    def extract(self, path: Path) -> Optional[str]:
        try:
            try:
                raw_html = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                raw_html = path.read_text(encoding="iso-8859-1")

            soup = BeautifulSoup(raw_html, "html.parser")

            for tag in soup(["script", "style", "nav", "noscript"]):
                tag.decompose()

            text = soup.get_text(separator="\n")
            text = normalize_text(text)

            if self.is_invalid_html(raw_html, text):
                logging.warning(f"HTML inválido/ruim descartado: {path}")
                return None

            return text if text else None
        except Exception as e:
            logging.exception(f"Erro ao extrair HTML: {path} | {e}")
            return None

class ExtractorFactory:
    @staticmethod
    def get_extractor(path: Path) -> Optional[BaseExtractor]:
        ext = path.suffix.lower()
        if ext == ".pdf":
            return PDFExtractor()
        if ext in {".html", ".htm"}:
            return HTMLExtractor()
        return None

# ============================================================
# PROCESSAMENTO (LÓGICA DE LOTES)
# ============================================================

def process_extraction():
    setup_logging(CONFIG.log_file)
    CONFIG.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    CONFIG.tracker_file.parent.mkdir(parents=True, exist_ok=True)
    
    if "--reset" in sys.argv:
        if CONFIG.output_jsonl.exists():
            CONFIG.output_jsonl.unlink() 
        if CONFIG.tracker_file.exists():
            CONFIG.tracker_file.unlink() 
        print("\n MODO RESET: Histórico apagado. Começando extração do zero...\n")

    df = pd.read_csv(CONFIG.manifest_csv)
    
    try:
        while True:
            uids_processados = carregar_uids_processados(CONFIG.tracker_file)
            df_pendentes = df[~df["registro_uid"].astype(str).isin(uids_processados)]
            
            if df_pendentes.empty:
                print("\n" + "="*50)
                print("TODOS OS DOCUMENTOS JÁ FORAM PROCESSADOS!")
                print("="*50 + "\n")
                logging.info("Processamento completo. Não há lotes pendentes.")
                break 
                
            df_lote = df_pendentes.head(CONFIG.batch_size)
            
            print("\n" + "="*50)
            print(f"PROCESSANDO LOTE DE {len(df_lote)} ARQUIVOS")
            print(f"Restam {len(df_pendentes) - len(df_lote)} arquivos pendentes no total.")
            print("="*50 + "\n")

            sucesso_count = 0
            erro_count = 0

            with open(CONFIG.output_jsonl, "a", encoding="utf-8") as fout, \
                 open(CONFIG.tracker_file, "a", encoding="utf-8") as ftrack:
                 
                for idx, row in df_lote.iterrows():
                    registro_uid = str(row.get("registro_uid", ""))
                    if not registro_uid: 
                        continue
                    
                    arquivos_encontrados = list(CONFIG.downloads_dir.glob(f"{registro_uid}.*"))
                    
                    if not arquivos_encontrados:
                        logging.warning(f"[{registro_uid}] Arquivo físico não encontrado.")
                        erro_count += 1
                    else:
                        file_path = arquivos_encontrados[0]
                        
                        if file_path.exists() and not file_path.is_dir():
                            extractor = ExtractorFactory.get_extractor(file_path)
                            if not extractor:
                                logging.warning(f"[{registro_uid}] Extensão não suportada: {file_path.suffix}")
                                erro_count += 1
                            else:
                                extracted_text = extractor.extract(file_path)
                                
                                if extracted_text and len(extracted_text) >= CONFIG.min_text_length:
                    
                                    record = {
                                    "registro_uid": registro_uid,
                                    "raw_text": extracted_text
                    
                                    }
                                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                                    logging.info(f"[{registro_uid}] Extraído com sucesso via {extractor.__class__.__name__}.")
                                    sucesso_count += 1
                                else:
                                    logging.warning(f"[{registro_uid}] Texto vazio/curto.")
                                    erro_count += 1
                
                    ftrack.write(registro_uid + "\n")
                    ftrack.flush()
                    fout.flush() 
                    gc.collect()

            print(f"Lote finalizado. Sucessos: {sucesso_count} | Erros: {erro_count}")
            time.sleep(5) 

    except KeyboardInterrupt:
        print("\n\n")
        print("EXECUÇÃO INTERROMPIDA PELO USUÁRIO (Ctrl+C)")
        print("Os arquivos jsonl e txt foram fechados com segurança.")
        print("Nenhum dado do progresso atual foi perdido.")
        print("\n")
        sys.exit(0)


if __name__ == "__main__":
    process_extraction()