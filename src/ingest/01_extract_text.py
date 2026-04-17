import os
import re
import json
import logging
import pandas as pd
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
    
    manifest_csv: Path = base_dir / "data/raw/selected/amostra_pdfs_150.csv"
    downloads_dir: Path = base_dir / "data/raw/documents/downloads"
    output_jsonl: Path = base_dir / "data/interim/parsed/parsed_documents.jsonl"
    log_file: Path = base_dir / "data/logs/extract.log"
    
    min_text_length: int = 80

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
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()

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

            pages: List[str] = []
            with pdfplumber.open(path) as pdf_plumb:
                for page_num in range(doc_fitz.page_count):
                    page_fitz = doc_fitz[page_num]
                    page_plumb = pdf_plumb.pages[page_num]
                    
                    tables = page_plumb.find_tables()
                    if tables:
                        page_text = page_plumb.extract_text() or ""
                        extracted_tables = page_plumb.extract_tables()
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
                        text = page_fitz.get_text()
                        if text: pages.append(text)

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
# PROCESSAMENTO
# ============================================================

def process_extraction():
    setup_logging(CONFIG.log_file)
    CONFIG.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(CONFIG.manifest_csv)
    
    with open(CONFIG.output_jsonl, "w", encoding="utf-8") as fout:
        for idx, row in df.iterrows():
            ano = str(row.get("ano", ""))
            arquivo = str(row.get("arquivo", ""))
            registro_uid = str(row.get("registro_uid", ""))
            
            if not arquivo or not ano: continue
            
            file_path = CONFIG.downloads_dir / ano / arquivo
            
            if file_path.exists() and not file_path.is_dir():
                
                # Instancia dinamicamente o extrator correto
                extractor = ExtractorFactory.get_extractor(file_path)
                if not extractor:
                    logging.warning(f"[{registro_uid}] Extensão não suportada: {file_path.suffix}")
                    continue
                
                extracted_text = extractor.extract(file_path)
                
                if extracted_text and len(extracted_text) >= CONFIG.min_text_length:
                    record = {
                        "registro_uid": registro_uid,
                        "raw_text": extracted_text,
                        "metadata": row.dropna().to_dict()
                    }
                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                    logging.info(f"[{registro_uid}] Extraído com sucesso via {extractor.__class__.__name__}.")

if __name__ == "__main__":
    process_extraction()