#rode no terminal: python3 -m src.ingest.01_extract_text
#rode no terminal: python3 -m src.ingest.01_extract_text --reset PARA RESETAR O PROGRESSO E COMEÇAR DO INICIO


import os
import gc
import json
import logging
import pandas as pd
import sys
import time
import argparse
import shutil
import re
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
    output_jsonl: Path = base_dir / "data/interim/parsed/parsed_documents.jsonl"
    log_file: Path = base_dir / "data/logs/extract.log"
    tracker_file: Path = base_dir / "data/logs/processed_uids.txt" 
    
    drive_dir: Path = Path.home() / "meu_drive" / "lotes_baixados" 
    
    local_buffer_dir: Path = base_dir / "data/raw/documents/downloads/full_dataset"
    
    min_text_length: int = 80
    batch_size: int = 100 

CONFIG = ExtractConfig()

# ============================================================
# EXTRATORES
# ============================================================

class BaseExtractor:
    def extract(self, file_path: Path) -> Optional[str]:
        raise NotImplementedError

class PDFExtractor(BaseExtractor):
    def extract(self, file_path: Path) -> Optional[str]:
        text = ""
        try:
            with fitz.open(str(file_path)) as doc:
                for page in doc:
                    text += page.get_text()
            if not text.strip():
                with pdfplumber.open(file_path) as pdf:
                    text = " ".join([page.extract_text() or "" for page in pdf.pages])
            return text.strip()
        except Exception as e:
            logging.error(f"Erro no PDF {file_path.name}: {e}")
            return None

class HTMLExtractor(BaseExtractor):
    def extract(self, file_path: Path) -> Optional[str]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
                for script in soup(["script", "style"]):
                    script.decompose()
                return re.sub(r'\s+', ' ', soup.get_text()).strip()
        except Exception as e:
            logging.error(f"Erro no HTML {file_path.name}: {e}")
            return None

class ExtractorFactory:
    @staticmethod
    def get_extractor(file_path: Path) -> Optional[BaseExtractor]:
        suffix = file_path.suffix.lower()
        if suffix == '.pdf':
            return PDFExtractor()
        elif suffix in ['.html', '.htm']:
            return HTMLExtractor()
        return None

# ============================================================
# LÓGICA DE ESTADO
# ============================================================

def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_processed_uids(tracker_file: Path) -> set:
    if tracker_file.exists():
        with open(tracker_file, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def reset_progress():
    print("RESET: Apagando progresso e limpando pasta local...")
    if CONFIG.tracker_file.exists(): CONFIG.tracker_file.unlink()
    if CONFIG.output_jsonl.exists(): CONFIG.output_jsonl.unlink()
    if CONFIG.local_buffer_dir.exists():
        for f in CONFIG.local_buffer_dir.iterdir():
            if f.is_file(): f.unlink()
    print("Iniciando do zero.")

# ============================================================
# PROCESSO SINCRONIZADO
# ============================================================

def process_extraction():
    CONFIG.local_buffer_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(CONFIG.log_file)
    
    processed_uids = load_processed_uids(CONFIG.tracker_file)
    df = pd.read_csv(CONFIG.manifest_csv)
    df_pendentes = df[~df['registro_uid'].isin(processed_uids)]

    if df_pendentes.empty:
        print("Nada para processar!")
        return

    print("Mapeando arquivos no Drive...")
    mapa_drive = {f.stem: f for f in CONFIG.drive_dir.iterdir() if f.is_file()}

    sucesso_count = 0

    try:
        with open(CONFIG.output_jsonl, "a", encoding="utf-8") as fout, \
             open(CONFIG.tracker_file, "a", encoding="utf-8") as ftrack:

            for i in range(0, len(df_pendentes), CONFIG.batch_size):
                lote = df_pendentes.iloc[i : i + CONFIG.batch_size]
                buffer_atual = []

                print(f"\nLote {(i//CONFIG.batch_size)+1}...")

                for _, row in lote.iterrows():
                    uid = str(row['registro_uid'])
                    if uid in mapa_drive:
                        origem = mapa_drive[uid]
                        destino = CONFIG.local_buffer_dir / origem.name
                        if not destino.exists():
                            shutil.copyfile(origem, destino)
                        buffer_atual.append(destino)

                for arquivo_local in buffer_atual:
                    uid = arquivo_local.stem
                    extractor = ExtractorFactory.get_extractor(arquivo_local)
                    
                    if extractor:
                        text = extractor.extract(arquivo_local)
                        if text and len(text) >= CONFIG.min_text_length:
                            record = {"registro_uid": uid, "raw_text": text}
                            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                            ftrack.write(f"{uid}\n")
                            logging.info(f"[{uid}] Extraído.")
                            sucesso_count += 1
                        
                for arquivo_local in buffer_atual:
                    if arquivo_local.exists():
                        arquivo_local.unlink()
                
                fout.flush()
                ftrack.flush()
                gc.collect()

    except KeyboardInterrupt:
        print("\nInterrompido. Progresso salvo.")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    if args.reset:
        reset_progress()
    
    process_extraction()