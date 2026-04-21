#Script usado para padronizar nomes dos PDFs da amostra inicial para registro_uid.pdf
#rode no terminal: python3 -m src.utils.rename_batches

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import urllib.parse
from pathlib import Path
import pandas as pd

def main():
    raiz_projeto = Path(__file__).resolve().parents[2]
    caminho_csv = raiz_projeto / "data" / "raw" / "selected" / "amostra_pdfs_150_v2.csv"
    pasta_pdfs = raiz_projeto / "data" / "raw" / "documents" / "downloads"

    if not caminho_csv.exists() or not pasta_pdfs.exists():
        print("Erro crítico: Caminho do CSV ou da pasta de PDFs não encontrado.")
        sys.exit(1)

    df = pd.read_csv(caminho_csv)
    
    if 'registro_uid' not in df.columns or 'arquivo' not in df.columns:
        print("Erro: Colunas 'registro_uid' ou 'arquivo' não encontradas no CSV.")
        sys.exit(1)

    contadores = {'sucesso': 0, 'nao_encontrado': 0, 'conflito': 0, 'erros': 0}

    for index, row in df.iterrows():
        uid = str(row['registro_uid']).strip()
        arquivo_original = str(row['arquivo']).strip()

        if not uid or uid.lower() == 'nan' or not arquivo_original or arquivo_original.lower() == 'nan':
            continue

        nome_limpo = urllib.parse.unquote(arquivo_original)
        
        extensao_real = Path(nome_limpo).suffix.lower()
        
        if not extensao_real:
            extensao_real = '.pdf'
        
        nome_novo = f"{uid}{extensao_real}"
        caminho_novo = pasta_pdfs / nome_novo

        if caminho_novo.exists():
            contadores['conflito'] += 1
            continue

        arquivo_encontrado = None
        caminho_exato = pasta_pdfs / nome_limpo

        if caminho_exato.exists() and caminho_exato.is_file():
            arquivo_encontrado = caminho_exato
        else:
            nome_base = Path(nome_limpo).stem.lower()

            for f in pasta_pdfs.iterdir():
                if f.is_file() and (nome_base in f.name.lower() or f.stem.lower() in nome_base):
                    arquivo_encontrado = f
                    break

        if arquivo_encontrado:
            try:
                arquivo_encontrado.rename(caminho_novo)
                contadores['sucesso'] += 1
            except Exception as e:
                print(f"Erro ao renomear {arquivo_encontrado.name}: {e}")
                contadores['erros'] += 1
        else:
            contadores['nao_encontrado'] += 1

    print(f"Concluído! Renomeados corretamente: {contadores['sucesso']} | Não encontrados: {contadores['nao_encontrado']} | Já existiam: {contadores['conflito']} | Erros: {contadores['erros']}")

if __name__ == "__main__":
    main()