#rode no terminal: python3 -m src.utils.split_manifest

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import sys

def main():
    base_dir = Path(__file__).resolve().parents[2]
    caminho_mestre = base_dir / "data" / "raw" / "selected" / "fila_downloads_mestre.csv"
    
    if not caminho_mestre.exists():
        print(f"Erro: O arquivo '{caminho_mestre}' não foi encontrado.")
        sys.exit(1)

    print(f"Lendo manifesto mestre: {caminho_mestre}...")
    df = pd.read_csv(caminho_mestre)

    if 'status_processamento' not in df.columns:
        print("Erro: A coluna 'status_processamento' não existe neste CSV.")
        sys.exit(1)

    mascara_sucesso = df['status_processamento'] == 'baixado_local'
    df_sucesso = df[mascara_sucesso].copy()
    df_falhas = df[~mascara_sucesso].copy()

    colunas_para_remover = [
        'url', 'arquivo', 'baixado_json', 'cat_key',
        'is_first_of_kind', 'score_prioridade', 'status_processamento',
        'tentativas', 'mensagem_erro'
    ]

    df_sucesso = df_sucesso.drop(columns=colunas_para_remover, errors='ignore')
    df_falhas = df_falhas.drop(columns=colunas_para_remover, errors='ignore')

    pasta_destino = caminho_mestre.parent
    caminho_sucesso = pasta_destino / "manifesto_1_sucesso_pdfs.csv"
    caminho_falhas = pasta_destino / "manifesto_2_pendentes_pdfs.csv"

    df_sucesso.to_csv(caminho_sucesso, index=False)
    df_falhas.to_csv(caminho_falhas, index=False)

    print("\n--- Separação e Limpeza Concluídas ---")
    print(f"Total de registros na Fila Mestre: {len(df)}")
    print(f"CSV 1 (Arquivos Baixados Limpos): {len(df_sucesso)} registros -> Salvo em: {caminho_sucesso.name}")
    print(f"CSV 2 (Pendentes/Erros Limpos): {len(df_falhas)} registros -> Salvo em: {caminho_falhas.name}")
    print("---------------------------------------")
    print("Sucesso! Os novos arquivos contém apenas os metadados ricos do negócio.")

if __name__ == "__main__":
    main()