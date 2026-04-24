#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

def main():
    # Caminhos dos arquivos (ajuste conforme a sua estrutura)
    raiz_projeto = Path(__file__).resolve().parents[2]
    caminho_novo_csv = raiz_projeto / "data" / "raw" / "selected" / "fila_downloads_mestre.csv"
    pasta_pdfs = raiz_projeto / "data" / "raw" / "documents" / "temp" / "lotes_baixados"

    if not caminho_novo_csv.exists():
        print(f"❌ Erro: CSV Mestre não encontrado em {caminho_novo_csv}")
        return

    if not pasta_pdfs.exists():
        print(f"❌ Erro: Pasta de PDFs não encontrada em {pasta_pdfs}")
        return

    print("📖 Carregando o manifesto Mestre v2...")
    df_novo = pd.read_csv(caminho_novo_csv)

    # 1. Extraímos os UIDs direto dos nomes dos arquivos físicos no disco
    print("🔍 Lendo os arquivos físicos na pasta local...")
    
    # O .stem pega o nome do arquivo ignorando se é .pdf, .html, etc.
    # Usamos set() (chaves {}) para a busca ficar super rápida (O(1))
    uids_no_disco = {arquivo.stem for arquivo in pasta_pdfs.iterdir() if arquivo.is_file()}
    
    print(f"   -> Encontrados {len(uids_no_disco)} arquivos válidos na pasta.")

    if len(uids_no_disco) == 0:
        print("Nenhum arquivo encontrado para reconciliar. Encerrando.")
        return

    # 2. Atualizamos o novo CSV cruzando os dados
    print("✍️ Atualizando o manifesto de 27k linhas...")
    
    # Marcamos no DataFrame quem tem o registro_uid igual ao nome de um arquivo na pasta
    mascara_reconciliacao = df_novo['registro_uid'].isin(uids_no_disco)
    df_novo.loc[mascara_reconciliacao, 'status_processamento'] = 'baixado_local'

    # 3. Salvaguarda: Verificar se a quantidade bate
    atualizados = df_novo[df_novo['status_processamento'] == 'baixado_local'].shape[0]
    
    # Salva o resultado final sobrescrevendo o arquivo
    df_novo.to_csv(caminho_novo_csv, index=False, encoding="utf-8")

    print("\n" + "="*50)
    print("✅ RECONCILIAÇÃO FÍSICA CONCLUÍDA")
    print(f"   Total de arquivos lidos na pasta: {len(uids_no_disco)}")
    print(f"   Total de linhas atualizadas no CSV para 'baixado_local': {atualizados}")
    
    if len(uids_no_disco) != atualizados:
        print("\n⚠️ AVISO: A quantidade de arquivos na pasta difere dos atualizados no CSV.")
        print("Isso pode ocorrer se houver arquivos na pasta com nomes que não estão no CSV.")
        
    print("="*50 + "\n")

if __name__ == "__main__":
    main()