# rode no terminal: python3 -m src.utils.merge_required_pdfs_into_manifest <manifesto_base.csv> <missing_pdfs.csv>

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# python3 -m src.utils.merge_required_pdfs_into_manifest data/raw/selected/manifesto_1_sucesso_pdfs.csv data/interim/download/missing_pdfs.csv

import pandas as pd
from pathlib import Path
import sys


def main():
    if len(sys.argv) != 3:
        print("Uso: python3 -m src.utils.merge_required_pdfs_into_manifest <manifesto_base.csv> <missing_pdfs.csv>")
        sys.exit(1)

    base_dir = Path(__file__).resolve().parents[2]

    caminho_manifesto_base = base_dir / sys.argv[1]
    caminho_missing = base_dir / sys.argv[2]
    caminho_mestre = base_dir / "data" / "raw" / "selected" / "fila_downloads_mestre.csv"
    caminho_saida = base_dir / "data" / "raw" / "selected" / "manifesto_3_unified_pdfs.csv"

    if not caminho_manifesto_base.exists():
        print(f"Erro: arquivo não encontrado: {caminho_manifesto_base}")
        sys.exit(1)

    if not caminho_missing.exists():
        print(f"Erro: arquivo não encontrado: {caminho_missing}")
        sys.exit(1)

    if not caminho_mestre.exists():
        print(f"Erro: arquivo não encontrado: {caminho_mestre}")
        sys.exit(1)

    df_base = pd.read_csv(caminho_manifesto_base)
    df_missing = pd.read_csv(caminho_missing)
    df_mestre = pd.read_csv(caminho_mestre)

    if "registro_uid" not in df_base.columns:
        print("Erro: o manifesto base não contém a coluna 'registro_uid'.")
        sys.exit(1)

    if "registro_uid" not in df_missing.columns:
        print("Erro: o arquivo missing não contém a coluna 'registro_uid'.")
        sys.exit(1)

    if "registro_uid" not in df_mestre.columns:
        print("Erro: a fila mestre não contém a coluna 'registro_uid'.")
        sys.exit(1)

    base_ids = set(df_base["registro_uid"].dropna().astype(str).str.strip())
    missing_ids = set(df_missing["registro_uid"].dropna().astype(str).str.strip())

    final_ids = base_ids | missing_ids

    df_final = df_mestre[df_mestre["registro_uid"].astype(str).str.strip().isin(final_ids)].copy()

    colunas_para_remover = [
        "url",
        "arquivo",
        "baixado_json",
        "cat_key",
        "is_first_of_kind",
        "score_prioridade",
        "status_processamento",
        "tentativas",
        "mensagem_erro",
    ]

    df_final = df_final.drop(columns=colunas_para_remover, errors="ignore")

    df_final.to_csv(caminho_saida, index=False)

    print("OK")


if __name__ == "__main__":
    main()