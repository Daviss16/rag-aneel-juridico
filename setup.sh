#!/bin/bash
set -e 

echo "Iniciando o setup automatizado do RAG ANEEL..."

echo "Criando ambiente virtual..."
python3 -m venv .venv

VENV_PIP="./.venv/bin/pip"
VENV_PYTHON="./.venv/bin/python"

echo "Instalando bibliotecas..."
$VENV_PIP install --upgrade pip
$VENV_PIP install -r requirements.txt

echo "Descompactando a base de dados preparada..."
unzip -o data/retrieval/retrieval_data.zip -d data/retrieval/

echo "Construindo o Banco SQLite..."
$VENV_PYTHON -m src.ingest.05a_import_to_sqlite

echo "Construindo o Índice BM25..."
$VENV_PYTHON -m src.ingest.05b_create_bm25_index

echo "Setup concluído com sucesso! A base de dados está indexada."
echo ""
echo "Para ativar o ambiente virtual:"
echo "source .venv/bin/activate"
echo ""
