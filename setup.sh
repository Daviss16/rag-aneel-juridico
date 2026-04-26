#!/bin/bash
set -e 

echo "Iniciando o setup automatizado do RAG ANEEL..."

echo "Criando ambiente virtual..."
python3 -m venv .venv
source .venv/bin/activate


echo "Instalando bibliotecas..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Descompactando a base de dados preparada..."
unzip -o data/retrieval_data.zip -d data/retrieval/

echo "Construindo o Banco SQLite..."
python3 -m src.ingest.05a_import_to_sqlite

echo "Construindo o Índice BM25..."
python3 -m src.ingest.05b_create_bm25_index

echo "Setup concluído com sucesso! A base de dados está indexada."
echo "Para testar o lote de perguntas, rode: python3 -m src.rag.answer_batches data/questions/perguntas.txt"