# RAG Jurídico ANEEL

Projeto de desenvolvimento de um sistema RAG para perguntas e respostas sobre documentos jurídicos da ANEEL, com foco em benchmark e recuperação orientada por metadados + conteúdo textual de PDFs.

## Objetivo inicial
Construir uma pipeline reproduzível de:
- análise exploratória dos metadados
- seleção de amostra representativa de PDFs
- download e parsing
- chunking
- retrieval
- avaliação

## Estrutura
- `data/raw/json/`: arquivos JSON brutos
- `data/raw/metadata/`: CSVs normalizados dos registros
- `data/raw/selected/`: amostras selecionadas
- `data/raw/pdfs/`: PDFs baixados
- `src/sampling/`: scripts de seleção
- `src/download/`: scripts de download

## Status
Em andamento. Fase atual: seleção estratificada de 150 PDFs (50 por ano).