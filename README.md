# RAG Jurídico ANEEL

Projeto de desenvolvimento de uma pipeline de RAG para perguntas e respostas sobre documentos jurídicos da ANEEL, com foco em benchmark, rastreabilidade e reprodutibilidade.

## Objetivo

Construir uma base técnica reproduzível para:

- análise exploratória dos metadados
- seleção de amostra representativa
- aquisição dos documentos
- extração de texto
- chunking para futura indexação vetorial
- avaliação posterior em benchmark de Q/A

## Estratégia do projeto

Em vez de começar diretamente com todo o volume de dados, o projeto adota uma abordagem incremental:

1. entender os metadados e padrões do acervo
2. selecionar uma amostra controlada de documentos
3. validar a pipeline ponta a ponta nessa amostra
4. só depois escalar para o conjunto completo

A amostra atual foi pensada como ambiente de teste para maximizar aprendizado sobre:

- qualidade dos documentos
- limitações de aquisição
- robustez da ingestão
- impacto futuro nas métricas de Q/A

## Contexto dos dados

A base original contém aproximadamente **27 mil documentos** descritos em arquivos JSON com metadados e links para PDFs.

Até o momento, o projeto trabalha sobre uma **amostra de 150 documentos**, selecionada a partir dos metadados e organizada para permitir desenvolvimento iterativo da pipeline.

## Nota sobre os dados

A amostra de 150 documentos foi incluída diretamente no repositório para garantir reprodutibilidade do experimento.

O conjunto completo (~27 mil documentos) não será versionado devido ao volume, sendo tratado separadamente na etapa de escalabilidade.

## Princípio arquitetural central

O **manifesto CSV é a fonte da verdade**.

Isso significa que:

- nomes de arquivos locais não são usados como fonte de metadado
- toda associação entre documento e registro deve partir do manifesto
- os arquivos locais são tratados apenas como blobs de conteúdo

## Arquitetura atual da ingestão

A antiga abordagem monolítica foi substituída por uma pipeline em dois estágios:

### 1. Extração pesada (`01_extract_text.py`)
Responsável por:

- ler o manifesto CSV
- localizar os documentos baixados
- abrir os PDFs
- extrair texto com PyMuPDF (`fitz`)
- usar `pdfplumber` como apoio/fallback quando necessário
- converter tabelas detectadas para Markdown
- salvar o texto integral em um arquivo intermediário

Saída:

- `data/interim/parsed/parsed_documents.jsonl`

### 2. Chunking leve (`02_create_chunks.py`)
Responsável por:

- ler o JSONL intermediário
- injetar cabeçalho enriquecido com metadados
- aplicar chunking com overlap
- gerar o JSONL final pronto para etapa futura de embeddings/indexação

Saída:

- `data/processed/chunks/chunks.jsonl`

## Decisões de engenharia já adotadas

- separação entre extração e chunking
- uso de arquivo intermediário JSONL para desacoplamento
- uso de `pathlib.Path(__file__)` para tornar os scripts reproduzíveis em qualquer ambiente
- tratamento do CSV como fonte da verdade
- preparação da arquitetura para futura escala (~27k documentos)

## Estrutura do projeto

```text
rag-aneel/
├── data/
│   ├── raw/
│   │   ├── json/
│   │   ├── metadata/
│   │   ├── selected/
│   │   │   └── amostra_pdfs_150.csv
│   │   └── documents/
│   │       └── downloads/
│   │           ├── 2016/
│   │           ├── 2021/
│   │           └── 2022/
│   ├── interim/
│   │   ├── resolved/
│   │   └── parsed/
│   │       └── parsed_documents.jsonl
│   ├── processed/
│   │   └── chunks/
│   │       └── chunks.jsonl
│   └── logs/
├── src/
│   ├── sampling/
│   │   └── selecionar_amostra_pdfs.py
│   ├── resolver/
│   │   └── resolver_fontes_alternativas.py
│   ├── downloads/
│   │   └── baixar_pdfs_GUI.py
│   ├── ingest/
│   │   ├── 01_extract_text.py
│   │   └── 02_create_chunks.py
│   └── retrieval/
├── docs/
├── notebooks/
├── archive/
│   └── deprecated/
├── README.md
└── .gitignore