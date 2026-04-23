# RAG Jurídico ANEEL

Projeto de desenvolvimento de uma pipeline de RAG para perguntas e respostas sobre documentos jurídicos da ANEEL, com foco em benchmark, rastreabilidade, reprodutibilidade e escala de dados em arquitetura ETL.

## Objetivo

Construir uma base técnica reproduzível e resiliente para:

- análise exploratória dos metadados governamentais
- seleção e pontuação de amostras representativas (Priorização)
- aquisição automatizada dos documentos (bypassing de restrições de rede/Cloudflare)
- extração limpa de texto
- chunking enriquecido com metadados para futura indexação vetorial
- avaliação posterior em benchmark de Q/A cego

## Estratégia do projeto

Em vez de começar diretamente com todo o volume de dados, o projeto adota uma abordagem iterativa e modular:

1. entender os metadados e padrões do acervo
2. selecionar uma amostra controlada de documentos (Prova de Conceito - PoC)
3. validar a pipeline ponta a ponta nessa amostra
4. só depois escalar para o conjunto completo
5. implementar padrão de Fila de Tarefas com Estado (Stateful Queue) para download resiliente em lotes massivos, lidando com os ~27 mil documentos.

A amostra inicial (150 PDFs) foi pensada como ambiente de teste para maximizar o aprendizado sobre a qualidade dos documentos, limitações de aquisição e robustez da ingestão.

## Contexto dos dados

A base original contém aproximadamente 27 mil documentos descritos em arquivos JSON com metadados e links para PDFs (Resoluções, Despachos, Portarias, etc).

Iniciamos com o processamento local de uma amostra de ouro (150 documentos) e evoluímos a arquitetura para lidar com o acervo total. O sistema agora é alimentado por uma **Fila Mestre (`fila_downloads_mestre.csv`) que pontua, classifica e gerencia o estado de download de cada documento do acervo original.

## Nota sobre os dados

A amostra inicial de 150 documentos foi incluída diretamente no repositório para garantir reprodutibilidade do experimento. O conjunto completo (~27 mil documentos) não será versionado em Git; em vez disso, o sistema faz a ingestão em lotes (`download_gui_batches.py`) e migra os artefatos baixados temporariamente para um Data Lake/Drive, esvaziando o armazenamento local do nó de execução.

- `amostra_pdfs_150.csv` → versão original  
- `amostra_pdfs_150_v2.csv` → versão com `registro_uid` único por PDF (`_pdfN`)

A v2 resolve casos onde um mesmo registro possui múltiplos PDFs.

### Manifesto de Escala Unificado

Durante a transição da amostra inicial (150 documentos) para um corpus ampliado (>1000 documentos), foi introduzido um artefato adicional:

- `manifesto_3_unified_pdfs.csv`

Este arquivo representa a **base oficial da fase de escala**, sendo construído a partir de:

- um manifesto base de escala (~1000 PDFs)
- uma lista de documentos obrigatórios ausentes (`missing_pdfs.csv`)
- reconstrução completa das linhas a partir da `fila_downloads_mestre.csv`

O objetivo desse processo é garantir que:

- a amostra histórica de benchmark (150 PDFs) esteja totalmente contida no corpus ampliado
- o manifesto final seja consistente, limpo e baseado em uma única fonte da verdade

Esse arquivo passa a ser o ponto de entrada para as próximas etapas da pipeline em larga escala.

## Validação de dados

O projeto utiliza utilitários para garantir consistência entre os manifests e o estado real dos dados.

### Detecção de PDFs faltantes

- `find_missing_pdfs.py`
- Compara um manifesto com outro manifesto
- Gera:
  - `data/interim/download/missing_pdfs.csv`

### Consolidação de manifesto de escala

- `merge_required_pdfs_into_manifest.py`
- Responsável por gerar um manifesto unificado contendo:
  - o conjunto base de escala
  - os PDFs obrigatórios ausentes (ex: benchmark)

A lógica aplicada é:

1. unir os `registro_uid` do manifesto base e dos faltantes
2. reconstruir os registros completos a partir da `fila_downloads_mestre.csv`
3. remover colunas operacionais (seguindo padrão do `split_manifest.py`)
4. gerar um CSV final consistente e pronto para uso

Saída:

- `data/raw/selected/manifesto_3_unified_pdfs.csv`

Esse processo garante reprodutibilidade e consistência na fase de expansão do corpus.

## Estrutura Atual do Pipeline de Ingestão

### 1. Planejamento de Prioridades (`generate_priority_queue.py`)

Responsável por:
- Ler os JSONs aninhados originais.
- "Desempacotar" anexos (criando IDs únicos `registro_uid_pdfN`).
- Aplicar score de prioridade baseado em tipo, sigla, ano e um bônus de diversidade.
- Gerar o CSV Mestre que atua como banco de dados de estado (`pendente`, `baixado_local`, `erro`).

### 2. O Worker de Aquisição (`download_gui_batches.py`)

Responsável por:
- Consumir o CSV Mestre pegando apenas lotes (ex: 100) não processados.
- Executar automação de GUI orientada por metadados para baixar sem acionar proteções anti-bot.
- Identificar e renomear o arquivo localmente com o seu `registro_uid` para evitar sobreescritas.
- Atualizar dinamicamente o status no CSV de controle.

### 2.5 Consolidação do Manifesto de Escala (`merge_required_pdfs_into_manifest.py`)

Responsável por:

- garantir que o corpus ampliado contenha integralmente a amostra de benchmark
- unificar:
  - manifesto base (~1000 PDFs)
  - lista de PDFs obrigatórios ausentes (`missing_pdfs.csv`)
- reconstruir os registros a partir da `fila_downloads_mestre.csv`

Características:

- usa `registro_uid_pdfN` como identificador único
- evita inconsistências entre diferentes manifests
- gera um artefato final limpo e padronizado

Saída:

- `manifesto_3_unified_pdfs.csv`

Esse arquivo passa a ser a base oficial para a fase de ingestão em larga escala.

### 3. Extração Limpa (`01_extract_text.py`)

Responsável por converter os PDFs padronizados em texto bruto.

Estado atual:

- extração focada exclusivamente no conteúdo textual dos documentos
- saída minimalista contendo apenas:
  - `registro_uid`
  - `raw_text`
- uso de PyMuPDF e pdfplumber para garantir cobertura de texto e tabelas

#### Observação recente

A lógica de metadados foi completamente removida desta etapa.

Objetivo:

- reduzir custo computacional
- evitar reprocessamento pesado em mudanças futuras
- desacoplar extração de texto da lógica documental

O arquivo gerado (`parsed_documents.jsonl`) é leve, rápido de produzir e reutilizável.

#### Parsing em escala (batch processing)

Para suportar o corpus ampliado, foi implementado um mecanismo de parsing em lotes:

- processamento incremental (ex: 150 documentos por execução)
- controle de progresso via arquivo (`processed_uids.txt`)
- capacidade de retomada sem reprocessamento

Essa abordagem permite:

- processamento de grandes volumes com baixo consumo de memória
- execução resiliente a falhas
- escalabilidade para o acervo completo

### 3.1 Catálogo de Metadados (`02_build_metadata_catalog.py`)

Responsável por construir um índice documental enriquecido a partir dos JSONs originais da ANEEL.

Como funciona:

- percorre os JSONs brutos mantendo a mesma ordem de geração dos `registro_uid`
- replica metadados do registro original para cada PDF associado
- limpa campos textuais (ex: remoção de prefixos como "Assunto:" e ruídos como "Imprimir")

Saída:

- catálogo indexado por `registro_uid`
- contém:
  - título
  - autor
  - ementa
  - assunto
  - tipo do ato
  - demais campos relevantes

Benefícios:

- execução extremamente rápida (segundos)
- lookup O(1) durante o chunking
- evita reprocessamento dos PDFs ao alterar metadados

### 4. Chunking e Enriquecimento (`03_create_chunks.py`)

Responsável por transformar o texto bruto em unidades de contexto otimizadas para retrieval.

Estado atual:

- leitura do texto bruto extraído
- carregamento do catálogo de metadados em sqlite
- chunking com overlap
- respeito a limites de palavras, evitando corte no meio
- injeção de contexto documental no cabeçalho dos chunks

#### Enriquecimento de contexto

Cada chunk passa a receber um cabeçalho enriquecido com metadados do documento, incluindo:

- título
- autor
- ementa
- identificador do ato

Essa mudança foi introduzida para reduzir colisões de contexto entre documentos com trechos jurídicos muito similares.

#### Escolha do tamanho de chunk

Foram testados tamanhos de chunk de 1200, 2500 e 3000 caracteres.

O valor de **2500 caracteres** foi adotado como configuração principal por apresentar o melhor equilíbrio entre:

- manutenção do **Top-3 recall**
- redução drástica da quantidade total de chunks 
- maior coerência textual dentro de cada unidade recuperada

Essa decisão segue o objetivo principal do projeto nesta fase:

> maximizar a recuperação correta no **top-3**, que será repassado para a etapa posterior com LLM

### 5. Preparação do Corpus de Retrieval (`prepare_retrieval_corpus.py`)

Responsável por:

- transformar os chunks em um corpus otimizado para retrieval
- aplicar normalização de texto para melhorar busca lexical e semântica
- gerar estruturas auxiliares para indexação e avaliação

Outputs gerados:

- `data/retrieval/prepared/prepared_chunks.jsonl`
- `data/retrieval/indexes/chunk_id_to_row.json`
- `data/retrieval/indexes/doc_to_chunk_ids.json`
- `data/retrieval/indexes/corpus_stats.json`

Essa etapa desacopla completamente a ingestão da fase de retrieval, permitindo experimentação independente com diferentes estratégias (lexical, semântica e híbrida).

### 6. Baseline de Retrieval (`bm25_retriever.py`)

Foi implementado um baseline lexical utilizando BM25 sobre os chunks preparados.

Características:

- busca baseada em termos (lexical)
- ranking por similaridade textual
- recuperação feita no nível de chunk e avaliada no nível de documento (`registro_uid`)

Resultados no benchmark atual:

- Top-1 accuracy: **78,57%**
- Top-3 recall: **100%**

### Resultados adicionais em escala com chunks enriquecidos

Com o corpus ampliado e os chunks enriquecidos com ementa, o BM25 manteve desempenho forte em recuperação no top-3.

Resultados observados:

**Benchmark V1**
- Top-1 accuracy: **82,1%** 
- Top-3 recall: **92,8%** 

**Benchmark V2**
- Top-1 accuracy: **80%** 
- Top-3 recall: **93,3%** 

Observação:

- O BM25 apresentou melhora significativa no Benchmark V2, que utiliza termos mais alinhados ao conteúdo textual dos documentos.
- Isso reforça que o modelo é altamente eficaz em cenários com correspondência lexical forte.

O baseline já consegue recuperar todos os documentos esperados dentro do top-3, indicando boa qualidade do corpus e da pipeline.

### 7. Retrieval Semântico (`semantic_retriever.py`)

Foi implementado um segundo baseline de retrieval utilizando embeddings.

Características:

- modelo: `sentence-transformers/all-MiniLM-L6-v2`
- geração de embeddings para todos os chunks
- similaridade por cosine
- busca no nível de chunk e avaliação no nível de documento (`registro_uid`)

Resultados no benchmark atual:

- Top-1 accuracy: **32,14%**
- Top-3 recall: **60,71%**

Resultados adicionais (Benchmark V2) para mais de 1000 documentos:

- Top-1 accuracy: **17,8%**
- Top-3 recall: **25%**

Comparado ao BM25, o método semântico apresentou queda significativa de desempenho, principalmente em perguntas com:

- números (tabelas, percentuais)
- entidades específicas
- linguagem regulatória repetitiva

Isso indica que embeddings genéricos não são suficientes para esse domínio e reforça a necessidade de uma abordagem híbrida.

### 8. Retrieval Híbrido (`hybrid_retriever.py`)

Foi implementado um terceiro baseline combinando BM25 e embeddings.

Estratégia:

- BM25 utilizado para recuperação de candidatos
- embeddings usados apenas para reordenar (reranking)
- combinação de scores normalizados (bm25 + semântico)

Características:

- preserva o alto recall do BM25
- melhora o ranking em alguns casos específicos
- evita perda de desempenho observada no semântico puro

A implementação também expõe os seguintes scores para análise:

- score_bm25
- score_semantic
- score_final

Essa abordagem equilibra precisão lexical com similaridade semântica.

#### Resultados adicionais em escala com chunks enriquecidos

**Benchmark V1**
- Top-1 accuracy: **78,5%**
- Top-3 recall: **92,8%**

**Benchmark V2**
- Top-1 accuracy: **83,3%** 
- Top-3 recall: **93,3%** 

Observação:

- O modelo híbrido apresentou melhora com o enriquecimento por ementa, mas o principal critério de decisão permaneceu o **Top-3 recall**.
- Como o recall se manteve estável com chunk 2500 e o banco foi drasticamente reduzido, essa configuração foi mantida como padrão.


## Decisões de engenharia já adotadas

- adoção de automação GUI em vez de requests puras para bypass de restrições de infraestrutura do alvo (ex: Cloudflare).
- separação estrita entre extração, chunking e armazenamento vetorial.
- uso de arquivo intermediário JSONL para desacoplamento.
- uso de `pathlib.Path(__file__)` para tornar os scripts reproduzíveis em qualquer ambiente.
- tratamento do CSV como fonte da verdade estática e gerenciador de estado da fila.
- preparação da arquitetura para escala (~27k documentos) rodando de forma assíncrona ao desenvolvimento do modelo.
- desacoplamento entre o controle de estado dos downloads (Fila Mestre) e o armazenamento físico local.
- renomeação inteligente injetada via sufixo `_pdfN` para evitar sobreescrita de anexos processuais.
- separação explícita entre ingestão e retrieval, permitindo experimentação controlada sobre o corpus.
- avaliação baseada em benchmark estruturado com métricas de recuperação (top-1 e top-3).
- todo manifesto utilizado em experimentos de escala deve conter integralmente a amostra histórica de benchmark.
- validação do sistema em escala (>1000 documentos) com comparação entre benchmarks V1 e V2
- adicionar ementas e autores ao cabeçalho dos chunks, para garantir que identificadores únicos estejam presentes em todos os chunks
- desacoplamento completo entre extração de texto e metadados documentais
- introdução de catálogo de metadados indexado por `registro_uid`
- enriquecimento de chunks com contexto documental (ementa, autor, título)
- aumento do chunk size para melhorar coerência contextual
- tratamento do problema de colisão de contexto como principal gargalo do retrieval
- adoção de chunk size 2500 como melhor equilíbrio entre recall e custo operacional

### Refatorações na Camada de Retrieval

Foram realizadas melhorias estruturais para suportar escalabilidade e reutilização:

- centralização do schema e loader de chunks em `schemas.py`
- remoção de lógica duplicada de leitura de dados nos retrievers
- compartilhamento de dados em memória entre BM25, semântico e híbrido
- criação de `evaluation_utils.py` para unificar a lógica de avaliação
- padronização dos evaluators como camadas finas de orquestração

Essas mudanças reduziram duplicação, melhoraram a eficiência de memória e facilitaram a evolução do sistema.

## Estrutura do projeto

```text
rag-aneel/
├── data/
│   ├── benchmark/
│   │   |── benchmark_questions.json
│   │   └── benchmark_questions_v2.json  
│   ├── raw/
│   │   ├── json/
│   │   ├── metadata/
│   │   ├── selected/
│   │   │   |── manifesto_3_unified_pdfs.csv
│   │   |   └── fila_downloads_mestre.csv      
│   │   └── documents/
│   │       ├── temp/
│   │       │   └── lotes_baixados/       
│   │       └── downloads/                
│   │           ├── 2016/
│   │           ├── 2021/
│   │           └── 2022/
│   ├── interim/
│   │   ├── resolved/
│   │   |── parsed/
│   │   |   └── parsed_documents.jsonl
|   |   └── download/
│   │       └── missing_pdfs.csv
│   ├── processed/
│   │   └── chunks/
│   ├── retrieval/
│   │   ├── prepared/
│   │   ├── indexes/
│   │   └── evaluation/
│   └── logs/
├── src/
│   ├── sampling/
│   │   ├── select_pdf_sample.py
│   │   └── generate_priority_queue.py       
│   ├── downloads/
│   │   ├── download_gui_150pdf.py
│   │   └── download_gui_batches.py            
│   ├── ingest/
│   │   └── 01_parse_documents.py
│   |   └── 02_create_chunks.py
│   ├── retrieval/
│   │   ├── evaluations/
│   │   |   |── evaluate_bm25.py
│   │   │   |── evaluate_hybrid.py
│   │   │   |── evaluate_semantic.py
│   │   |   └── evaluate_utils.py   
│   │   ├── prepare_retrieval_corpus.py
│   │   ├── text_normalization.py
│   │   ├── data_loader.py
│   │   ├── schemas.py
│   |   ├── hybrid_retriever.py
│   │   ├── bm25_retriever.py
│   |   └── semantic_retriever.py
│   └── utils/
│       ├── split_manifest.py
│       ├── merge_required_pdfs_into_manifest.py
│       └── find_missing_pdfs.py
├── .gitignore
├── requirements.txt
└── README.md