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

A amostra inicial de 150 documentos foi incluída diretamente no repositório para garantir reprodutibilidade do experimento. O conjunto completo (~27 mil documentos) não será versionado em Git; em vez disso, o sistema faz a ingestão em lotes (`worker_gui_lotes.py`) e migra os artefatos baixados temporariamente para um Data Lake/Drive, esvaziando o armazenamento local do nó de execução.

- `amostra_pdfs_150.csv` → versão original  
- `amostra_pdfs_150_v2.csv` → versão com `registro_uid` único por PDF (`_pdfN`)

A v2 resolve casos onde um mesmo registro possui múltiplos PDFs.

## Validação de dados

Foi adicionado um util simples para comparar coleções de PDFs a partir dos arquivos .csv

## Estrutura Atual do Pipeline de Ingestão

### 1. Planejamento de Prioridades (`gerar_fila_prioridade.py`)
Responsável por:
- Ler os JSONs aninhados originais.
- "Desempacotar" anexos (criando IDs únicos `registro_uid_pdfN`).
- Aplicar score de prioridade baseado em tipo, sigla, ano e um bônus de diversidade.
- Gerar o CSV Mestre que atua como banco de dados de estado (`pendente`, `baixado_local`, `erro`).

### 2. O Worker de Aquisição (`worker_gui_lotes.py`)
Responsável por:
- Consumir o CSV Mestre pegando apenas lotes (ex: 100) não processados.
- Executar automação de GUI orientada por metadados para baixar sem acionar proteções anti-bot.
- Identificar e renomear o arquivo localmente com o seu `registro_uid` para evitar sobreescritas.
- Atualizar dinamicamente o status no CSV de controle.

### 3. Extração Limpa (`01_parse_documents.py` - Em progresso)
Responsável por converter os PDFs padronizados em dados legíveis por máquina, extraindo texto limpo para o arquivo `data/interim/parsed/parsed_documents.jsonl`.

### 4. Chunking e Enriquecimento (`02_create_chunks.py` - Em progresso)
Responsável por aplicar chunking com overlap e injetar o cabeçalho enriquecido com metadados no início de cada documento, garantindo o Data Lineage para o LLM.

## Decisões de engenharia já adotadas

- separação estrita entre extração, chunking e armazenamento vetorial.
- uso de arquivo intermediário JSONL para desacoplamento.
- uso de `pathlib.Path(__file__)` para tornar os scripts reproduzíveis em qualquer ambiente.
- tratamento do CSV como fonte da verdade estática e gerenciador de estado da fila.
- preparação da arquitetura para escala (~27k documentos) rodando de forma assíncrona ao desenvolvimento do modelo.
- desacoplamento entre o controle de estado dos downloads (Fila Mestre) e o armazenamento físico local.
- renomeação inteligente injetada via sufixo `_pdfN` para evitar sobreescrita de anexos processuais.
- adoção de automação GUI em vez de requests puras para bypass de restrições de infraestrutura do alvo (ex: Cloudflare).

## Estrutura do projeto

```text
rag-aneel/
├── data/
│   ├── raw/
│   │   ├── json/
│   │   ├── metadata/
│   │   ├── selected/
│   │   │   |── amostra_pdfs_150.csv
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
│   │       └── chunks.jsonl
│   └── logs/
├── src/
│   ├── sampling/
│   │   ├── selecionar_amostra_pdfs.py
│   │   └── gerar_fila_prioridade.py       
│   ├── downloads/
│   │   ├── baixar_pdfs_script_mouse_teclado.py
│   │   └── worker_gui_lotes.py            
│   ├── parsing/
│   │   └── 01_parse_documents.py
│   |── chunking/
│   |   └── 02_create_chunks.py
│   └── utils/
│       └── find_missing_pdfs.py
├── .gitignore
├── requirements.txt
└── README.md