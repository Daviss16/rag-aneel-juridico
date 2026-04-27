# RAG ANEEL - Instruções de Execução

## 1. Setup

Clone o repositório e rode na raiz do projeto:

chmod +x setup.sh && ./setup.sh

Isso irá:
- criar o ambiente virtual
- instalar dependências
- descompactar os dados
- construir o banco e índice BM25

---

## 2. Configurar LLM

Copie o arquivo de exemplo:

cp .env.example .env

O sistema suporta múltiplos provedores de LLM (OpenAI, Anthropic e Gemini).

1. Basta configurar a chave de sua preferência no arquivo `.env`:
   - `OPENAI_API_KEY="sua_chave"`
   - `ANTHROPIC_API_KEY="sua_chave"`
   - `GEMINI_API_KEY="sua_chave"`

2. Ao rodar o comando, indique qual modelo você quer usar:
   `python3 -m src.rag.answer_batches data/questions/perguntas.txt --model gemini-1.5-flash-latest`

Sem a chave, o sistema roda normalmente exibindo apenas os documentos recuperados (modo fallback).

---

## 3. Testar o sistema

### Pergunta única:

python3 -m src.rag.answer "sua pergunta" --model

### Sem LLM:

python3 -m src.rag.answer "sua pergunta" --no-llm

---

## 4. Rodar perguntas em lote

python3 -m src.rag.answer_batches data/questions/perguntas.txt --model

Pra adicionar as perguntas do Benchmark, o codigo le uma pergunta por linha

---

## 5. Avaliação automática

python3 -m src.rag.evaluate_batch data/rag/results/<arquivo>.json --model

Adiciona apenas como uma alternativa de avaliação das respostas

---

## Observações

- O sistema utiliza BM25 como mecanismo principal de recuperação
- A LLM é opcional e utilizada apenas para geração de respostas
- O sistema foi testado com dataset completo (8,3k documentos + Ementa e metadados dos documentos restantes)