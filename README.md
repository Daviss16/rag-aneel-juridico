# RAG ANEEL - Instruções de Execução

## 1. Setup

Clone o repositório e rode na raiz do projeto:

chmod +x setup.sh && ./setup.sh^C

Isso irá:
- criar o ambiente virtual
- instalar dependências
- descompactar os dados
- construir o banco e índice BM25

---

## 2. Configurar LLM

Copie o arquivo de exemplo:

cp .env.example .env

O sistema suporta múltiplos provedores de LLM.

Basta configurar uma das seguintes variáveis no arquivo .env:

- OPENAI_API_KEY
- ANTHROPIC_API_KEY
- GEMINI_API_KEY

O sistema detecta automaticamente qual chave está disponível.

Sem a chave, o sistema roda normalmente (modo fallback).

---

## 3. Testar o sistema

### Pergunta única:

python3 -m src.rag.answer "sua pergunta"

### Sem LLM:

python3 -m src.rag.answer "sua pergunta" --no-llm

---

## 4. Rodar perguntas em lote

python3 -m src.rag.batch_answer data/perguntas.txt

Pra adicionar as perguntas do Benchmark, o codigo le uma pergunta por linha

---

## 5. Avaliação automática

python3 -m src.rag.evaluate_batch data/rag/results/<arquivo>.json

Adiciona apenas como uma alternativa de avaliação das respostas

---

## Observações

- O sistema utiliza BM25 como mecanismo principal de recuperação
- A LLM é opcional e utilizada apenas para geração de respostas
- O sistema foi testado com dataset completo (8,3k documentos + Ementa e metadados dos documentos restantes)