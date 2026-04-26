"""

python3 -m src.rag.answer <question>
#rode no terminal com chave API para integrar com LLM e gerar a resposta


python3 -m src.rag.answer <question> --no-llm
#rode isso no terminal para mostrar apenas os documentos recuperados | "fallback"

python3 -m src.rag.answer "Na Resolução Homologatória nº 2865/2021, qual foi o efeito tarifário médio homologado para a Ceres?" --no-llm
python3 -m src.rag.answer "Qual era a capacidade instalada da unidade geradora liberada pelo Despacho nº 1.978/2016?" --no-llm
python3 -m src.rag.answer "Onde a resolução define os percentuais de descontos relativos aos benefícios tarifários incidentes sobre as tarifas de aplicação da Ceres?" --no-llm
#exemplos de perguntas para testar, sem integração com LLM

"""


from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from dotenv import load_dotenv


load_dotenv()

from src.retrieval.bm25_retriever import build_bm25_retriever


@dataclass(frozen=True)
class AnswerConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    top_k: int = 3
    model: str = "gpt-4o" 


def _get_field(result: Any, field: str, default: Any = None) -> Any:
    if isinstance(result, dict):
        return result.get(field, default)
    return getattr(result, field, default)


def _truncate(text: str, limit: int = 1800) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[:limit].rstrip() + "..."


def format_sources(results: list[Any]) -> str:
    blocks: list[str] = []

    for i, result in enumerate(results, start=1):
        chunk_id = _get_field(result, "chunk_id", "")
        registro_uid = _get_field(result, "registro_uid", "")
        text_full = _get_field(result, "text_full", "")

        block = f"[DOCUMENTO {i}]\n"
        block += f"ID: {chunk_id} | Registro: {registro_uid}\n"
        block += f"Conteúdo Extraído:\n{text_full}\n"
        
        blocks.append(block)

    return "\n---\n".join(blocks)


def build_prompt(question: str, context: str) -> str:
    return f"""
Você é um assistente especializado em documentos regulatórios da ANEEL.

Responda à pergunta usando EXCLUSIVAMENTE o contexto fornecido.

Regras Gerais:
- Não use conhecimento externo.
- Se a resposta não estiver no contexto, diga: "Não encontrei essa informação nos documentos recuperados."
- Seja direto, claro e analítico.
- Cite o documento usado pelo Título ou pelo Registro UID (ex: "Segundo a REN 703/2016 (Registro: 2016_03243_pdf3)...").
- Não invente valores, datas, nomes ou fundamentos.

Regras Críticas de Interpretação:
1. DOCUMENTOS REVOGADOS: Se o contexto contiver a tag "[ALERTA: DOCUMENTO REVOGADO...]", você pode usá-lo para responder a perguntas históricas sobre o que a norma dizia na época. Porém, você OBRIGATORIAMENTE deve adicionar uma nota ao final da resposta informando que a regra mencionada encontra-se revogada e não é mais vigente.
2. LIMITAÇÃO DE EMENTA: Se o contexto contiver a tag "[AVISO: O texto completo deste documento não está disponível...]", responda baseando-se no que for possível extrair da Ementa. Se a pergunta exigir detalhes profundos do "miolo" do texto que não estão na Ementa, informe ao usuário: "Nota: Não é possível detalhar mais a fundo pois apenas a ementa deste documento está disponível na base de dados."

Contexto:
{context}

Pergunta:
{question}

Resposta:
""".strip()


def resolve_llm_provider(requested_model: str) -> tuple[str, str, str]:
    """Avalia as chaves do .env e define o provedor, o modelo e a chave API adequados."""
    model_lower = requested_model.lower()
    
    if ("gpt" in model_lower or "o1" in model_lower) and os.getenv("OPENAI_API_KEY"):
        return "openai", requested_model, os.environ["OPENAI_API_KEY"]
    if "claude" in model_lower and os.getenv("ANTHROPIC_API_KEY"):
        return "anthropic", requested_model, os.environ["ANTHROPIC_API_KEY"]
    if "gemini" in model_lower and (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")):
        key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        return "gemini", requested_model, key
        
    if os.getenv("OPENAI_API_KEY"):
        return "openai", "gpt-4o", os.environ["OPENAI_API_KEY"]
    if os.getenv("ANTHROPIC_API_KEY"):
        return "anthropic", "claude-3-5-sonnet-20240620", os.environ["ANTHROPIC_API_KEY"]
    if os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"):
        key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        return "gemini", "gemini-1.5-flash", key
        
    return None, None, None


def call_llm(question: str, context: str, model: str) -> str:
    provider, resolved_model, api_key = resolve_llm_provider(model)
    
    if not provider:
        return "[ERRO]: Nenhuma chave de API (OpenAI, Anthropic ou Gemini) foi encontrada no seu .env."
        
    prompt = build_prompt(question, context)
    
    try:
        if provider == "openai":
            import openai
            client = openai.OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model=resolved_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0
            )
            return response.choices[0].message.content.strip()
            
        elif provider == "anthropic":
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model=resolved_model,
                max_tokens=1024,
                temperature=0.0,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
            
        elif provider == "gemini":
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            generative_model = genai.GenerativeModel(resolved_model)
            response = generative_model.generate_content(
                prompt, 
                generation_config={"temperature": 0.0}
            )
            return response.text.strip()
            
    except Exception as e:
        return f"[ERRO API {provider.upper()}]: Falha ao gerar resposta com o modelo '{resolved_model}'. Detalhe: {e}"


def answer(question: str, top_k: int, model: str, use_llm: bool) -> None:
    retriever = build_bm25_retriever()
    results = retriever.search(question, top_k=top_k)

    if not results:
        print("Nenhum documento recuperado.")
        return

    context = format_sources(results)

    print("\n=== DOCUMENTOS RECUPERADOS ===\n")
    print(context)

    provider, _, _ = resolve_llm_provider(model)
    if not use_llm or not provider:
        if not provider and use_llm:
            print("\n[AVISO]: Nenhuma chave API detectada no .env. Executando fallback (apenas recuperação).")
        return

    print("\n=== RESPOSTA DA LLM ===\n")
    print(call_llm(question=question, context=context, model=model))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa RAG simples com BM25 Top-K e LLM opcional."
    )
    parser.add_argument("question", type=str, help="Pergunta a ser respondida.")
    parser.add_argument("--top-k", type=int, default=AnswerConfig.top_k, help="Número de chunks recuperados.")
    parser.add_argument("--model", type=str, default=AnswerConfig.model, help="Modelo da LLM.")
    parser.add_argument("--no-llm", action="store_true", help="Executa apenas retrieval, sem chamar LLM.")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    answer(
        question=args.question,
        top_k=args.top_k,
        model=args.model,
        use_llm=not args.no_llm,
    )


if __name__ == "__main__":
    main()
