# rode no terminal: python3 -m src.rag.evaluate_batch data/rag/results/"seu_arquivo".json

import argparse
import json
import os
from pathlib import Path

from src.rag.answer import call_llm 

EVALUATOR_PROMPT = """
Você é um avaliador estrito e imparcial de um sistema de Inteligência Artificial para a ANEEL.
Sua missão é avaliar a resposta gerada pelo sistema RAG com base na pergunta do usuário e nos documentos recuperados.

Você deve avaliar 2 critérios:
1. Fidelidade (Groundedness): A resposta é 100% baseada APENAS nos documentos fornecidos? (Se a IA inventou algo, a nota cai drasticamente).
2. Completude (Answer Relevance): A resposta respondeu exatamente o que foi perguntado de forma clara e sem enrolação?

Retorne SUA AVALIAÇÃO OBRIGATORIAMENTE EM FORMATO JSON com as chaves exatas abaixo:
{
  "nota_fidelidade": <int de 0 a 10>,
  "nota_completude": <int de 0 a 10>,
  "justificativa": "<uma frase curta explicando as notas>"
}

---
DADOS PARA AVALIAÇÃO:
Pergunta do Usuário: {question}

Resposta Gerada pelo RAG: {answer}

Documentos Recuperados (Contexto):
{context}
"""

def check_api_key(model_name: str) -> bool:
    model_lower = model_name.lower()
    if "gpt" in model_lower or "o1" in model_lower:
        return bool(os.getenv("OPENAI_API_KEY"))
    elif "claude" in model_lower:
        return bool(os.getenv("ANTHROPIC_API_KEY"))
    elif "gemini" in model_lower:
        return bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
    return False


def main():
    parser = argparse.ArgumentParser(description="Avalia resultados do RAG usando LLM-as-a-Judge.")
    parser.add_argument("results_file", type=str, help="Caminho para o JSON gerado pelo batch_answer.py")
    parser.add_argument("--model", type=str, default="gpt-4o", help="Modelo avaliador (use o melhor disponível)")
    args = parser.parse_args()

    input_path = Path(args.results_file)
    if not input_path.exists():
        print(f"Arquivo não encontrado: {input_path}")
        return

    with open(input_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    print(f"Iniciando avaliação de {len(report['results'])} respostas...")
    print("=" * 80)

    total_fidelidade = 0
    total_completude = 0
    avaliados = 0

    for idx, item in enumerate(report["results"], start=1):
        if item.get("llm_response") in [None, "Execução configurada para pular a geração de resposta (--no-llm)."]:
            print(f"[{idx}] Pulando pergunta sem resposta gerada.")
            continue

        print(f"Avaliando [{idx:02d}]: {item['question'][:60]}...")


        context_str = "\n".join([f"ID: {d['chunk_id']} | UID: {d['registro_uid']}" for d in item["retrieved_docs"]])
        
        prompt = EVALUATOR_PROMPT.format(
            question=item["question"],
            answer=item["llm_response"],
            context=context_str
        )

        try:
            raw_eval = call_llm(question="Avalie os dados abaixo.", context=prompt, model=args.model)
            
            clean_eval = raw_eval.replace("```json", "").replace("```", "").strip()
            eval_dict = json.loads(clean_eval)
            
            item["avaliacao_automatica"] = eval_dict
            
            total_fidelidade += eval_dict.get("nota_fidelidade", 0)
            total_completude += eval_dict.get("nota_completude", 0)
            avaliados += 1
            
        except Exception as e:
            print(f"Erro ao avaliar item {idx}: {e}")
            item["avaliacao_automatica"] = {"erro": "Falha na conversão da avaliação."}

    if avaliados > 0:
        report["metricas_finais"] = {
            "media_fidelidade": round(total_fidelidade / avaliados, 2),
            "media_completude": round(total_completude / avaliados, 2)
        }

    output_path = input_path.parent / f"evaluated_{input_path.name}"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("=" * 80)
    print(f"Avaliação concluída!")
    if avaliados > 0:
        print(f"Média de Fidelidade (Não alucinou): {report['metricas_finais']['media_fidelidade']}/10")
        print(f"Média de Completude (Respondeu bem): {report['metricas_finais']['media_completude']}/10")
    print(f"Arquivo final salvo em: {output_path}")

if __name__ == "__main__":
    main()