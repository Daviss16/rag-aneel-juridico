#rode no terminal: python3 -m src.rag.answer_batches data/questions/perguntas.txt

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from src.retrieval.bm25_retriever import build_bm25_retriever
from src.rag.answer import format_sources, call_llm


def load_questions(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    questions: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            questions.append(line)

    return questions

def check_api_key(model_name: str) -> bool:
    model_lower = model_name.lower()
    if "gpt" in model_lower or "o1" in model_lower:
        return bool(os.getenv("OPENAI_API_KEY"))
    elif "claude" in model_lower:
        return bool(os.getenv("ANTHROPIC_API_KEY"))
    elif "gemini" in model_lower:
        return bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
    return False

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Executa perguntas em lote no RAG ANEEL e exporta para JSON."
    )
    parser.add_argument("questions_file", type=str, help="Arquivo .txt com uma pergunta por linha.")
    parser.add_argument("--top-k", type=int, default=3, help="Quantidade de chunks recuperados.")
    parser.add_argument("--no-llm", action="store_true", help="Executa apenas retrieval, sem chamar LLM.")
    parser.add_argument("--model", type=str, default="gpt-5.5", help="Modelo da LLM.")

    args = parser.parse_args()

    questions = load_questions(Path(args.questions_file))
    use_llm = not args.no_llm and check_api_key(args.model)

    if not args.no_llm and not use_llm:
        print(f"AVISO: Nenhuma chave API encontrada para o modelo '{args.model}'.")
        print("O sistema fará o fallback e recuperará apenas os documentos.")

    print(f"Iniciando processamento em lote...")
    print(f"Total de perguntas: {len(questions)}")
    print(f"LLM ativada: {use_llm}")
    print("=" * 80)

    print("Carregando banco de dados (SQLite e BM25)...")
    retriever = build_bm25_retriever()
    
    report = {
        "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "config": {
            "top_k": args.top_k,
            "model": args.model,
            "use_llm": use_llm,
            "total_questions": len(questions)
        },
        "results": []
    }

    for idx, question in enumerate(questions, start=1):
        print(f"Processando [{idx:02d}/{len(questions):02d}]: {question[:60]}...")
        
        docs = retriever.search(question, top_k=args.top_k)
        
        docs_info = []
        for d in docs:
            docs_info.append({
                "chunk_id": d.get("chunk_id"),
                "registro_uid": d.get("registro_uid"),
                "score_bm25": d.get("score_final", d.get("score", 0.0)),
                "text_full": d.get("text_full", "")
            })

        llm_response = None
        if use_llm and docs:
            context = format_sources(docs)
            llm_response = call_llm(question=question, context=context, model=args.model)
        elif not docs:
            llm_response = "Nenhum documento recuperado para basear a resposta."
        else:
            llm_response = "Execução configurada para pular a geração de resposta (--no-llm)."

        report["results"].append({
            "question": question,
            "retrieved_docs": docs_info,
            "llm_response": llm_response
        })

    output_dir = Path(__file__).resolve().parent.parent.parent / "data" / "rag" / "results"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = output_dir / f"batch_answers_{timestamp}.json"

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("=" * 80)
    print(f"Lote finalizado com sucesso!")
    print(f"Arquivo salvo em: {out_file}")


if __name__ == "__main__":
    main()