#rodar no terminal: python3 -m src.retrieval.evaluate_bm25

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from src.retrieval.bm25_retriever import build_bm25_retriever

@dataclass(frozen=True)
class EvaluateBM25Config:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    benchmark_path: Path = base_dir / "data" / "benchmark" / "benchmark_questions.json"
    output_path: Path = base_dir / "data" / "retrieval" / "evaluation" / "bm25_metrics.json"

def load_benchmark(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("O benchmark deve ser uma lista de perguntas.")

    items = []
    for idx, item in enumerate(data, start=1):
        question = (item.get("question") or "").strip()
        expected_doc = (item.get("expected_doc") or "").strip()

        if not question:
            raise ValueError(f"Item {idx}: campo 'question' ausente.")
        if not expected_doc:
            raise ValueError(f"Item {idx}: campo 'expected_doc' ausente.")

        items.append(item)

    return items

def unique_docs_from_results(results: list[dict], top_k_docs: int) -> list[str]:
    docs = []
    seen = set()

    for result in results:
        registro_uid = result["registro_uid"]
        if registro_uid in seen:
            continue

        seen.add(registro_uid)
        docs.append(registro_uid)

        if len(docs) >= top_k_docs:
            break

    return docs

def evaluate_question(retriever, question: str, expected_doc: str, search_k: int = 10) -> dict:
    chunk_results = retriever.search(question, top_k=search_k)
    ranked_docs = unique_docs_from_results(chunk_results, top_k_docs=3)

    top_1_hit = bool(ranked_docs) and ranked_docs[0] == expected_doc
    top_3_hit = expected_doc in ranked_docs

    return {
        "question": question,
        "expected_doc": expected_doc,
        "retrieved_docs": ranked_docs,
        "top_1_hit": top_1_hit,
        "top_3_hit": top_3_hit,
    }

def evaluate_benchmark(benchmark: list[dict]) -> dict:
    retriever = build_bm25_retriever()

    per_question = []
    top_1_hits = 0
    top_3_hits = 0

    for item in benchmark:
        result = evaluate_question(
            retriever=retriever,
            question=item["question"],
            expected_doc=item["expected_doc"],
            search_k=10,
        )

        per_question.append(result)

        if result["top_1_hit"]:
            top_1_hits += 1
        if result["top_3_hit"]:
            top_3_hits += 1

    total = len(benchmark)

    return {
        "total_questions": total,
        "top_1_hits": top_1_hits,
        "top_3_hits": top_3_hits,
        "top_1_accuracy": top_1_hits / total if total else 0.0,
        "top_3_recall": top_3_hits / total if total else 0.0,
        "results": per_question,
    }

def save_metrics(data: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    config = EvaluateBM25Config()

    benchmark_path = Path(args.benchmark).resolve() if args.benchmark else config.benchmark_path
    output_path = Path(args.output).resolve() if args.output else config.output_path

    benchmark = load_benchmark(benchmark_path)
    metrics = evaluate_benchmark(benchmark)
    save_metrics(metrics, output_path)

    print(f"Total de perguntas: {metrics['total_questions']}")
    print(f"Top-1 hits: {metrics['top_1_hits']}")
    print(f"Top-3 hits: {metrics['top_3_hits']}")
    print(f"Top-1 accuracy: {metrics['top_1_accuracy']:.4f}")
    print(f"Top-3 recall: {metrics['top_3_recall']:.4f}")
    print(f"Output salvo em: {output_path}")

if __name__ == "__main__":
    main()