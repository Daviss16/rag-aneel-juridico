#rode no terminal: python3 -m src.retrieval.evaluate_semantic

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from src.retrieval.semantic_retriever import build_semantic_retriever


@dataclass(frozen=True)
class EvaluateSemanticConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    benchmark_path: Path = base_dir / "data" / "benchmark" / "benchmark_questions.json"
    output_path: Path = base_dir / "data" / "retrieval" / "evaluation" / "semantic_metrics.json"


def load_benchmark(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    items = []
    for item in data:
        question = (item.get("question") or "").strip()
        expected_doc = (item.get("expected_doc") or "").strip()

        if not question or not expected_doc:
            continue

        items.append(item)

    return items

def unique_docs_from_results(results: list[dict], top_k_docs: int) -> list[str]:
    docs = []
    seen = set()

    for result in results:
        doc = result["registro_uid"]
        if doc in seen:
            continue

        seen.add(doc)
        docs.append(doc)

        if len(docs) >= top_k_docs:
            break

    return docs

def evaluate_question(retriever, question: str, expected_doc: str) -> dict:
    chunk_results = retriever.search(question, top_k=10)
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
    retriever = build_semantic_retriever()

    total = len(benchmark)
    top_1_hits = 0
    top_3_hits = 0

    results = []

    for item in benchmark:
        result = evaluate_question(
            retriever,
            item["question"],
            item["expected_doc"]
        )

        results.append(result)

        if result["top_1_hit"]:
            top_1_hits += 1

        if result["top_3_hit"]:
            top_3_hits += 1

    return {
        "total_questions": total,
        "top_1_hits": top_1_hits,
        "top_3_hits": top_3_hits,
        "top_1_accuracy": top_1_hits / total if total else 0.0,
        "top_3_recall": top_3_hits / total if total else 0.0,
        "results": results,
    }


def save_metrics(data: dict, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    config = EvaluateSemanticConfig()

    benchmark_path = Path(args.benchmark).resolve() if args.benchmark else config.benchmark_path
    output_path = Path(args.output).resolve() if args.output else config.output_path

    benchmark = load_benchmark(benchmark_path)
    metrics = evaluate_benchmark(benchmark)

    save_metrics(metrics, output_path)

    print(f"Total: {metrics['total_questions']}")
    print(f"Top-1: {metrics['top_1_hits']}")
    print(f"Top-3: {metrics['top_3_hits']}")
    print(f"Top-1 accuracy: {metrics['top_1_accuracy']:.4f}")
    print(f"Top-3 recall: {metrics['top_3_recall']:.4f}")
    print(f"Output: {output_path}")

if __name__ == "__main__":
    main()