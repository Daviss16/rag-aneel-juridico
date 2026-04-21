#rodar no terminal: python3 -m src.retrieval.evaluations.evaluate_bm25

from __future__ import annotations
import argparse
from dataclasses import dataclass
from pathlib import Path

from src.retrieval.evaluations.evaluation_utils import load_benchmark, evaluate_benchmark, save_metrics
from src.retrieval.bm25_retriever import build_bm25_retriever

@dataclass(frozen=True)
class EvaluateBM25Config:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent.parent
    benchmark_path: Path = base_dir / "data" / "benchmark" / "benchmark_questions_v2.json"
    output_path: Path = base_dir / "data" / "retrieval" / "evaluation" / "bm25_metrics.json"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    return parser.parse_args()

def main():
    args = parse_args()
    config = EvaluateBM25Config()

    benchmark_path = Path(args.benchmark).resolve() if args.benchmark else config.benchmark_path
    output_path = Path(args.output).resolve() if args.output else config.output_path

    benchmark = load_benchmark(benchmark_path)
    retriever = build_bm25_retriever()
    
    metrics = evaluate_benchmark(benchmark, retriever=retriever)
    save_metrics(metrics, output_path)

    print(f"BM25 - Top-1 Accuracy: {metrics['top_1_accuracy']:.4f}")
    print(f"BM25 - Top-3 Recall: {metrics['top_3_recall']:.4f}")
    print(f"Resultados salvos em: {output_path}")

if __name__ == "__main__":
    main()