#rode no terminal: python3 -m src.retrieval.evaluations.evaluate_hybrid

import argparse
from dataclasses import dataclass
from pathlib import Path


from src.retrieval.evaluations.evaluation_utils import load_benchmark, evaluate_benchmark, save_metrics
from src.retrieval.hybrid_retriever import build_hybrid_retriever

@dataclass(frozen=True)
class EvaluateHybridConfig:
    base_dir: Path = Path(__file__).resolve().parent.parent.parent.parent
    benchmark_path: Path = base_dir / "data" / "benchmark" / "benchmark_questions_v2.json"
    output_path: Path = base_dir / "data" / "retrieval" / "evaluation" / "hybrid_metrics.json"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    return parser.parse_args()

def main():
    args = parse_args()
    config = EvaluateHybridConfig()

    benchmark_path = Path(args.benchmark).resolve() if args.benchmark else config.benchmark_path
    output_path = Path(args.output).resolve() if args.output else config.output_path

    print("Carregando benchmark...")
    benchmark = load_benchmark(benchmark_path)
    
    print("Construindo Hybrid Retriever...")
    retriever = build_hybrid_retriever() 

    print("Avaliando...")
    metrics = evaluate_benchmark(benchmark, retriever=retriever)

    save_metrics(metrics, output_path)

    print(f"Total: {metrics['total_questions']}")
    print(f"Hybrid - Top-1 accuracy: {metrics['top_1_accuracy']:.4f}")
    print(f"Hybrid - Top-3 recall: {metrics['top_3_recall']:.4f}")
    print(f"Output: {output_path}")

if __name__ == "__main__":
    main()