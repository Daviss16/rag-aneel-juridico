# rodar no terminal: python3 -m src.retrieval.evaluations.grid_search_reranker

from __future__ import annotations
import argparse
import itertools
import json
from pathlib import Path

from src.retrieval.evaluations.evaluation_utils import load_benchmark, evaluate_benchmark
from src.retrieval.bm25_retriever import build_bm25_retriever
from src.retrieval.metadata_reranker import MetadataRerankConfig

def generate_grid_configs():
    grid = {
        "top_n_rerank": [20, 50, 100],
        "act_number_match_boost": [0.10, 0.14, 0.25],
        "year_match_boost": [0.03, 0.08, 0.15],
        "ementa_overlap_weight": [0.10, 0.16, 0.25],
        "max_total_boost_ratio": [0.35, 0.50, 0.70]
    }
    
    keys = list(grid.keys())
    combinations = itertools.product(*(grid[k] for k in keys))
    
    configs = []
    for combo in combinations:
        params = dict(zip(keys, combo))
        config = MetadataRerankConfig(
            top_n_rerank=params["top_n_rerank"],
            act_number_match_boost=params["act_number_match_boost"],
            year_match_boost=params["year_match_boost"],
            ementa_overlap_weight=params["ementa_overlap_weight"],
            max_total_boost_ratio=params["max_total_boost_ratio"],
            act_type_match_boost=0.08,
            sigla_match_boost=0.06,
            assunto_overlap_weight=0.10,
            pdf_tipo_overlap_weight=0.04,
            autor_overlap_weight=0.03,
            min_token_len_for_overlap=4
        )
        configs.append((params, config))
        
    return configs

def main():
    base_dir = Path(__file__).resolve().parent.parent.parent.parent
    benchmark_path = base_dir / "data" / "benchmark" / "benchmark_questions_v3.json"
    output_path = base_dir / "data" / "retrieval" / "evaluation" / "grid_search_results.json"

    print("Carregando benchmark...")
    benchmark = load_benchmark(benchmark_path)
    
    configs_to_test = generate_grid_configs()
    total_tests = len(configs_to_test)
    print(f"Iniciando Grid Search com {total_tests} combinações...\n")

    best_top3_recall = 0
    best_top1_accuracy = 0
    best_params = None
    all_results = []

    for idx, (params, config) in enumerate(configs_to_test, 1):
        retriever = build_bm25_retriever(rerank_config=config)
        
        metrics = evaluate_benchmark(benchmark, retriever=retriever)
        
        top3 = metrics["top_3_recall"]
        top1 = metrics["top_1_accuracy"]
        
        all_results.append({
            "params": params,
            "top_1_accuracy": top1,
            "top_3_recall": top3
        })
        
        print(f"[{idx}/{total_tests}] Top-3: {top3:.4f} | Top-1: {top1:.4f} | Params: {params}")

        if top3 > best_top3_recall or (top3 == best_top3_recall and top1 > best_top1_accuracy):
            best_top3_recall = top3
            best_top1_accuracy = top1
            best_params = params
            print(f"  >>> NOVO RECORDE! Top-3: {top3:.4f}\n")

    print("\n" + "="*50)
    print("GRID SEARCH CONCLUÍDO!")
    print(f"Melhor Top-3 Recall: {best_top3_recall:.4f}")
    print(f"Melhor Top-1 Accuracy: {best_top1_accuracy:.4f}")
    print(f"Melhores Parâmetros: {best_params}")
    print("="*50)

    all_results.sort(key=lambda x: (x["top_3_recall"], x["top_1_accuracy"]), reverse=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"best_params": best_params, "all_results": all_results}, f, indent=2, ensure_ascii=False)
    print(f"Resultados detalhados salvos em {output_path}")

if __name__ == "__main__":
    main()