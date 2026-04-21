from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List, Dict

def load_benchmark(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("O benchmark deve ser uma lista de perguntas.")

    items = []
    for idx, item in enumerate(data, start=1):
        question = (item.get("question") or "").strip()
        expected_doc = (item.get("expected_doc") or "").strip()
        question_type = (item.get("type") or "desconhecido").strip()

        if not question or not expected_doc:
            continue  

        item["question"] = question
        item["expected_doc"] = expected_doc
        item["type"] = question_type

        items.append(item)

    return items

def unique_docs_from_results(results: List[Dict[str, Any]], top_k_docs: int) -> List[str]:
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

def evaluate_question(retriever: Any, question: str, question_type: str, expected_doc: str, search_k: int = 10) -> Dict[str, Any]:
    chunk_results = retriever.search(question, top_k=search_k)
    ranked_docs = unique_docs_from_results(chunk_results, top_k_docs=3)

    top_1_hit = bool(ranked_docs) and ranked_docs[0] == expected_doc
    top_3_hit = expected_doc in ranked_docs

    return {
        "question": question,
        "type": question_type,
        "expected_doc": expected_doc,
        "retrieved_docs": ranked_docs,
        "detailed_results": chunk_results[:3],
        "top_1_hit": top_1_hit,
        "top_3_hit": top_3_hit,
    }

def evaluate_benchmark(benchmark: List[Dict[str, str]], retriever: Any, search_k: int = 10) -> Dict[str, Any]:
    total = len(benchmark)
    top_1_hits = 0
    top_3_hits = 0
    results = []

    for item in benchmark:
        result = evaluate_question(
            retriever=retriever,
            question=item["question"],
            question_type=item.get("type", "desconhecido"), 
            expected_doc=item["expected_doc"],
            search_k=search_k
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

def save_metrics(data: Dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)