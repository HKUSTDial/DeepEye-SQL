from __future__ import annotations

import json
import sys
from argparse import ArgumentParser
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

sys.path.append(".")

from app.dataset import load_dataset
from app.logger import configure_logger


def main() -> None:
    parser = ArgumentParser(description="Inspect a dataset snapshot with dynamic few-shot preparation results.")
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML config file")
    parser.add_argument("--input_path", type=str, default=None, help="Prepared dataset snapshot path")
    parser.add_argument("--output_path", type=str, default=None, help="Optional JSON summary output path")
    parser.add_argument("--details_output_path", type=str, default=None, help="Optional JSONL per-item detail output path")
    parser.add_argument("--max_items", type=int, default=None, help="Only inspect the first N items")
    args = parser.parse_args()

    if args.config:
        import os

        os.environ["CONFIG_PATH"] = args.config

    from app.config import get_config

    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)

    input_path = args.input_path or app_config.few_shot_index_config.prepared_save_path
    dataset = load_dataset(input_path)
    items = list(dataset)
    if args.max_items is not None:
        items = items[: args.max_items]

    details = [_inspect_item(item) for item in items]
    summary = _summarize(details, input_path=input_path)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.output_path:
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.details_output_path:
        details_output_path = Path(args.details_output_path)
        details_output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(details_output_path, "w", encoding="utf-8") as f:
            for detail in details:
                f.write(json.dumps(detail, ensure_ascii=False) + "\n")


def _inspect_item(item: Any) -> Dict[str, Any]:
    examples = getattr(item, "few_shot_examples", None) or []
    metadata = getattr(item, "few_shot_preparation_metadata", None) or {}
    if not isinstance(metadata, dict):
        metadata = {}
    preliminary_metadata = metadata.get("preliminary_sql") or {}
    if not isinstance(preliminary_metadata, dict):
        preliminary_metadata = {}

    preliminary_sql = getattr(item, "few_shot_preliminary_sql", None)
    selected = bool(preliminary_metadata.get("selected", bool(preliminary_sql)))
    source = preliminary_metadata.get("source")
    if source is None:
        source = "unknown" if preliminary_sql else "none"

    example_scores = [_optional_float(example.get("retrieval_score")) for example in examples]
    question_scores = [_optional_float(example.get("question_score")) for example in examples]
    sql_scores = [_optional_float(example.get("sql_score")) for example in examples]
    source_db_ids = [str(example.get("source_db_id")) for example in examples if example.get("source_db_id") is not None]

    return {
        "item_id": item.get_item_id() if hasattr(item, "get_item_id") else str(getattr(item, "question_id", "")),
        "database_id": getattr(item, "database_id", None),
        "example_count": len(examples),
        "top1_retrieval_score": _first_present(example_scores),
        "top1_question_score": _first_present(question_scores),
        "top1_sql_score": _first_present(sql_scores),
        "mean_retrieval_score": _mean([score for score in example_scores if score is not None]),
        "mean_question_score": _mean([score for score in question_scores if score is not None]),
        "mean_sql_score": _mean([score for score in sql_scores if score is not None]),
        "source_db_count": len(set(source_db_ids)),
        "preliminary_sql_selected": selected,
        "preliminary_sql_source": source,
        "preliminary_sql_length": len(preliminary_sql or ""),
        "candidate_count": _optional_int(preliminary_metadata.get("candidate_count")),
        "executable_candidate_count": _optional_int(preliminary_metadata.get("executable_candidate_count")),
        "non_empty_candidate_count": _optional_int(preliminary_metadata.get("non_empty_candidate_count")),
        "consistency_score": _optional_float(preliminary_metadata.get("consistency_score")),
        "preliminary_total_tokens": _extract_total_tokens(preliminary_metadata.get("token_usage")),
        "target_mask_source": metadata.get("target_mask_source", "unknown"),
        "used_sql_similarity": bool(metadata.get("used_sql_similarity", any(score is not None for score in sql_scores))),
    }


def _summarize(details: List[Dict[str, Any]], *, input_path: str) -> Dict[str, Any]:
    total_items = len(details)
    source_counts = Counter(str(detail["preliminary_sql_source"]) for detail in details)
    target_mask_source_counts = Counter(str(detail["target_mask_source"]) for detail in details)
    selected_count = sum(1 for detail in details if detail["preliminary_sql_selected"])
    items_with_examples = sum(1 for detail in details if detail["example_count"] > 0)
    items_using_sql_similarity = sum(1 for detail in details if detail["used_sql_similarity"])

    candidate_counts = _present_numbers(detail["candidate_count"] for detail in details)
    executable_counts = _present_numbers(detail["executable_candidate_count"] for detail in details)
    non_empty_counts = _present_numbers(detail["non_empty_candidate_count"] for detail in details)
    consistency_scores = _present_numbers(detail["consistency_score"] for detail in details)
    preliminary_total_tokens = _present_numbers(detail["preliminary_total_tokens"] for detail in details)

    executable_ratios = []
    non_empty_ratios = []
    for detail in details:
        candidate_count = detail["candidate_count"]
        if candidate_count:
            executable_count = detail["executable_candidate_count"] or 0
            non_empty_count = detail["non_empty_candidate_count"] or 0
            executable_ratios.append(executable_count / candidate_count)
            non_empty_ratios.append(non_empty_count / candidate_count)

    return {
        "input_path": str(input_path),
        "total_items": total_items,
        "few_shot_examples": {
            "items_with_examples": items_with_examples,
            "items_with_examples_rate": _rate(items_with_examples, total_items),
            "example_count": _stats(detail["example_count"] for detail in details),
            "source_db_count": _stats(detail["source_db_count"] for detail in details),
        },
        "retrieval_scores": {
            "top1_retrieval_score": _stats(detail["top1_retrieval_score"] for detail in details),
            "top1_question_score": _stats(detail["top1_question_score"] for detail in details),
            "top1_sql_score": _stats(detail["top1_sql_score"] for detail in details),
            "mean_retrieval_score": _stats(detail["mean_retrieval_score"] for detail in details),
            "mean_question_score": _stats(detail["mean_question_score"] for detail in details),
            "mean_sql_score": _stats(detail["mean_sql_score"] for detail in details),
        },
        "preliminary_sql": {
            "selected_items": selected_count,
            "selected_rate": _rate(selected_count, total_items),
            "source_counts": dict(source_counts),
            "candidate_count": _stats(candidate_counts),
            "executable_candidate_count": _stats(executable_counts),
            "non_empty_candidate_count": _stats(non_empty_counts),
            "executable_candidate_ratio": _stats(executable_ratios),
            "non_empty_candidate_ratio": _stats(non_empty_ratios),
            "consistency_score": _stats(consistency_scores),
            "total_tokens": _stats(preliminary_total_tokens),
        },
        "target_mask": {
            "source_counts": dict(target_mask_source_counts),
            "items_using_sql_similarity": items_using_sql_similarity,
            "items_using_sql_similarity_rate": _rate(items_using_sql_similarity, total_items),
        },
    }


def _present_numbers(values: Iterable[Optional[float]]) -> List[float]:
    return [float(value) for value in values if value is not None]


def _stats(values: Iterable[Optional[float]]) -> Dict[str, Optional[float]]:
    present_values = sorted(_present_numbers(values))
    if not present_values:
        return {"count": 0, "min": None, "mean": None, "p50": None, "p90": None, "max": None}
    return {
        "count": len(present_values),
        "min": present_values[0],
        "mean": sum(present_values) / len(present_values),
        "p50": _quantile(present_values, 0.5),
        "p90": _quantile(present_values, 0.9),
        "max": present_values[-1],
    }


def _quantile(values: List[float], quantile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = quantile * (len(values) - 1)
    lower_idx = int(position)
    upper_idx = min(lower_idx + 1, len(values) - 1)
    fraction = position - lower_idx
    return values[lower_idx] * (1 - fraction) + values[upper_idx] * fraction


def _rate(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return numerator / denominator


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _first_present(values: List[Optional[float]]) -> Optional[float]:
    for value in values:
        if value is not None:
            return value
    return None


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_total_tokens(token_usage: Any) -> Optional[int]:
    if not isinstance(token_usage, dict):
        return None
    return _optional_int(token_usage.get("total_tokens"))


if __name__ == "__main__":
    main()
