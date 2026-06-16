from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple


FewShotExample = Dict[str, str]


def normalize_few_shot_examples(raw_examples: Any) -> List[FewShotExample]:
    if not isinstance(raw_examples, list):
        return []

    examples: List[FewShotExample] = []
    for raw_example in raw_examples:
        if not isinstance(raw_example, Mapping):
            continue

        question = raw_example.get("question")
        sql = raw_example.get("sql", raw_example.get("SQL"))
        evidence = raw_example.get("evidence", raw_example.get("hint", raw_example.get("HINT", "")))
        if not isinstance(question, str) or not isinstance(sql, str):
            continue

        question = question.strip()
        sql = sql.strip()
        evidence = evidence.strip() if isinstance(evidence, str) else ""
        if question and sql:
            example = {"question": question, "sql": sql}
            if evidence:
                example["evidence"] = evidence
            examples.append(example)

    return examples


def get_few_shot_examples_for_item(
    data_item: Any,
    examples_by_id: Optional[Mapping[Any, Any]] = None,
) -> Tuple[List[FewShotExample], Optional[str]]:
    dynamic_examples = normalize_few_shot_examples(getattr(data_item, "few_shot_examples", None))
    if dynamic_examples:
        return dynamic_examples, "data_item"

    if not examples_by_id:
        return [], None

    question_id = str(data_item.question_id) if hasattr(data_item, "question_id") else None
    instance_id = str(data_item.instance_id) if hasattr(data_item, "instance_id") else None

    for item_id, source in ((question_id, "question_id"), (instance_id, "instance_id")):
        if not item_id:
            continue
        examples = _lookup_examples_by_id(examples_by_id, item_id)
        if examples:
            return examples, source

    return [], None


def _lookup_examples_by_id(examples_by_id: Mapping[Any, Any], item_id: str) -> List[FewShotExample]:
    candidate_keys: List[Any] = [item_id]
    if item_id.isdigit():
        candidate_keys.append(int(item_id))

    for key in candidate_keys:
        if key not in examples_by_id:
            continue
        examples = normalize_few_shot_examples(examples_by_id[key])
        if examples:
            return examples

    return []
