from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class TrainingExample:
    example_id: str
    dataset: str
    db_id: str
    question: str
    sql: str
    evidence: str = ""
    source: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def question_context(self) -> str:
        if self.evidence:
            return f"{self.question}\nHint: {self.evidence}"
        return self.question

    def to_record(self, masked_question: str, masked_sql: str, mask_source: str) -> Dict[str, Any]:
        return {
            "example_id": self.example_id,
            "dataset": self.dataset,
            "db_id": self.db_id,
            "question": self.question,
            "question_context": self.question_context,
            "evidence": self.evidence,
            "sql": self.sql,
            "masked_question": masked_question,
            "masked_sql": masked_sql,
            "mask_source": mask_source,
            "source": self.source,
            "metadata": self.metadata,
        }


def load_training_examples(
    dataset_type: str,
    root_path: str | Path,
    max_samples: Optional[int] = None,
    max_samples_per_db: Optional[int] = None,
) -> List[TrainingExample]:
    dataset_type = dataset_type.lower()
    root_path = Path(root_path)

    if dataset_type == "bird":
        examples = _load_bird_training_examples(root_path)
    elif dataset_type == "spider":
        examples = _load_spider_training_examples(root_path)
    else:
        raise ValueError(f"Unsupported few-shot training dataset: {dataset_type}")

    if max_samples_per_db is not None:
        examples = _limit_examples_per_db(examples, max_samples_per_db)
    if max_samples is not None:
        examples = examples[:max_samples]
    return examples


def _limit_examples_per_db(examples: List[TrainingExample], max_samples_per_db: int) -> List[TrainingExample]:
    if max_samples_per_db < 1:
        raise ValueError(f"max_samples_per_db must be >= 1, got {max_samples_per_db}")

    db_counts: Dict[str, int] = {}
    limited_examples = []
    for example in examples:
        count = db_counts.get(example.db_id, 0)
        if count >= max_samples_per_db:
            continue
        limited_examples.append(example)
        db_counts[example.db_id] = count + 1
    return limited_examples


def _load_bird_training_examples(root_path: Path) -> List[TrainingExample]:
    train_path = _first_existing_path(
        [
            root_path / "train" / "train.json",
            root_path / "train.json",
        ]
    )
    raw_examples = _load_json_list(train_path)
    return list(_iter_bird_examples(raw_examples, source=str(train_path)))


def _iter_bird_examples(raw_examples: Iterable[Dict[str, Any]], source: str) -> Iterable[TrainingExample]:
    for idx, raw_example in enumerate(raw_examples):
        question = _clean_str(raw_example.get("question"))
        sql = _clean_str(raw_example.get("SQL", raw_example.get("query", raw_example.get("sql"))))
        db_id = _clean_str(raw_example.get("db_id"))
        if not question or not sql or not db_id:
            continue

        question_id = raw_example.get("question_id", idx)
        yield TrainingExample(
            example_id=f"bird:{question_id}",
            dataset="bird",
            db_id=db_id,
            question=question,
            evidence=_clean_str(raw_example.get("evidence")),
            sql=sql,
            source=source,
            metadata={
                "question_id": question_id,
                "difficulty": raw_example.get("difficulty", ""),
            },
        )


def _load_spider_training_examples(root_path: Path) -> List[TrainingExample]:
    train_paths = [
        _first_existing_path([root_path / "train_spider.json"]),
        _first_existing_path([root_path / "train_others.json"], required=False),
    ]
    examples: List[TrainingExample] = []
    for train_path in train_paths:
        if train_path is None:
            continue
        split_name = train_path.stem
        raw_examples = _load_json_list(train_path)
        examples.extend(_iter_spider_examples(raw_examples, source=str(train_path), split_name=split_name))
    return examples


def _iter_spider_examples(
    raw_examples: Iterable[Dict[str, Any]],
    source: str,
    split_name: str,
) -> Iterable[TrainingExample]:
    for idx, raw_example in enumerate(raw_examples):
        question = _clean_str(raw_example.get("question"))
        sql = _clean_str(raw_example.get("query", raw_example.get("SQL", raw_example.get("sql"))))
        db_id = _clean_str(raw_example.get("db_id"))
        if not question or not sql or not db_id:
            continue

        yield TrainingExample(
            example_id=f"spider:{split_name}:{idx}",
            dataset="spider",
            db_id=db_id,
            question=question,
            sql=sql,
            source=source,
            metadata={"source_split": split_name, "source_index": idx},
        )


def _load_json_list(path: Path) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON list at {path}")
    return data


def _first_existing_path(paths: List[Path], required: bool = True) -> Optional[Path]:
    for path in paths:
        if path.exists():
            return path
    if required:
        candidates = ", ".join(str(path) for path in paths)
        raise FileNotFoundError(f"Could not find any expected training file: {candidates}")
    return None


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return value.strip()
