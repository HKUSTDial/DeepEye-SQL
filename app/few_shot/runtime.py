from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.few_shot.masker import TargetMaskResult, mask_target_question_sql
from app.few_shot.retriever import FewShotRetriever
from app.llm import LLM


@dataclass
class PreparedFewShotExamples:
    examples: List[Dict[str, Any]]
    masked_question: str
    masked_sql: Optional[str]
    mask_source: str


class TargetMaskCache:
    def __init__(self, cache_path: str | Path) -> None:
        self.cache_path = Path(cache_path)
        self._lock = threading.Lock()
        self._records: Dict[str, TargetMaskResult] = {}
        self._load()

    def get(self, key: str) -> Optional[TargetMaskResult]:
        with self._lock:
            return self._records.get(key)

    def append(self, key: str, result: TargetMaskResult) -> None:
        record = {
            "key": key,
            "masked_question": result.masked_question,
            "masked_sql": result.masked_sql,
            "source": result.source,
        }
        with self._lock:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                f.flush()
            self._records[key] = result

    def _load(self) -> None:
        if not self.cache_path.exists():
            return

        with open(self.cache_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                key = record.get("key")
                masked_question = record.get("masked_question")
                masked_sql = record.get("masked_sql")
                if not isinstance(key, str) or not isinstance(masked_question, str):
                    continue
                if masked_sql is not None and not isinstance(masked_sql, str):
                    continue

                masked_question = masked_question.strip()
                masked_sql = masked_sql.strip() if isinstance(masked_sql, str) else None
                if not masked_question:
                    continue

                self._records[key] = TargetMaskResult(
                    masked_question=masked_question,
                    masked_sql=masked_sql or None,
                    source=str(record.get("source", "cache")),
                )


def prepare_few_shot_examples_for_item(
    data_item: Any,
    retriever: FewShotRetriever,
    llm: Optional[LLM],
    top_k: int,
    question_weight: float,
    sql_weight: float,
    preliminary_sql: Optional[str] = None,
    cache: Optional[TargetMaskCache] = None,
    skip_mask_llm: bool = False,
    llm_timeout: int = 300,
) -> PreparedFewShotExamples:
    mask_result = _get_or_create_target_mask(
        data_item=data_item,
        preliminary_sql=preliminary_sql,
        llm=llm,
        cache=cache,
        skip_mask_llm=skip_mask_llm,
        llm_timeout=llm_timeout,
    )
    retrieval_results = retriever.retrieve_by_texts(
        masked_question=mask_result.masked_question,
        masked_sql=mask_result.masked_sql,
        top_k=top_k,
        question_weight=question_weight,
        sql_weight=sql_weight,
    )
    examples = [
        {
            "question": result.example["question"],
            "evidence": result.example.get("evidence", ""),
            "sql": result.example["sql"],
            "source_example_id": result.example.get("example_id"),
            "source_db_id": result.example.get("db_id"),
            "retrieval_score": result.score,
            "question_score": result.question_score,
            "sql_score": result.sql_score,
            "masked_question": result.example.get("masked_question"),
            "masked_sql": result.example.get("masked_sql"),
        }
        for result in retrieval_results
    ]
    return PreparedFewShotExamples(
        examples=examples,
        masked_question=mask_result.masked_question,
        masked_sql=mask_result.masked_sql,
        mask_source=mask_result.source,
    )


def _get_or_create_target_mask(
    data_item: Any,
    preliminary_sql: Optional[str],
    llm: Optional[LLM],
    cache: Optional[TargetMaskCache],
    skip_mask_llm: bool,
    llm_timeout: int,
) -> TargetMaskResult:
    cache_key = make_target_mask_cache_key(data_item=data_item, preliminary_sql=preliminary_sql)
    if cache is not None and not skip_mask_llm:
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            return TargetMaskResult(
                masked_question=cached_result.masked_question,
                masked_sql=cached_result.masked_sql,
                source="cache",
            )

    mask_result = mask_target_question_sql(
        question=data_item.question,
        evidence=getattr(data_item, "evidence", "") or "",
        sql=preliminary_sql,
        llm=llm,
        skip_llm=skip_mask_llm,
        llm_timeout=llm_timeout,
        item_id=data_item.get_item_id() if hasattr(data_item, "get_item_id") else None,
    )
    if cache is not None and mask_result.source == "llm":
        cache.append(cache_key, mask_result)
    return mask_result


def make_target_mask_cache_key(data_item: Any, preliminary_sql: Optional[str]) -> str:
    payload = {
        "version": 2,
        "item_id": data_item.get_item_id() if hasattr(data_item, "get_item_id") else getattr(data_item, "question_id", None),
        "database_id": getattr(data_item, "database_id", ""),
        "question": getattr(data_item, "question", ""),
        "evidence": getattr(data_item, "evidence", "") or "",
        "preliminary_sql": preliminary_sql.strip() if preliminary_sql else "",
        "mode": "question_sql" if preliminary_sql else "question_only",
    }
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(content.encode("utf-8")).hexdigest()


def load_preliminary_sql_map(path: Optional[str | Path]) -> Dict[str, str]:
    if path is None:
        return {}

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Preliminary SQL map not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        return {str(key): str(value) for key, value in data.items() if value is not None}
    if isinstance(data, list):
        result: Dict[str, str] = {}
        for record in data:
            if not isinstance(record, dict):
                continue
            sql = record.get("preliminary_sql", record.get("sql", record.get("final_selected_sql")))
            if sql is None:
                continue
            for key_name in ("item_id", "question_id", "instance_id"):
                key = record.get(key_name)
                if key is not None:
                    result[str(key)] = str(sql)
                    break
        return result

    raise ValueError(f"Unsupported preliminary SQL map format at {path}")


def get_preliminary_sql_for_item(data_item: Any, preliminary_sql_map: Dict[str, str]) -> Optional[str]:
    for key in _candidate_item_keys(data_item):
        sql = preliminary_sql_map.get(key)
        if isinstance(sql, str) and sql.strip():
            return sql.strip()
    return None


def _candidate_item_keys(data_item: Any) -> Iterable[str]:
    if hasattr(data_item, "get_item_id"):
        yield str(data_item.get_item_id())
    if hasattr(data_item, "question_id"):
        yield str(data_item.question_id)
    if hasattr(data_item, "instance_id"):
        instance_id = getattr(data_item, "instance_id")
        if instance_id:
            yield str(instance_id)
