from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.few_shot.train_loader import TrainingExample
from app.llm import LLM
from app.logger import logger


MASK_SYSTEM_PROMPT = {
    "role": "system",
    "content": (
        "You produce abstract retrieval keys for text-to-SQL few-shot selection. "
        "Return only a compact JSON object and no explanation."
    ),
}


MASK_USER_PROMPT_TEMPLATE = """Mask database-specific names and literal values while preserving query intent.

Rules:
- In the question, replace schema names, entity names, literal values, numbers, dates, and domain-specific nouns with generic placeholders such as <entity>, <value>, <number>, <date>, or <concept>.
- In the SQL, replace every table name, column name, alias, string literal, numeric literal, and date/time literal with placeholders.
- Use SQL placeholders such as <table>, <column>, <alias>, <value>, <number>, and <date>.
- The masked_sql MUST NOT contain original table names, original column names, aliases, string literals, numeric literals, or date/time literals from the input SQL.
- Preserve SQL operators, aggregation functions, GROUP BY / HAVING / ORDER BY / LIMIT, joins, subqueries, set operators, and comparison logic.
- Keep the masked SQL syntactically recognizable enough to compare query skeletons.
- If the hint/evidence clarifies an entity or value, reflect only its abstract role in the masked question.
- Return exactly this JSON schema: {{"masked_question": "...", "masked_sql": "..."}}

Examples:
Input SQL: SELECT director_name FROM movies WHERE movie_title = 'Sex, Drink and Bloodshed'
Masked SQL: SELECT <column> FROM <table> WHERE <column> = <value>

Input SQL: SELECT COUNT(*) FROM head WHERE age > 56 GROUP BY department_id ORDER BY COUNT(*) DESC LIMIT 1
Masked SQL: SELECT COUNT(*) FROM <table> WHERE <column> > <number> GROUP BY <column> ORDER BY COUNT(*) DESC LIMIT <number>

Question:
{question}

Evidence:
{evidence}

SQL:
{sql}
"""


@dataclass
class MaskResult:
    masked_question: str
    masked_sql: str
    source: str


class MaskCache:
    def __init__(self, cache_path: str | Path) -> None:
        self.cache_path = Path(cache_path)
        self._lock = threading.Lock()
        self._records: Dict[str, MaskResult] = {}
        self._load()

    def get(self, key: str) -> Optional[MaskResult]:
        with self._lock:
            return self._records.get(key)

    def append(self, key: str, result: MaskResult) -> None:
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
                if not isinstance(key, str) or not isinstance(masked_question, str) or not isinstance(masked_sql, str):
                    continue

                masked_question = masked_question.strip()
                masked_sql = masked_sql.strip()
                if not masked_question or not masked_sql:
                    continue

                self._records[key] = MaskResult(
                    masked_question=masked_question,
                    masked_sql=masked_sql,
                    source=str(record.get("source", "cache")),
                )


def make_mask_cache_key(example: TrainingExample) -> str:
    payload = {
        "dataset": example.dataset,
        "db_id": example.db_id,
        "question": example.question,
        "evidence": example.evidence,
        "sql": example.sql,
    }
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(content.encode("utf-8")).hexdigest()


def mask_training_examples(
    examples: Iterable[TrainingExample],
    llm: Optional[LLM],
    cache: Optional[MaskCache] = None,
    skip_llm: bool = False,
) -> List[MaskResult]:
    results: List[MaskResult] = []
    for example in examples:
        results.append(mask_training_example(example=example, llm=llm, cache=cache, skip_llm=skip_llm))
    return results


def mask_training_example(
    example: TrainingExample,
    llm: Optional[LLM],
    cache: Optional[MaskCache] = None,
    skip_llm: bool = False,
) -> MaskResult:
    if skip_llm:
        return MaskResult(masked_question=example.question_context, masked_sql=example.sql, source="raw")

    cache_key = make_mask_cache_key(example)
    if cache is not None:
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            return MaskResult(
                masked_question=cached_result.masked_question,
                masked_sql=cached_result.masked_sql,
                source="cache",
            )

    if llm is None:
        return MaskResult(masked_question=example.question_context, masked_sql=example.sql, source="raw")

    try:
        messages = [
            {
                "role": "user",
                "content": MASK_USER_PROMPT_TEMPLATE.format(
                    question=example.question,
                    evidence=example.evidence or "None",
                    sql=example.sql,
                ),
            }
        ]
        choices, _ = llm.ask(
            messages=messages,
            system_message=MASK_SYSTEM_PROMPT,
            max_tokens=2048,
            temperature=0.0,
            timeout=300,
        )
        parsed = parse_mask_response(choices[0].content)
        result = MaskResult(
            masked_question=parsed["masked_question"],
            masked_sql=parsed["masked_sql"],
            source="llm",
        )
        if cache is not None:
            cache.append(cache_key, result)
        return result
    except Exception as exc:
        logger.warning(f"Failed to mask few-shot example {example.example_id}; using raw text for this run. Error: {exc}")
        return MaskResult(masked_question=example.question_context, masked_sql=example.sql, source="fallback")


def parse_mask_response(response: str) -> Dict[str, str]:
    if not isinstance(response, str) or not response.strip():
        raise ValueError("Mask response is empty")

    for candidate in _iter_json_candidates(response):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue

        masked_question = parsed.get("masked_question")
        masked_sql = parsed.get("masked_sql")
        if isinstance(masked_question, str) and isinstance(masked_sql, str):
            masked_question = masked_question.strip()
            masked_sql = masked_sql.strip()
            if masked_question and masked_sql:
                return {"masked_question": masked_question, "masked_sql": masked_sql}

    raise ValueError(f"Could not parse masked question/sql JSON from response: {response[:500]}")


def _iter_json_candidates(response: str) -> Iterable[str]:
    fenced_matches = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", response, flags=re.IGNORECASE | re.DOTALL)
    for match in fenced_matches:
        yield match.strip()

    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", response):
        start = match.start()
        try:
            _, end = decoder.raw_decode(response[start:])
        except json.JSONDecodeError:
            continue
        yield response[start : start + end].strip()
