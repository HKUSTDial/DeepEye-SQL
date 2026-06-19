from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import traceback
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from app.config import DatasetConfig
from app.config.config import PreliminarySQLConfig
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.pipeline.sql_generation.generators import DCGenerator, SkeletonGenerator
from app.services import (
    configure_execution_service,
    configure_schema_service,
    get_execution_service,
    reset_execution_service,
    reset_schema_service,
)


TOKEN_USAGE_ZERO = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


@dataclass
class PreliminarySQLResult:
    sql: Optional[str]
    candidates: List[str]
    executable_candidates: int
    non_empty_candidates: int
    consistency_score: float
    token_usage: Dict[str, int]


class PreliminarySQLGenerator:
    def __init__(
        self,
        config: PreliminarySQLConfig,
        dataset_config: DatasetConfig,
        *,
        extractor_max_retry: int,
        parallelism: int,
    ) -> None:
        if config.llm is None:
            raise ValueError("[few_shot_index.preliminary_sql.llm] is required")

        self._config = config
        self._dataset_config = dataset_config
        configure_schema_service(max_value_example_length=dataset_config.max_value_example_length)
        configure_execution_service(
            default_timeout=dataset_config.sql_execution_timeout,
            bigquery_credential_path=dataset_config.bigquery_credential_path,
            snowflake_credential_path=dataset_config.snowflake_credential_path,
        )
        self._llm = LLM(config.llm)
        self._parallelism = max(1, parallelism)
        self._executor = ThreadPoolExecutor(max_workers=self._parallelism)
        logger.info(f"Preliminary SQL parallelism: {self._parallelism}")
        self._execution_service = get_execution_service()
        self._dc_generator = DCGenerator(extractor_max_retry=extractor_max_retry)
        self._skeleton_generator = SkeletonGenerator(extractor_max_retry=extractor_max_retry)

    def generate(self, data_item: DataItem) -> PreliminarySQLResult:
        generation_item = data_item.model_copy(deep=True)
        generation_item.database_schema_after_schema_linking = data_item.database_schema

        generation_tasks = {
            "dc": self._executor.submit(
                self._dc_generator.generate,
                generation_item,
                self._llm,
                self._config.dc_sampling_budget,
            ),
            "skeleton": self._executor.submit(
                self._skeleton_generator.generate,
                generation_item,
                self._llm,
                self._config.skeleton_sampling_budget,
            ),
        }

        candidates: List[str] = []
        total_token_usage = dict(TOKEN_USAGE_ZERO)
        for name, future in generation_tasks.items():
            try:
                generated_candidates, token_usage = future.result()
            except Exception as exc:
                logger.error(f"Error in preliminary {name} generation for item {data_item.get_item_id()}: {exc}")
                traceback.print_exc()
                generated_candidates, token_usage = [], TOKEN_USAGE_ZERO

            for key in total_token_usage:
                total_token_usage[key] += token_usage.get(key, 0)
            candidates.extend(generated_candidates or [])

        candidates = _deduplicate_sql_candidates(candidates)
        selected_sql, executable_count, non_empty_count, consistency_score = select_preliminary_sql_by_consistency(
            data_item,
            candidates,
            execution_service=self._execution_service,
        )
        return PreliminarySQLResult(
            sql=selected_sql,
            candidates=candidates,
            executable_candidates=executable_count,
            non_empty_candidates=non_empty_count,
            consistency_score=consistency_score,
            token_usage=total_token_usage,
        )

    def close(self) -> None:
        self._executor.shutdown(wait=True)
        reset_execution_service()
        reset_schema_service()
        self._llm = None
        self._execution_service = None


def select_preliminary_sql_by_consistency(
    data_item: DataItem,
    sql_candidates: List[str],
    *,
    execution_service: Any = None,
) -> Tuple[Optional[str], int, int, float]:
    if not sql_candidates:
        return None, 0, 0, 0.0

    execution_service = execution_service or get_execution_service()
    valid_candidates: List[Tuple[str, Any]] = []
    fallback_candidates: List[Tuple[str, Any]] = []
    result_by_sql = {}

    for sql_candidate in sql_candidates:
        execution_result = execution_service.execute(data_item, sql_candidate)
        result_by_sql[sql_candidate] = execution_result
        if execution_result.result_rows is None:
            continue
        result_hash = execution_service.hash_result(data_item, execution_result.result_rows)
        fallback_candidates.append((sql_candidate, result_hash))
        if len(execution_result.result_rows) > 0:
            valid_candidates.append((sql_candidate, result_hash))

    executable_count = len(fallback_candidates)
    non_empty_count = len(valid_candidates)
    if not valid_candidates and fallback_candidates:
        valid_candidates = fallback_candidates

    if not valid_candidates:
        return None, executable_count, non_empty_count, 0.0

    consistency_counter = Counter(result_hash for _, result_hash in valid_candidates)
    denominator = len(valid_candidates)
    deduplicated_candidates = []
    seen_result_hashes = set()
    for sql_candidate, result_hash in valid_candidates:
        if result_hash in seen_result_hashes:
            continue
        execution_result = result_by_sql[sql_candidate]
        execution_time = execution_result.execution_time if execution_result.execution_time is not None else np.inf
        consistency_score = consistency_counter[result_hash] / denominator
        deduplicated_candidates.append((sql_candidate, result_hash, consistency_score, execution_time))
        seen_result_hashes.add(result_hash)

    selected = sorted(
        deduplicated_candidates,
        key=lambda item: (item[2], -item[3]),
        reverse=True,
    )[0]
    return selected[0], executable_count, non_empty_count, selected[2]


def _deduplicate_sql_candidates(sql_candidates: List[str]) -> List[str]:
    deduplicated_candidates = []
    seen = set()
    for sql in sql_candidates:
        if not sql:
            continue
        normalized_sql = sql.strip()
        if not normalized_sql or normalized_sql in seen:
            continue
        deduplicated_candidates.append(normalized_sql)
        seen.add(normalized_sql)
    return deduplicated_candidates
