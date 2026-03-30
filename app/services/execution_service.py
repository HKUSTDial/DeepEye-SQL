import threading
from typing import Any, Optional

from app.db_utils import execute_sql_for_data_item, measure_execution_time_for_data_item
from app.pipeline.utils import get_execution_result_hash


class ExecutionService:
    def __init__(self):
        self._result_cache: dict[tuple[Any, ...], Any] = {}
        self._time_cache: dict[tuple[Any, ...], float] = {}
        self._lock = threading.Lock()

    def execute(self, data_item: Any, sql: str, timeout: Optional[int] = None, use_cache: bool = True):
        cache_key = self._build_result_key(data_item, sql, timeout)
        if use_cache:
            with self._lock:
                if cache_key in self._result_cache:
                    return self._result_cache[cache_key]

        result = execute_sql_for_data_item(data_item, sql, timeout=timeout)
        if use_cache:
            with self._lock:
                self._result_cache[cache_key] = result
        return result

    def measure_time(self, data_item: Any, sql: str, timeout: Optional[int] = None, repeat: int = 10, use_cache: bool = True) -> float:
        cache_key = self._build_time_key(data_item, sql, timeout, repeat)
        if use_cache:
            with self._lock:
                if cache_key in self._time_cache:
                    return self._time_cache[cache_key]

        execution_time = measure_execution_time_for_data_item(data_item, sql, timeout=timeout, repeat=repeat)
        if use_cache:
            with self._lock:
                self._time_cache[cache_key] = execution_time
        return execution_time

    def hash_result(self, data_item: Any, result_rows: Any) -> Any:
        return get_execution_result_hash(data_item, result_rows)

    def reset(self) -> None:
        with self._lock:
            self._result_cache.clear()
            self._time_cache.clear()

    @staticmethod
    def _build_result_key(data_item: Any, sql: str, timeout: Optional[int]) -> tuple[Any, ...]:
        return (
            getattr(data_item, "db_type", "sqlite"),
            data_item.database_path,
            sql,
            timeout,
        )

    @staticmethod
    def _build_time_key(data_item: Any, sql: str, timeout: Optional[int], repeat: int) -> tuple[Any, ...]:
        return (
            getattr(data_item, "db_type", "sqlite"),
            data_item.database_path,
            sql,
            timeout,
            repeat,
        )


_execution_service: ExecutionService | None = None


def get_execution_service() -> ExecutionService:
    global _execution_service
    if _execution_service is None:
        _execution_service = ExecutionService()
    return _execution_service


def reset_execution_service() -> None:
    global _execution_service
    if _execution_service is not None:
        _execution_service.reset()
        _execution_service = None
