import threading
from typing import Any, Optional

from app.db_utils.defaults import DEFAULT_SQL_EXECUTION_TIMEOUT
from app.db_utils import execute_sql_for_data_item, measure_execution_time_for_data_item
from app.pipeline.utils import get_execution_result_hash
from ._bounded_cache import BoundedCache


DEFAULT_EXECUTION_RESULT_CACHE_SIZE = 4096
DEFAULT_EXECUTION_TIME_CACHE_SIZE = 4096


class ExecutionService:
    def __init__(
        self,
        default_timeout: int = DEFAULT_SQL_EXECUTION_TIMEOUT,
        *,
        bigquery_credential_path: Optional[str] = None,
        snowflake_credential_path: Optional[str] = None,
        result_cache_size: int = DEFAULT_EXECUTION_RESULT_CACHE_SIZE,
        time_cache_size: int = DEFAULT_EXECUTION_TIME_CACHE_SIZE,
    ):
        self._result_cache: BoundedCache[tuple[Any, ...], Any] = BoundedCache(result_cache_size)
        self._time_cache: BoundedCache[tuple[Any, ...], float] = BoundedCache(time_cache_size)
        self._result_lock = threading.Lock()
        self._time_lock = threading.Lock()
        self._default_timeout = default_timeout
        self._bigquery_credential_path = bigquery_credential_path
        self._snowflake_credential_path = snowflake_credential_path

    def execute(self, data_item: Any, sql: str, timeout: Optional[int] = None, use_cache: bool = True):
        resolved_timeout = self._resolve_timeout(timeout)
        cache_key = self._build_result_key(data_item, sql, resolved_timeout)
        if use_cache:
            with self._result_lock:
                cached_result = self._result_cache.get(cache_key)
                if cached_result is not None:
                    return cached_result

        result = execute_sql_for_data_item(
            data_item,
            sql,
            timeout=resolved_timeout,
            bigquery_credential_path=self._bigquery_credential_path,
            snowflake_credential_path=self._snowflake_credential_path,
        )
        if use_cache:
            with self._result_lock:
                self._result_cache.set(cache_key, result)
        return result

    def measure_time(self, data_item: Any, sql: str, timeout: Optional[int] = None, repeat: int = 10, use_cache: bool = True) -> float:
        resolved_timeout = self._resolve_timeout(timeout)
        cache_key = self._build_time_key(data_item, sql, resolved_timeout, repeat)
        initial_execution_time = None
        if use_cache:
            with self._time_lock:
                cached_time = self._time_cache.get(cache_key)
                if cached_time is not None:
                    return cached_time
            with self._result_lock:
                cached_result = self._result_cache.get(self._build_result_key(data_item, sql, resolved_timeout))
            if cached_result is not None and cached_result.result_rows is not None:
                initial_execution_time = cached_result.execution_time

        execution_time = measure_execution_time_for_data_item(
            data_item,
            sql,
            timeout=resolved_timeout,
            repeat=repeat,
            initial_execution_time=initial_execution_time,
        )
        if use_cache:
            with self._time_lock:
                self._time_cache.set(cache_key, execution_time)
        return execution_time

    def hash_result(self, data_item: Any, result_rows: Any) -> Any:
        return get_execution_result_hash(data_item, result_rows)

    def reset(self) -> None:
        with self._result_lock:
            self._result_cache.clear()
        with self._time_lock:
            self._time_cache.clear()

    def _resolve_timeout(self, timeout: Optional[int]) -> int:
        return timeout if timeout is not None else self._default_timeout

    @staticmethod
    def _build_result_key(data_item: Any, sql: str, timeout: int) -> tuple[Any, ...]:
        return (
            getattr(data_item, "db_type", "sqlite"),
            data_item.database_path,
            sql,
            timeout,
        )

    @staticmethod
    def _build_time_key(data_item: Any, sql: str, timeout: int, repeat: int) -> tuple[Any, ...]:
        return (
            getattr(data_item, "db_type", "sqlite"),
            data_item.database_path,
            sql,
            timeout,
            repeat,
        )


_execution_service: ExecutionService | None = None


def configure_execution_service(
    *,
    default_timeout: int = DEFAULT_SQL_EXECUTION_TIMEOUT,
    bigquery_credential_path: Optional[str] = None,
    snowflake_credential_path: Optional[str] = None,
    result_cache_size: int = DEFAULT_EXECUTION_RESULT_CACHE_SIZE,
    time_cache_size: int = DEFAULT_EXECUTION_TIME_CACHE_SIZE,
) -> ExecutionService:
    global _execution_service
    _execution_service = ExecutionService(
        default_timeout=default_timeout,
        bigquery_credential_path=bigquery_credential_path,
        snowflake_credential_path=snowflake_credential_path,
        result_cache_size=result_cache_size,
        time_cache_size=time_cache_size,
    )
    return _execution_service


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
