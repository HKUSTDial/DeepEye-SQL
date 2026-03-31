import sqlite3
import threading
import time
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path
from typing import Any, List, Literal, Optional, Tuple

import numpy as np
from pydantic import BaseModel, Field, PrivateAttr
from tabulate import tabulate

from .defaults import DEFAULT_SQL_EXECUTION_TIMEOUT


class SQLExecutionResult(BaseModel):
    result_type: Literal["success", "timeout", "empty_result", "all_null_result", "execution_error"] = Field(..., description="The type of the result")
    db_path: str = Field(..., description="The path of the database")
    sql: str = Field(..., description="The sql to be executed")
    execution_time: Optional[float] = Field(default=None, description="Wall-clock execution time in seconds")
    result_cols: Optional[List[str]] = Field(default=None, description="The columns of the result")
    result_rows: Optional[List[Tuple[Any, ...]]] = Field(default=None, description="The rows of the result")
    error_message: Optional[str] = Field(default=None, description="The error message")
    _result_table_str_cache: Optional[str] = PrivateAttr(default=None)

    @property
    def result_table_str(self) -> Optional[str]:
        if self._result_table_str_cache is None:
            self._result_table_str_cache = self._build_result_table_str()
        return self._result_table_str_cache

    def _build_result_table_str(self) -> Optional[str]:
        if self.result_cols is not None and self.result_rows is not None:
            table_rows = []
            for row in self.result_rows[:5]:
                table_row = []
                for val in row:
                    if isinstance(val, str) and len(val) > 100:
                        table_row.append(f"'{val[:100]}...'")
                    else:
                        table_row.append(val)
                table_rows.append(table_row)
            return tabulate(
                tabular_data=table_rows,
                headers=self.result_cols,
                tablefmt="psql",
            )
        return self.error_message


def _resolve_timeout(timeout: Optional[int]) -> int:
    return timeout if timeout is not None else DEFAULT_SQL_EXECUTION_TIMEOUT


DEFAULT_SQLITE_CONNECTION_CACHE_SIZE = 32


_SQLITE_CONNECTION_LOCAL = threading.local()


def _resolve_db_path(db_path: str) -> str:
    return str(Path(db_path).resolve())


def _get_connection_cache() -> OrderedDict[str, sqlite3.Connection]:
    connections = getattr(_SQLITE_CONNECTION_LOCAL, "connections", None)
    if connections is None:
        connections = OrderedDict()
        _SQLITE_CONNECTION_LOCAL.connections = connections
    return connections


def _create_connection(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.text_factory = lambda x: str(x, "utf-8", errors="replace")
    return connection


def _get_readonly_connection(db_path: str) -> sqlite3.Connection:
    cache = _get_connection_cache()
    connection = cache.pop(db_path, None)
    if connection is None:
        connection = _create_connection(db_path)
    cache[db_path] = connection
    while len(cache) > DEFAULT_SQLITE_CONNECTION_CACHE_SIZE:
        _, stale_connection = cache.popitem(last=False)
        try:
            stale_connection.close()
        except sqlite3.Error:
            pass
    return connection


def _clear_cached_connection(db_path: str) -> None:
    cache = _get_connection_cache()
    connection = cache.pop(db_path, None)
    if connection is not None:
        try:
            connection.close()
        except sqlite3.Error:
            pass


def _build_sql_execution_result(
    *,
    db_path: str,
    sql: str,
    execution_time: float,
    result_cols: Optional[List[str]] = None,
    result_rows: Optional[List[Tuple[Any, ...]]] = None,
    error_message: Optional[str] = None,
) -> SQLExecutionResult:
    if result_rows is None:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=db_path,
            sql=sql,
            execution_time=execution_time,
            error_message=error_message,
        )
    if len(result_rows) == 0:
        return SQLExecutionResult(
            result_type="empty_result",
            db_path=db_path,
            sql=sql,
            execution_time=execution_time,
            result_cols=result_cols,
            result_rows=result_rows,
            error_message="The SQL query returned an empty result table.",
        )
    if not any(any(val is not None for val in row) for row in result_rows):
        return SQLExecutionResult(
            result_type="all_null_result",
            db_path=db_path,
            sql=sql,
            execution_time=execution_time,
            result_cols=result_cols,
            result_rows=result_rows,
            error_message="The SQL query returned an result table with all null values.",
        )
    return SQLExecutionResult(
        result_type="success",
        db_path=db_path,
        sql=sql,
        execution_time=execution_time,
        result_cols=result_cols,
        result_rows=result_rows,
    )


def _execute_sql_once(db_path: str, sql: str, timeout: int) -> SQLExecutionResult:
    resolved_db_path = _resolve_db_path(db_path)
    start_time = time.perf_counter()
    deadline = start_time + timeout
    timed_out = False
    cursor = None

    def _run_query(connection: sqlite3.Connection) -> SQLExecutionResult:
        nonlocal cursor, timed_out

        def check_timeout() -> int:
            nonlocal timed_out
            if time.perf_counter() >= deadline:
                timed_out = True
                return 1
            return 0

        connection.set_progress_handler(check_timeout, 1000)
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            result_cols = [d[0] for d in cursor.description] if cursor.description else []
            result_rows = cursor.fetchall()
            return _build_sql_execution_result(
                db_path=resolved_db_path,
                sql=sql,
                execution_time=time.perf_counter() - start_time,
                result_cols=result_cols,
                result_rows=result_rows,
            )
        except sqlite3.OperationalError as exc:
            elapsed_time = time.perf_counter() - start_time
            error_message = str(exc)
            if timed_out or "interrupted" in error_message.lower():
                return SQLExecutionResult(
                    result_type="timeout",
                    db_path=resolved_db_path,
                    sql=sql,
                    execution_time=elapsed_time,
                    error_message=f"SQL execution timed out after {timeout} seconds",
                )
            return SQLExecutionResult(
                result_type="execution_error",
                db_path=resolved_db_path,
                sql=sql,
                execution_time=elapsed_time,
                error_message=error_message,
            )
        except Exception as exc:
            return SQLExecutionResult(
                result_type="execution_error",
                db_path=resolved_db_path,
                sql=sql,
                execution_time=time.perf_counter() - start_time,
                error_message=str(exc),
            )
        finally:
            try:
                connection.set_progress_handler(None, 0)
            except sqlite3.Error:
                pass
            if cursor is not None:
                cursor.close()

    connection = _get_readonly_connection(resolved_db_path)
    try:
        return _run_query(connection)
    except sqlite3.ProgrammingError:
        _clear_cached_connection(resolved_db_path)
        connection = _get_readonly_connection(resolved_db_path)
        return _run_query(connection)


@lru_cache(maxsize=1000)
def _execute_sql_cached(db_path: str, sql: str, timeout: int) -> SQLExecutionResult:
    return _execute_sql_once(db_path, sql, timeout)


def execute_sql(db_path: str, sql: str, timeout: Optional[int] = None) -> SQLExecutionResult:
    return _execute_sql_cached(_resolve_db_path(str(db_path)), sql, _resolve_timeout(timeout))


def execute_sql_without_cache(db_path: str, sql: str, timeout: Optional[int] = None) -> SQLExecutionResult:
    return _execute_sql_once(_resolve_db_path(str(db_path)), sql, _resolve_timeout(timeout))


def measure_execution_time(
    db_path: str,
    sql: str,
    timeout: Optional[int] = None,
    repeat: int = 10,
    initial_execution_time: Optional[float] = None,
) -> float:
    """
    Measure SQL execution time for SQLite databases.

    Args:
        db_path: Path to SQLite database.
        sql: SQL query to execute.
        timeout: Query timeout in seconds.
        repeat: Number of times to repeat execution for averaging.

    Returns:
        Average execution time in seconds, or np.inf if execution fails.
    """
    resolved_timeout = _resolve_timeout(timeout)
    execution_times = []
    if initial_execution_time is not None and np.isfinite(initial_execution_time):
        execution_times.append(float(initial_execution_time))
    for _ in range(max(repeat - len(execution_times), 0)):
        execution_result = execute_sql_without_cache(db_path, sql, resolved_timeout)
        if execution_result.result_rows is not None and execution_result.execution_time is not None:
            execution_times.append(float(execution_result.execution_time))
    if len(execution_times) == 0:
        return np.inf
    if len(execution_times) == 1:
        return float(execution_times[0])
    std = np.std(execution_times)
    mean = np.mean(execution_times)
    if std == 0:
        return float(mean)
    filtered_times = [t for t in execution_times if abs(t - mean) <= 3 * std]
    if len(filtered_times) == 0:
        return float(mean)
    return float(np.mean(filtered_times))


def measure_execution_time_for_data_item(
    data_item,
    sql: str,
    timeout: Optional[int] = None,
    repeat: int = 10,
    initial_execution_time: Optional[float] = None,
) -> float:
    """
    Measure SQL execution time based on the data item's database type.

    For SQLite databases, measures actual execution time.
    For cloud databases (BigQuery/Snowflake), returns np.inf as execution time
    measurement is not supported (and would be costly).

    Args:
        data_item: DataItem or Spider2DataItem with database information.
        sql: SQL query to execute.
        timeout: Query timeout in seconds.
        repeat: Number of times to repeat execution for averaging.

    Returns:
        Average execution time in seconds, or np.inf for cloud databases or if execution fails.
    """
    resolved_timeout = _resolve_timeout(timeout)
    db_type = getattr(data_item, "db_type", None)

    if db_type is not None and db_type in ("bigquery", "snowflake"):
        return np.inf

    return measure_execution_time(
        data_item.database_path,
        sql,
        resolved_timeout,
        repeat,
        initial_execution_time=initial_execution_time,
    )


def execute_sql_for_data_item(
    data_item,
    sql: str,
    timeout: Optional[int] = None,
    *,
    bigquery_credential_path: Optional[str] = None,
    snowflake_credential_path: Optional[str] = None,
) -> SQLExecutionResult:
    """
    Execute SQL based on the data item's database type.
    Automatically handles SQLite, BigQuery, and Snowflake databases.

    Args:
        data_item: DataItem or Spider2DataItem with database information.
        sql: SQL query to execute.
        timeout: Query timeout in seconds.
        bigquery_credential_path: Optional BigQuery credential override.
        snowflake_credential_path: Optional Snowflake credential override.

    Returns:
        SQLExecutionResult with query results.
    """
    resolved_timeout = _resolve_timeout(timeout)
    db_type = getattr(data_item, "db_type", None)

    if db_type is None or db_type == "sqlite":
        return execute_sql(data_item.database_path, sql, timeout=resolved_timeout)

    from .cloud_execution import execute_cloud_sql

    credential_path = None
    if db_type == "bigquery":
        credential_path = bigquery_credential_path
    elif db_type == "snowflake":
        credential_path = snowflake_credential_path

    return execute_cloud_sql(sql, db_type, data_item.database_path, credential_path, resolved_timeout)
