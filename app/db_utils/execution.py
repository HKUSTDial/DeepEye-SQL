import sqlite3
import threading
import time
from functools import lru_cache
from typing import Any, List, Literal, Optional, Tuple

import numpy as np
from pydantic import BaseModel, Field
from tabulate import tabulate

from .defaults import DEFAULT_SQL_EXECUTION_TIMEOUT


class SQLExecutionResult(BaseModel):
    result_type: Literal["success", "timeout", "empty_result", "all_null_result", "execution_error"] = Field(..., description="The type of the result")
    db_path: str = Field(..., description="The path of the database")
    sql: str = Field(..., description="The sql to be executed")
    result_cols: Optional[List[str]] = Field(default=None, description="The columns of the result")
    result_rows: Optional[List[Tuple[Any, ...]]] = Field(default=None, description="The rows of the result")
    result_table_str: Optional[str] = Field(default=None, description="The table string of the result")
    error_message: Optional[str] = Field(default=None, description="The error message")

    def model_post_init(self, __context: Any) -> None:
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
            self.result_table_str = tabulate(
                tabular_data=table_rows,
                headers=self.result_cols,
                tablefmt="psql",
            )
        else:
            self.result_table_str = self.error_message


def _resolve_timeout(timeout: Optional[int]) -> int:
    return timeout if timeout is not None else DEFAULT_SQL_EXECUTION_TIMEOUT


class SQLExecutionThread(threading.Thread):
    def __init__(self, db_path: str, sql: str, timeout: Optional[int] = None):
        super().__init__()
        self.db_path = db_path
        self.sql = sql
        self.timeout = _resolve_timeout(timeout)
        self.result_rows = None
        self.result_cols = None
        self.exception = None
        self.timeout_event = threading.Event()

    def run(self):
        def check_timeout():
            if self.timeout_event.is_set():
                raise TimeoutError(f"SQL execution timed out after {self.timeout} seconds")

        try:
            with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True) as conn:
                conn.text_factory = lambda x: str(x, "utf-8", errors="replace")
                conn.set_progress_handler(check_timeout, 1000)
                cursor = conn.cursor()
                cursor.execute(self.sql)
                self.result_cols = [d[0] for d in cursor.description] if cursor.description else []
                self.result_rows = cursor.fetchall()
        except Exception as e:
            self.exception = e


def _execute_sql_once(db_path: str, sql: str, timeout: int) -> SQLExecutionResult:
    thread = SQLExecutionThread(db_path, sql, timeout)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        thread.timeout_event.set()
        thread.join(1)
        return SQLExecutionResult(
            result_type="timeout",
            db_path=str(db_path),
            sql=sql,
            error_message=f"SQL execution timed out after {timeout} seconds",
        )
    if thread.exception:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=str(db_path),
            sql=sql,
            error_message=str(thread.exception),
        )
    if thread.result_rows is not None and len(thread.result_rows) == 0:
        return SQLExecutionResult(
            result_type="empty_result",
            db_path=str(db_path),
            sql=sql,
            result_cols=thread.result_cols,
            result_rows=thread.result_rows,
            error_message="The SQL query returned an empty result table.",
        )
    if thread.result_rows is not None and not any(any(val is not None for val in row) for row in thread.result_rows):
        return SQLExecutionResult(
            result_type="all_null_result",
            db_path=str(db_path),
            sql=sql,
            result_cols=thread.result_cols,
            result_rows=thread.result_rows,
            error_message="The SQL query returned an result table with all null values.",
        )
    return SQLExecutionResult(
        result_type="success",
        db_path=str(db_path),
        sql=sql,
        result_cols=thread.result_cols,
        result_rows=thread.result_rows,
    )


@lru_cache(maxsize=1000)
def _execute_sql_cached(db_path: str, sql: str, timeout: int) -> SQLExecutionResult:
    return _execute_sql_once(db_path, sql, timeout)


def execute_sql(db_path: str, sql: str, timeout: Optional[int] = None) -> SQLExecutionResult:
    return _execute_sql_cached(str(db_path), sql, _resolve_timeout(timeout))


def execute_sql_without_cache(db_path: str, sql: str, timeout: Optional[int] = None) -> SQLExecutionResult:
    return _execute_sql_once(str(db_path), sql, _resolve_timeout(timeout))


def measure_execution_time(db_path: str, sql: str, timeout: Optional[int] = None, repeat: int = 10) -> float:
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
    for _ in range(repeat):
        start_time = time.time()
        execution_result = execute_sql_without_cache(db_path, sql, resolved_timeout)
        if execution_result.result_rows is not None:
            end_time = time.time()
            execution_times.append(end_time - start_time)
    if len(execution_times) == 0:
        return np.inf
    std = np.std(execution_times)
    mean = np.mean(execution_times)
    execution_times = [t for t in execution_times if t > mean - 3 * std and t < mean + 3 * std]
    return float(np.mean(execution_times))


def measure_execution_time_for_data_item(data_item, sql: str, timeout: Optional[int] = None, repeat: int = 10) -> float:
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

    return measure_execution_time(data_item.database_path, sql, resolved_timeout, repeat)


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
