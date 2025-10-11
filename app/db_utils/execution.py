import sqlite3
from functools import lru_cache
from tabulate import tabulate
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal, Optional, Tuple
import threading
import time
import numpy as np
from collections import Counter
from app.logger import logger


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
                tablefmt="psql"
            )
        else:
            self.result_table_str = self.error_message


class SQLExecutionThread(threading.Thread):
    def __init__(self, db_path: str, sql: str, timeout: int = 30):
        super().__init__()
        self.db_path = db_path
        self.sql = sql
        self.timeout = timeout
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
                self.result_cols = [d[0] for d in cursor.description]
                self.result_rows = cursor.fetchall()
        except Exception as e:
            self.exception = e


@lru_cache(maxsize=1000)
def execute_sql(db_path: str, sql: str, timeout: int = 30) -> SQLExecutionResult:
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
            error_message=f"SQL execution timed out after {timeout} seconds"
        )
    if thread.exception:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=str(db_path),
            sql=sql,
            error_message=str(thread.exception)
        )
    if thread.result_rows is not None and len(thread.result_rows) == 0:
        return SQLExecutionResult(
            result_type="empty_result",
            db_path=str(db_path),
            sql=sql,
            result_cols=thread.result_cols,
            result_rows=thread.result_rows,
            error_message="The SQL query returned an empty result table."
        )
    if thread.result_rows is not None and not any(any(val is not None for val in row) for row in thread.result_rows):
        return SQLExecutionResult(
            result_type="all_null_result",
            db_path=str(db_path),
            sql=sql,
            result_cols=thread.result_cols,
            result_rows=thread.result_rows,
            error_message="The SQL query returned an result table with all null values."
        )
    return SQLExecutionResult(
        result_type="success",
        db_path=str(db_path),
        sql=sql,
        result_cols=thread.result_cols,
        result_rows=thread.result_rows
    )
    

def execute_sql_without_cache(db_path: str, sql: str, timeout: int = 30) -> SQLExecutionResult:
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
            error_message=f"SQL execution timed out after {timeout} seconds"
        )
    if thread.exception:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=str(db_path),
            sql=sql,
            error_message=str(thread.exception)
        )
    if thread.result_rows is not None and len(thread.result_rows) == 0:
        return SQLExecutionResult(
            result_type="empty_result",
            db_path=str(db_path),
            sql=sql,
            result_cols=thread.result_cols,
            result_rows=thread.result_rows,
            error_message="The SQL query returned an empty result table."
        )
    if thread.result_rows is not None and not any(any(val is not None for val in row) for row in thread.result_rows):
        return SQLExecutionResult(
            result_type="all_null_result",
            db_path=str(db_path),
            sql=sql,
            result_cols=thread.result_cols,
            result_rows=thread.result_rows,
            error_message="The SQL query returned an result table with all null values."
        )
    return SQLExecutionResult(
        result_type="success",
        db_path=str(db_path),
        sql=sql,
        result_cols=thread.result_cols,
        result_rows=thread.result_rows
    )
    

def measure_execution_time(db_path: str, sql: str, timeout: int = 30, repeat: int = 10) -> float:
    execution_times = []
    for _ in range(repeat):
        start_time = time.time()
        execution_result = execute_sql_without_cache(db_path, sql, timeout)
        if execution_result.result_rows is not None:
            end_time = time.time()
            execution_times.append(end_time - start_time)
    if len(execution_times) == 0:
        return np.inf
    std = np.std(execution_times)
    mean = np.mean(execution_times)
    # exclude outliers
    execution_times = [t for t in execution_times if t > mean - 3 * std and t < mean + 3 * std]
    return float(np.mean(execution_times))
