"""
Cloud SQL execution utilities for Spider2 datasets.
Executes SQL on Snowflake and BigQuery cloud databases.
"""

import os
import json
from pathlib import Path
from typing import Optional, List, Tuple, Any, Dict
import pandas as pd
from app.logger import logger
from app.config import config
from .execution import SQLExecutionResult
    

# Global cache for BigQuery clients to avoid repeated creation in multi-threaded environments
_bq_clients: Dict[str, Any] = {}

def _get_bigquery_client(credential_path: Optional[str] = None):
    """Get or create a thread-safe BigQuery client."""
    from google.cloud import bigquery
    
    cache_key = credential_path or "default"
    if cache_key not in _bq_clients:
        if credential_path:
            _bq_clients[cache_key] = bigquery.Client.from_service_account_json(credential_path)
        else:
            _bq_clients[cache_key] = bigquery.Client()
    return _bq_clients[cache_key]


def execute_bigquery_sql(
    sql: str,
    db_path: str,
    credential_path: Optional[str] = None,
    timeout: Optional[int] = None
) -> SQLExecutionResult:
    """
    Execute SQL on BigQuery.
    
    Args:
        sql: SQL query to execute.
        db_path: Database identifier (for result tracking).
        credential_path: Path to BigQuery credential JSON file.
        timeout: Query timeout in seconds.
        
    Returns:
        SQLExecutionResult with query results.
    """
    if timeout is None:
        timeout = config.dataset_config.sql_execution_timeout
    try:
        from google.cloud import bigquery
    except ImportError:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=db_path,
            sql=sql,
            error_message="google-cloud-bigquery package is not installed. Run: pip install google-cloud-bigquery"
        )
    
    try:
        # Get thread-safe client from cache
        client = _get_bigquery_client(credential_path)
        
        # Configure job
        job_config = bigquery.QueryJobConfig(
            job_timeout_ms=timeout * 1000
        )
        
        # Execute query
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result(timeout=timeout)
        
        # Convert to dataframe
        df = results.to_dataframe()
        
        if df.empty:
            return SQLExecutionResult(
                result_type="empty_result",
                db_path=db_path,
                sql=sql,
                result_cols=list(df.columns) if len(df.columns) > 0 else [],
                result_rows=[],
                error_message="The SQL query returned an empty result table."
            )
        
        # Convert to list of tuples
        result_rows = [tuple(row) for row in df.values]
        result_cols = list(df.columns)
        
        return SQLExecutionResult(
            result_type="success",
            db_path=db_path,
            sql=sql,
            result_cols=result_cols,
            result_rows=result_rows
        )
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"BigQuery execution error: {error_message}")
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=db_path,
            sql=sql,
            error_message=error_message
        )


def execute_snowflake_sql(
    sql: str,
    db_path: str,
    credential_path: Optional[str] = None,
    timeout: Optional[int] = None
) -> SQLExecutionResult:
    """
    Execute SQL on Snowflake.
    
    Args:
        sql: SQL query to execute.
        db_path: Database identifier (for result tracking).
        credential_path: Path to Snowflake credential JSON file.
        timeout: Query timeout in seconds.
        
    Returns:
        SQLExecutionResult with query results.
    """
    if timeout is None:
        timeout = config.dataset_config.sql_execution_timeout
    try:
        import snowflake.connector
    except ImportError:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=db_path,
            sql=sql,
            error_message="snowflake-connector-python package is not installed. Run: pip install snowflake-connector-python"
        )
    
    conn = None
    cursor = None
    
    try:
        # Load credentials
        if credential_path is None:
            return SQLExecutionResult(
                result_type="execution_error",
                db_path=db_path,
                sql=sql,
                error_message="Snowflake credential path is required"
            )
        
        with open(credential_path, "r") as f:
            credentials = json.load(f)
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**credentials)
        cursor = conn.cursor()
        
        # Set query timeout
        cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
        
        # Execute query
        cursor.execute(sql)
        results = cursor.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        if not results:
            return SQLExecutionResult(
                result_type="empty_result",
                db_path=db_path,
                sql=sql,
                result_cols=columns,
                result_rows=[],
                error_message="The SQL query returned an empty result table."
            )
        
        # Convert results to tuples
        result_rows = [tuple(row) for row in results]
        
        return SQLExecutionResult(
            result_type="success",
            db_path=db_path,
            sql=sql,
            result_cols=columns,
            result_rows=result_rows
        )
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Snowflake execution error: {error_message}")
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=db_path,
            sql=sql,
            error_message=error_message
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def execute_cloud_sql(
    sql: str,
    db_type: str,
    db_path: str,
    credential_path: Optional[str] = None,
    timeout: Optional[int] = None
) -> SQLExecutionResult:
    """
    Execute SQL on cloud database.
    
    Args:
        sql: SQL query to execute.
        db_type: Database type ("bigquery" or "snowflake").
        db_path: Database identifier (for result tracking).
        credential_path: Path to credential JSON file.
        timeout: Query timeout in seconds.
        
    Returns:
        SQLExecutionResult with query results.
    """
    if timeout is None:
        timeout = config.dataset_config.sql_execution_timeout
    if db_type == "bigquery":
        return execute_bigquery_sql(sql, db_path, credential_path, timeout)
    elif db_type == "snowflake":
        return execute_snowflake_sql(sql, db_path, credential_path, timeout)
    else:
        return SQLExecutionResult(
            result_type="execution_error",
            db_path=db_path,
            sql=sql,
            error_message=f"Unsupported cloud database type: {db_type}"
        )


def execute_sql_for_spider2(
    sql: str,
    db_type: str,
    db_path: str,
    credential_path: Optional[str] = None,
    timeout: Optional[int] = None
) -> SQLExecutionResult:
    """
    Execute SQL for Spider2 datasets. Handles both local SQLite and cloud databases.
    
    Args:
        sql: SQL query to execute.
        db_type: Database type ("sqlite", "bigquery", or "snowflake").
        db_path: Path to SQLite database or database identifier for cloud.
        credential_path: Path to credential JSON file (required for cloud types).
        timeout: Query timeout in seconds.
        
    Returns:
        SQLExecutionResult with query results.
    """
    if timeout is None:
        timeout = config.dataset_config.sql_execution_timeout
    if db_type == "sqlite":
        # Use existing SQLite execution
        from app.db_utils.execution import execute_sql
        return execute_sql(db_path, sql, timeout=timeout)
    else:
        return execute_cloud_sql(sql, db_type, db_path, credential_path, timeout)
