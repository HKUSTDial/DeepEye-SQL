"""
Cloud database schema loading utilities for Spider2 datasets.
Loads schema from pre-extracted JSON files for Snowflake and BigQuery databases.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import json
from functools import lru_cache
from app.logger import logger


def _get_value_examples_from_sample_rows(sample_rows: List[Dict], column_name: str, max_examples: int = 3, max_length: int = 100) -> List[str]:
    """
    Extract value examples from sample rows.
    
    Args:
        sample_rows: List of sample row dictionaries.
        column_name: Name of the column to extract examples from.
        max_examples: Maximum number of examples to return.
        max_length: Maximum length of string representation for each value.
        
    Returns:
        List of string representations of example values.
    """
    if not sample_rows:
        return []
    
    from app.config import config
    if config.dataset_config is not None:
        max_length = config.dataset_config.max_value_example_length
    
    examples = []
    for row in sample_rows:
        if column_name not in row:
            continue
            
        value = row[column_name]
        
        # Convert value to string representation
        if value is None:
            str_value = "NULL"
        elif isinstance(value, (dict, list)):
            # Convert complex types to JSON string
            str_value = json.dumps(value, ensure_ascii=False, default=str)
        else:
            str_value = str(value)
        
        # Check length constraint
        if len(str_value) <= max_length and str_value not in examples:
            examples.append(str_value)
            if len(examples) >= max_examples:
                break
                
    return examples


def load_table_schema_from_json(json_path: Path) -> Dict[str, Any]:
    """
    Load a single table schema from a JSON file.
    
    Args:
        json_path: Path to the table JSON file.
        
    Returns:
        Table schema dictionary in the standard format.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        table_data = json.load(f)
    
    table_name = table_data.get("table_name", "").strip()
    table_fullname = table_data.get("table_fullname", "").strip()
    # Normalize table_fullname by stripping spaces around dots
    if "." in table_fullname:
        table_fullname = ".".join([part.strip() for part in table_fullname.split(".")])
    
    column_names = [name.strip() for name in table_data.get("column_names", [])]
    # Normalize column names by stripping spaces around dots (for BigQuery nested fields)
    column_names = [".".join([p.strip() for p in name.split(".")]) if "." in name else name for name in column_names]
    
    column_types = [t.strip() if isinstance(t, str) else t for t in table_data.get("column_types", [])]
    descriptions = table_data.get("description", [])
    sample_rows = table_data.get("sample_rows", [])
    
    # Handle nested columns for BigQuery
    nested_column_names = [name.strip() for name in table_data.get("nested_column_names", [])]
    # Normalize nested column names by stripping spaces around dots
    nested_column_names = [".".join([p.strip() for p in name.split(".")]) if "." in name else name for name in nested_column_names]
    
    nested_column_types = [t.strip() if isinstance(t, str) else t for t in table_data.get("nested_column_types", [])]
    
    # Build table schema dict
    table_schema_dict = {
        "table_name": table_name,
        "table_fullname": table_fullname,
        "columns": {}
    }
    
    # Process columns
    for i, column_name in enumerate(column_names):
        column_type = column_types[i] if i < len(column_types) else "UNKNOWN"
        description = descriptions[i] if i < len(descriptions) and descriptions[i] else ""
        
        column_schema_dict = {
            "column_name": column_name,
            "column_type": column_type,
            "primary_key": False,  # Cloud databases don't provide PK info in JSON
            "foreign_keys": [],    # Cloud databases don't provide FK info in JSON
            "description": description,
            "value_examples": _get_value_examples_from_sample_rows(sample_rows, column_name),
            "value_statistics": None  # Not available for cloud databases
        }
        
        table_schema_dict["columns"][column_name] = column_schema_dict
    
    # Process nested columns for BigQuery
    if nested_column_names and nested_column_types:
        nested_columns = {}
        for i, nested_col_name in enumerate(nested_column_names):
            # Skip if it's a top-level column (already processed)
            if nested_col_name in column_names:
                continue
            nested_col_type = nested_column_types[i] if i < len(nested_column_types) else "UNKNOWN"
            nested_columns[nested_col_name] = {
                "column_name": nested_col_name,
                "column_type": nested_col_type
            }
        if nested_columns:
            table_schema_dict["nested_columns"] = nested_columns
    
    return table_schema_dict


def load_snowflake_database_schema(db_id: str, snowflake_db_dir: Path) -> Dict[str, Any]:
    """
    Load Snowflake database schema from JSON files.
    
    Directory structure: snowflake/{DATABASE}/{SCHEMA}/{TABLE}.json
    
    Args:
        db_id: Database ID (e.g., "STACKOVERFLOW")
        snowflake_db_dir: Path to snowflake databases directory
        
    Returns:
        Database schema dictionary.
    """
    # Find the database directory (case-insensitive search)
    db_dir = None
    for d in snowflake_db_dir.iterdir():
        if d.is_dir() and d.name.upper() == db_id.upper():
            db_dir = d
            break
    
    if db_dir is None:
        raise ValueError(f"Snowflake database directory not found: {db_id}")
    
    database_schema_dict = {
        "db_id": db_id,
        "db_path": str(db_dir),
        "db_type": "snowflake",
        "tables": {}
    }
    
    # Iterate through all schema directories and table JSON files
    for schema_dir in db_dir.iterdir():
        if not schema_dir.is_dir():
            continue
        for json_file in schema_dir.glob("*.json"):
            try:
                table_schema = load_table_schema_from_json(json_file)
                table_name = table_schema["table_name"]
                # Use fullname as key to avoid conflicts
                table_key = table_schema.get("table_fullname", table_name)
                database_schema_dict["tables"][table_key] = table_schema
            except Exception as e:
                logger.warning(f"Failed to load table schema from {json_file}: {e}")
    
    return database_schema_dict


def load_snowflake_database_schema_for_spider2_snow(db_id: str, databases_dir: Path) -> Dict[str, Any]:
    """
    Load Snowflake database schema from JSON files for Spider2-Snow dataset.
    
    Spider2-Snow has a different directory structure:
    databases/{DATABASE}/{SCHEMA}/{TABLE}.json (no 'snowflake' subdirectory)
    
    Args:
        db_id: Database ID (e.g., "GA4", "GA360")
        databases_dir: Path to databases directory
        
    Returns:
        Database schema dictionary.
    """
    # Find the database directory (case-insensitive search)
    db_dir = None
    for d in databases_dir.iterdir():
        if d.is_dir() and d.name.upper() == db_id.upper():
            db_dir = d
            break
    
    if db_dir is None:
        raise ValueError(f"Spider2-Snow database directory not found: {db_id}")
    
    database_schema_dict = {
        "db_id": db_id,
        "db_path": str(db_dir),
        "db_type": "snowflake",
        "tables": {}
    }
    
    # Iterate through all schema directories and table JSON files
    for schema_dir in db_dir.iterdir():
        if not schema_dir.is_dir():
            continue
        for json_file in schema_dir.glob("*.json"):
            try:
                table_schema = load_table_schema_from_json(json_file)
                table_name = table_schema["table_name"]
                # Use fullname as key to avoid conflicts
                table_key = table_schema.get("table_fullname", table_name)
                database_schema_dict["tables"][table_key] = table_schema
            except Exception as e:
                logger.warning(f"Failed to load table schema from {json_file}: {e}")
    
    return database_schema_dict


def load_bigquery_database_schema(db_id: str, bigquery_db_dir: Path) -> Dict[str, Any]:
    """
    Load BigQuery database schema from JSON files.
    
    Directory structure: bigquery/{db_id}/{project.dataset}/{table}.json
    
    Args:
        db_id: Database ID (e.g., "ga4", "ga360")
        bigquery_db_dir: Path to bigquery databases directory
        
    Returns:
        Database schema dictionary.
    """
    # Find the database directory (case-insensitive search)
    db_dir = None
    for d in bigquery_db_dir.iterdir():
        if d.is_dir() and d.name.lower() == db_id.lower():
            db_dir = d
            break
    
    if db_dir is None:
        raise ValueError(f"BigQuery database directory not found: {db_id}")
    
    database_schema_dict = {
        "db_id": db_id,
        "db_path": str(db_dir),
        "db_type": "bigquery",
        "tables": {}
    }
    
    # Iterate through all dataset directories and table JSON files
    for dataset_dir in db_dir.iterdir():
        if not dataset_dir.is_dir():
            continue
        for json_file in dataset_dir.glob("*.json"):
            try:
                table_schema = load_table_schema_from_json(json_file)
                table_name = table_schema["table_name"]
                # Use fullname as key to avoid conflicts
                table_key = table_schema.get("table_fullname", table_name)
                database_schema_dict["tables"][table_key] = table_schema
            except Exception as e:
                logger.warning(f"Failed to load table schema from {json_file}: {e}")
    
    return database_schema_dict


def load_sqlite_database_schema_for_spider2(db_id: str, sqlite_db_dir: Path) -> Dict[str, Any]:
    """
    Load SQLite database schema for Spider2-Lite local databases.
    This delegates to the standard SQLite schema loader.
    
    Args:
        db_id: Database ID (e.g., "local_001")
        sqlite_db_dir: Path to spider2-localdb directory
        
    Returns:
        Database schema dictionary.
    """
    from app.db_utils.schema import load_database_schema_dict
    
    db_path = sqlite_db_dir / f"{db_id}.sqlite"
    if not db_path.exists():
        raise ValueError(f"SQLite database not found: {db_path}")
    
    schema_dict = load_database_schema_dict(str(db_path))
    schema_dict["db_type"] = "sqlite"
    return schema_dict


@lru_cache(maxsize=500)
def load_cloud_database_schema_dict(
    db_id: str,
    db_type: str,
    resource_dir: str
) -> Dict[str, Any]:
    """
    Load database schema for Spider2 datasets.
    
    Args:
        db_id: Database ID
        db_type: Database type ("snowflake", "bigquery", "sqlite")
        resource_dir: Path to resource directory (as string for caching)
        
    Returns:
        Database schema dictionary in standard format.
    """
    resource_path = Path(resource_dir)
    
    if db_type == "snowflake":
        return load_snowflake_database_schema(
            db_id, 
            resource_path / "databases" / "snowflake"
        )
    elif db_type == "bigquery":
        return load_bigquery_database_schema(
            db_id,
            resource_path / "databases" / "bigquery"
        )
    elif db_type == "sqlite":
        return load_sqlite_database_schema_for_spider2(
            db_id,
            resource_path / "databases" / "spider2-localdb"
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def load_external_knowledge(doc_name: Optional[str], resource_dir: Path) -> str:
    """
    Load external knowledge document.
    
    Args:
        doc_name: Document filename (e.g., "ga4_obfuscated_sample_ecommerce.events.md")
        resource_dir: Path to resource directory
        
    Returns:
        Document content as string, or empty string if not found.
    """
    if not doc_name:
        return ""
    
    doc_path = resource_dir / "documents" / doc_name
    if doc_path.exists():
        try:
            return doc_path.read_text(encoding="utf-8")
        except Exception as e:
            logger.warning(f"Failed to load external knowledge from {doc_path}: {e}")
            return ""
    else:
        logger.warning(f"External knowledge document not found: {doc_path}")
        return ""
