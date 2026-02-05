"""
Spider2 dataset classes for Spider2-Lite and Spider2-Snow datasets.
"""

from app.config import DatasetConfig
from app.db_utils.cloud_schema import (
    load_cloud_database_schema_dict, 
    load_external_knowledge,
    load_snowflake_database_schema_for_spider2_snow
)
from app.logger import logger
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict, Literal
from pathlib import Path
import json
from tqdm import tqdm

from .dataset import DataItem, BaseDataset


class Spider2DataItem(DataItem):
    """
    Extended DataItem for Spider2 datasets.
    Adds Spider2-specific fields while maintaining compatibility with base DataItem.
    """
    # Spider2-specific fields
    instance_id: str = Field(..., description="Instance ID (e.g., 'bq011', 'sf_bq011', 'local001')")
    db_type: Literal["snowflake", "bigquery", "sqlite"] = Field(..., description="Database type")
    external_knowledge_path: Optional[str] = Field(default=None, description="Path to external knowledge document")
    
    # Override default values for base fields
    # gold_sql is not available during inference for Spider2
    gold_sql: str = Field(default="", description="Gold SQL (loaded separately for evaluation)")
    difficulty: str = Field(default="", description="Difficulty level (not available in Spider2)")


def get_db_type_from_instance_id(instance_id: str, dataset_split: str = "lite") -> str:
    """
    Infer database type from instance_id prefix.
    
    Args:
        instance_id: Instance ID string.
        dataset_split: Dataset split ("lite" or "snow").
        
    Returns:
        Database type string: "snowflake", "bigquery", or "sqlite".
        
    Raises:
        ValueError: If instance_id has an unknown prefix.
    """
    if dataset_split == "snow":
        # All Spider2-Snow databases are Snowflake
        return "snowflake"
    
    # Spider2-Lite: check prefix
    if instance_id.startswith("local"):
        return "sqlite"
    elif instance_id.startswith("bq") or instance_id.startswith("ga"):
        return "bigquery"
    elif instance_id.startswith("sf"):
        return "snowflake"
    else:
        raise ValueError(f"Unknown instance_id prefix: {instance_id}. Expected prefixes: 'local', 'bq', 'ga', or 'sf'.")


class Spider2LiteDataset(BaseDataset):
    """
    Spider2-Lite dataset class.
    Contains Snowflake, BigQuery, and SQLite databases.
    """
    
    _name = "spider2"
    _split = "lite"
    
    def __init__(self, dataset_config: DatasetConfig):
        self._config = dataset_config
        self._database_schema_cache: Dict[str, Any] = {}
        self._data = self._load_data()
    
    def _get_resource_dir(self) -> Path:
        """Get the resource directory path."""
        return Path(self._config.root_path) / "resource"
    
    def _load_database_schema(self, db_id: str, db_type: str) -> Dict[str, Any]:
        """Load database schema with caching."""
        cache_key = f"{db_type}:{db_id}"
        if cache_key in self._database_schema_cache:
            return self._database_schema_cache[cache_key]
        
        resource_dir = self._get_resource_dir()
        schema = load_cloud_database_schema_dict(db_id, db_type, str(resource_dir))
        self._database_schema_cache[cache_key] = schema
        return schema
    
    def _get_database_path(self, database_id: str, db_type: str = None) -> str:
        """
        Get database path/identifier.
        
        For SQLite: returns actual file path.
        For cloud databases: returns a logical identifier.
        """
        if db_type == "sqlite":
            return str(self._get_resource_dir() / "databases" / "spider2-localdb" / f"{database_id}.sqlite")
        else:
            # For cloud databases, return the db_id as identifier
            return database_id
    
    def _load_data(self) -> List[Spider2DataItem]:
        """Load Spider2-Lite dataset from JSONL file."""
        data_path = Path(self._config.root_path) / "spider2-lite.jsonl"
        
        if not data_path.exists():
            raise FileNotFoundError(f"Spider2-Lite data file not found: {data_path}")
        
        # Load all items from JSONL
        data_list = []
        with open(data_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    data_list.append(json.loads(line))
        
        if self._config.max_samples is not None:
            data_list = data_list[:self._config.max_samples]
        
        data = []
        resource_dir = self._get_resource_dir()
        db_sample_count = {}  # Track samples per database
        idx = 0
        
        for item in tqdm(data_list, desc="Loading Spider2-Lite data", total=len(data_list)):
            instance_id = item.get("instance_id", "")
            db_id = item.get("db", "")
            question = item.get("question", "")
            external_knowledge_file = item.get("external_knowledge")
            
            # Determine database type
            db_type = get_db_type_from_instance_id(instance_id, "lite")
            
            # Check if we've reached the max samples per database limit
            if self._config.max_samples_per_db is not None:
                if db_sample_count.get(db_id, 0) >= self._config.max_samples_per_db:
                    continue  # Skip this sample
            
            # Load external knowledge as evidence
            evidence = ""
            if external_knowledge_file:
                evidence = load_external_knowledge(external_knowledge_file, resource_dir)
            
            # Load database schema
            try:
                database_schema = self._load_database_schema(db_id, db_type)
            except Exception as e:
                logger.warning(f"Failed to load schema for {db_id} ({db_type}): {e}")
                database_schema = {"db_id": db_id, "db_type": db_type, "db_path": "", "tables": {}}
            
            # Get database path
            database_path = self._get_database_path(db_id, db_type)
            
            data_item = Spider2DataItem(
                question_id=idx,
                instance_id=instance_id,
                question=question,
                evidence=evidence,
                gold_sql="",  # Not available during inference
                difficulty="",
                database_id=db_id,
                database_path=database_path,
                database_schema=database_schema,
                db_type=db_type,
                external_knowledge_path=external_knowledge_file
            )
            data.append(data_item)
            
            # Increment the count for this database
            db_sample_count[db_id] = db_sample_count.get(db_id, 0) + 1
            idx += 1
        
        return data


class Spider2SnowDataset(BaseDataset):
    """
    Spider2-Snow dataset class.
    All databases are Snowflake.
    """
    
    _name = "spider2"
    _split = "snow"
    
    def __init__(self, dataset_config: DatasetConfig):
        self._config = dataset_config
        self._database_schema_cache: Dict[str, Any] = {}
        self._data = self._load_data()
    
    def _get_resource_dir(self) -> Path:
        """Get the resource directory path."""
        return Path(self._config.root_path) / "resource"
    
    def _load_database_schema(self, db_id: str) -> Dict[str, Any]:
        """Load Snowflake database schema with caching."""
        if db_id in self._database_schema_cache:
            return self._database_schema_cache[db_id]
        
        resource_dir = self._get_resource_dir()
        # Spider2-Snow has databases directly under databases/, not databases/snowflake/
        schema = load_snowflake_database_schema_for_spider2_snow(db_id, resource_dir / "databases")
        self._database_schema_cache[db_id] = schema
        return schema
    
    def _get_database_path(self, database_id: str) -> str:
        """Get database identifier (Snowflake doesn't have local paths)."""
        return database_id
    
    def _load_data(self) -> List[Spider2DataItem]:
        """Load Spider2-Snow dataset from JSONL file."""
        data_path = Path(self._config.root_path) / "spider2-snow.jsonl"
        
        if not data_path.exists():
            raise FileNotFoundError(f"Spider2-Snow data file not found: {data_path}")
        
        # Load all items from JSONL
        data_list = []
        with open(data_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    data_list.append(json.loads(line))
        
        if self._config.max_samples is not None:
            data_list = data_list[:self._config.max_samples]
        
        data = []
        resource_dir = self._get_resource_dir()
        db_sample_count = {}  # Track samples per database
        idx = 0
        
        for item in tqdm(data_list, desc="Loading Spider2-Snow data", total=len(data_list)):
            instance_id = item.get("instance_id", "")
            # Spider2-Snow uses "db_id" instead of "db", and it's uppercase
            db_id = item.get("db_id", "")
            # Spider2-Snow uses "instruction" instead of "question"
            question = item.get("instruction", "")
            external_knowledge_file = item.get("external_knowledge")
            
            # All Spider2-Snow databases are Snowflake
            db_type = "snowflake"
            
            # Check if we've reached the max samples per database limit
            if self._config.max_samples_per_db is not None:
                if db_sample_count.get(db_id, 0) >= self._config.max_samples_per_db:
                    continue  # Skip this sample
            
            # Load external knowledge as evidence
            evidence = ""
            if external_knowledge_file:
                evidence = load_external_knowledge(external_knowledge_file, resource_dir)
            
            # Load database schema
            try:
                database_schema = self._load_database_schema(db_id)
            except Exception as e:
                logger.warning(f"Failed to load schema for {db_id}: {e}")
                database_schema = {"db_id": db_id, "db_type": db_type, "db_path": "", "tables": {}}
            
            # Get database path (identifier for Snowflake)
            database_path = self._get_database_path(db_id)
            
            data_item = Spider2DataItem(
                question_id=idx,
                instance_id=instance_id,
                question=question,
                evidence=evidence,
                gold_sql="",  # Not available during inference
                difficulty="",
                database_id=db_id,
                database_path=database_path,
                database_schema=database_schema,
                db_type=db_type,
                external_knowledge_path=external_knowledge_file
            )
            data.append(data_item)
            
            # Increment the count for this database
            db_sample_count[db_id] = db_sample_count.get(db_id, 0) + 1
            idx += 1
        
        return data
