from app.config import config, DatasetConfig
from app.db_utils import load_database_schema_dict
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Any, Dict
from pathlib import Path
import json
import pickle
from abc import ABC, abstractmethod
from tqdm import tqdm


class DataItem(BaseModel):
    question_id: int = Field(..., description="The question id of the data item")
    question: str = Field(..., description="The question of the data item")
    evidence: str = Field(default="", description="The evidence of the data item")
    gold_sql: str = Field(..., description="The gold sql of the data item")
    difficulty: str = Field(default="", description="The difficulty of the data item")
    database_id: str = Field(..., description="The database id of the data item")
    database_path: str = Field(..., description="The database path of the data item")
    database_schema: Dict[str, Any] = Field(..., description="The database schema of the data item")
    
    # Value Retrieval Step
    question_keywords: Optional[List[str]] = Field(default=None, description="The question keywords of the data item")
    retrieved_values: Optional[Dict[str, Dict[str, Any]]] = Field(default=None, description="The retrieved values of the data item")
    database_schema_after_value_retrieval: Optional[Dict[str, Any]] = Field(default=None, description="The database schema with retrieved values of the data item")
    
    # Schema Linking Step
    direct_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None, description="The linked tables and columns of the data item by Direct Linking")
    reversed_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None, description="The linked tables and columns of the data item by Reversed Linking")
    value_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None, description="The linked tables and columns of the data item by Value Linking")
    final_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None, description="The final linked tables and columns of the data item")
    database_schema_after_schema_linking: Optional[Dict[str, Any]] = Field(default=None, description="The database schema with linked tables and columns of the data item")
    
    # SQL Generation Step
    sql_candidates: Optional[List[Dict[str, Any]]] = Field(default=None, description="The sql candidates of the data item")
    
    # SQL Revision Step
    sql_candidates_after_revision: Optional[List[Dict[str, Any]]] = Field(default=None, description="The sql candidates after revision of the data item")
    
    # SQL Selection Step
    top_k_sql_eval_scores: Optional[Dict[str, float]] = Field(default=None, description="The eval scores of the top k sql candidates of the data item")
    final_selected_sql: Optional[str] = Field(default=None, description="The final selected sql of the data item")
    
    # # SQL Regeneration Step
    # sql_candidates_after_regeneration: Optional[List[Dict[str, Any]]] = Field(default=None, description="The sql candidates after regeneration of the data item")
    # final_best_sql: Optional[str] = Field(default=None, description="The final best sql of the data item")
    
    # Schema linking recall metrics
    direct_linking_recall: Optional[Dict[str, float]] = Field(default=None, description="The direct linking recall")
    reversed_linking_recall: Optional[Dict[str, float]] = Field(default=None, description="The reversed linking recall")
    value_linking_recall: Optional[Dict[str, float]] = Field(default=None, description="The value linking recall")
    final_linking_recall: Optional[Dict[str, float]] = Field(default=None, description="The final linking recall")
    
    # Time cost metrics for each step
    value_retrieval_time: Optional[float] = Field(default=None, description="The time cost of value retrieval of the data item")
    schema_linking_time: Optional[float] = Field(default=None, description="The time cost of schema linking of the data item")
    sql_generation_time: Optional[float] = Field(default=None, description="The time cost of sql generation of the data item")
    sql_revision_time: Optional[float] = Field(default=None, description="The time cost of sql revision of the data item")
    sql_selection_time: Optional[float] = Field(default=None, description="The time cost of sql selection of the data item")
    sql_regeneration_time: Optional[float] = Field(default=None, description="The time cost of sql regeneration of the data item")
    total_time: Optional[float] = Field(default=None, description="The total time cost of the data item")
    
    # LLM cost metrics for each step
    value_retrieval_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of value retrieval of the data item")
    schema_linking_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of schema linking of the data item")
    sql_generation_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql generation of the data item")
    sql_revision_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql revision of the data item")
    sql_selection_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql selection of the data item")
    sql_regeneration_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql regeneration of the data item")
    total_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The total llm cost of the data item")


class BaseDataset(ABC):
    _config: DatasetConfig = None
    _data: List[DataItem] = None
    _database_schema_cache: Dict[str, Any] = {}

    def __init__(self, dataset_config: DatasetConfig):
        self._config = dataset_config
        self._data = self._load_data()

    def _load_database_schema(self, database_id: str):
        if database_id in self._database_schema_cache:
            return self._database_schema_cache[database_id]
        else:
            database_path = self._get_database_path(database_id)
            database_schema = load_database_schema_dict(database_path)
            self._database_schema_cache[database_id] = database_schema
            return database_schema
    
    @abstractmethod
    def _load_data(self):
        pass
    
    @abstractmethod
    def _get_database_path(self, database_id: str):
        pass
    
    def get_all_database_paths(self):
        return list(set([data_item.database_path for data_item in self._data]))
    
    def get_all_database_ids(self):
        return list(set([data_item.database_id for data_item in self._data]))
            
    def __len__(self):
        return len(self._data)
    
    def __getitem__(self, index: int):
        return self._data[index]
    
    def __iter__(self):
        return iter(self._data)
    

class BirdDataset(BaseDataset):
    
    _name = "bird"

    def _load_data(self):
        data_path = Path(self._config.root_path) / self._config.split / f"{self._config.split}.json"
        with open(data_path, "r") as f:
            data_list = json.load(f)
        data = []
        for data_item in tqdm(data_list, desc="Loading data"):
            question_id = data_item.get("question_id")
            question = data_item.get("question")
            evidence = data_item.get("evidence")
            gold_sql = data_item.get("SQL")
            difficulty = data_item.get("difficulty")
            database_id = data_item.get("db_id")
            database_path = self._get_database_path(database_id)
            database_schema = self._load_database_schema(database_id)
            data.append(
                DataItem(
                    question_id=question_id,
                    question=question,
                    evidence=evidence,
                    gold_sql=gold_sql,
                    difficulty=difficulty,
                    database_id=database_id,
                    database_path=database_path,
                    database_schema=database_schema,
                )
            )
        
        return data
        
    def _get_database_path(self, database_id: str):
        return str(Path(self._config.root_path) / self._config.split / f"{self._config.split}_databases" / database_id / f"{database_id}.sqlite")
    

class SpiderDataset(BaseDataset):
    
    _name = "spider"

    def _load_data(self):
        if self._config.split == "dev":
            data_path = Path(self._config.root_path) / "dev.json"
        elif self._config.split == "test":
            data_path = Path(self._config.root_path) / "test.json"
        else:
            raise ValueError(f"Invalid split: {self._config.split}")
        
        with open(data_path, "r") as f:
            data_list = json.load(f)
        data = []
        for question_id, data_item in tqdm(enumerate(data_list), desc="Loading data"):
            question = data_item.get("question")
            evidence = ""
            gold_sql = data_item.get("query")
            difficulty = ""
            database_id = data_item.get("db_id")
            database_path = self._get_database_path(database_id)
            database_schema = self._load_database_schema(database_id)
            data.append(
                DataItem(
                    question_id=question_id,
                    question=question,
                    evidence=evidence,
                    gold_sql=gold_sql,
                    difficulty=difficulty,
                    database_id=database_id,
                    database_path=database_path,
                    database_schema=database_schema,
                )
            )
        
        return data
    
    def _get_database_path(self, database_id: str):
        if self._config.split == "dev":
            return str(Path(self._config.root_path) / "database" / database_id / f"{database_id}.sqlite")
        elif self._config.split == "test":
            return str(Path(self._config.root_path) / "test_database" / database_id / f"{database_id}.sqlite")
        else:
            raise ValueError(f"Invalid split: {self._config.split}")
    

class DatasetFactory:

    @staticmethod
    def get_dataset(dataset_config: DatasetConfig):
        if dataset_config.type == "bird":
            return BirdDataset(dataset_config)
        elif dataset_config.type == "spider":
            return SpiderDataset(dataset_config)
        else:
            raise ValueError(f"Invalid dataset type: {dataset_config.type}")
