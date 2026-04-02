from __future__ import annotations

from pydantic import BaseModel, Field
from typing import TYPE_CHECKING, List, Optional, Any, Dict
from pathlib import Path
import json
from abc import ABC, abstractmethod
from tqdm import tqdm
from .artifacts import (
    AggregateMetrics,
    DataItemInput,
    PIPELINE_METRIC_FIELDS,
    PipelineArtifacts,
    STAGE_ARTIFACT_FIELDS,
    STAGE_ARTIFACT_MODELS,
    STAGE_VALIDATION_FIELDS,
    StageName,
)

if TYPE_CHECKING:
    from app.config import DatasetConfig


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
    direct_linked_tables_and_columns: Optional[Dict[str, List[str]]] = Field(default=None, description="The linked tables and columns of the data item by Direct Linking")
    reversed_linked_tables_and_columns: Optional[Dict[str, List[str]]] = Field(default=None, description="The linked tables and columns of the data item by Reversed Linking")
    value_linked_tables_and_columns: Optional[Dict[str, List[str]]] = Field(default=None, description="The linked tables and columns of the data item by Value Linking")
    final_linked_tables_and_columns: Optional[Dict[str, List[str]]] = Field(default=None, description="The final linked tables and columns of the data item")
    database_schema_after_schema_linking: Optional[Dict[str, Any]] = Field(default=None, description="The database schema with linked tables and columns of the data item")
    
    # SQL Generation Step
    sql_candidates: Optional[List[str]] = Field(default=None, description="The sql candidates of the data item")
    
    # SQL Revision Step
    sql_candidates_after_revision: Optional[List[str]] = Field(default=None, description="The sql candidates after revision of the data item")
    
    # SQL Selection Step
    final_selected_sql: Optional[str] = Field(default=None, description="The final selected sql of the data item")
    
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
    total_time: Optional[float] = Field(default=None, description="The total time cost of the data item")
    
    # LLM cost metrics for each step
    value_retrieval_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of value retrieval of the data item")
    schema_linking_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of schema linking of the data item")
    sql_generation_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql generation of the data item")
    sql_revision_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql revision of the data item")
    sql_selection_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The llm cost of sql selection of the data item")
    total_llm_cost: Optional[Dict[str, Any]] = Field(default=None, description="The total llm cost of the data item")

    def get_input_record(self) -> DataItemInput:
        return DataItemInput(
            question_id=self.question_id,
            question=self.question,
            evidence=self.evidence,
            gold_sql=self.gold_sql,
            difficulty=self.difficulty,
            database_id=self.database_id,
            database_path=self.database_path,
            database_schema=self.database_schema,
            instance_id=getattr(self, "instance_id", None),
            db_type=getattr(self, "db_type", None),
            external_knowledge_path=getattr(self, "external_knowledge_path", None),
        )

    def apply_input_record(self, input_record: DataItemInput | Dict[str, Any]) -> None:
        if isinstance(input_record, dict):
            input_record = DataItemInput(**input_record)

        base_fields = (
            "question_id",
            "question",
            "evidence",
            "gold_sql",
            "difficulty",
            "database_id",
            "database_path",
            "database_schema",
        )
        for field_name in base_fields:
            setattr(self, field_name, getattr(input_record, field_name))

        for optional_field in ("instance_id", "db_type", "external_knowledge_path"):
            optional_value = getattr(input_record, optional_field)
            if optional_value is not None and hasattr(self, optional_field):
                setattr(self, optional_field, optional_value)

    def get_stage_artifact(self, stage_name: StageName) -> BaseModel:
        artifact_model = STAGE_ARTIFACT_MODELS[stage_name]
        artifact_fields = STAGE_ARTIFACT_FIELDS[stage_name]
        return artifact_model(**{field_name: getattr(self, field_name) for field_name in artifact_fields})

    def apply_stage_artifact(self, stage_name: StageName, artifact: BaseModel | Dict[str, Any]) -> None:
        artifact_model = STAGE_ARTIFACT_MODELS[stage_name]
        artifact_fields = STAGE_ARTIFACT_FIELDS[stage_name]
        if isinstance(artifact, dict):
            artifact = artifact_model(**artifact)

        for field_name in artifact_fields:
            setattr(self, field_name, getattr(artifact, field_name))

    def get_metrics_record(self) -> AggregateMetrics:
        return AggregateMetrics(
            total_time=self.total_time,
            total_llm_cost=self.total_llm_cost,
        )

    def apply_metrics_record(self, metrics: AggregateMetrics | Dict[str, Any]) -> None:
        if isinstance(metrics, dict):
            metrics = AggregateMetrics(**metrics)

        self.total_time = metrics.total_time
        self.total_llm_cost = metrics.total_llm_cost

    def get_pipeline_artifacts(self) -> PipelineArtifacts:
        return PipelineArtifacts(
            value_retrieval=self.get_stage_artifact("value_retrieval"),
            schema_linking=self.get_stage_artifact("schema_linking"),
            sql_generation=self.get_stage_artifact("sql_generation"),
            sql_revision=self.get_stage_artifact("sql_revision"),
            sql_selection=self.get_stage_artifact("sql_selection"),
            metrics=self.get_metrics_record(),
        )

    def apply_pipeline_artifacts(self, pipeline_artifacts: PipelineArtifacts | Dict[str, Any]) -> None:
        if isinstance(pipeline_artifacts, dict):
            pipeline_artifacts = PipelineArtifacts(**pipeline_artifacts)

        self.apply_stage_artifact("value_retrieval", pipeline_artifacts.value_retrieval)
        self.apply_stage_artifact("schema_linking", pipeline_artifacts.schema_linking)
        self.apply_stage_artifact("sql_generation", pipeline_artifacts.sql_generation)
        self.apply_stage_artifact("sql_revision", pipeline_artifacts.sql_revision)
        self.apply_stage_artifact("sql_selection", pipeline_artifacts.sql_selection)
        self.apply_metrics_record(pipeline_artifacts.metrics)

    def get_item_id(self) -> str:
        if hasattr(self, "instance_id") and getattr(self, "instance_id"):
            return str(getattr(self, "instance_id"))
        return str(self.question_id)

    def is_stage_complete(self, stage_name: StageName) -> bool:
        artifact = self.get_stage_artifact(stage_name)
        if any(getattr(artifact, field_name) is None for field_name in STAGE_VALIDATION_FIELDS[stage_name]):
            return False

        metrics = self.get_metrics_record()
        return all(getattr(metrics, field_name) is not None for field_name in PIPELINE_METRIC_FIELDS)

    def get_stage_validation_errors(self, stage_name: StageName) -> List[Dict[str, str]]:
        errors: List[Dict[str, str]] = []
        artifact = self.get_stage_artifact(stage_name)
        for field_name in STAGE_VALIDATION_FIELDS[stage_name]:
            if getattr(artifact, field_name) is None:
                errors.append({"field": field_name, "error": f"Field '{field_name}' is None"})

        metrics = self.get_metrics_record()
        for field_name in PIPELINE_METRIC_FIELDS:
            if getattr(metrics, field_name) is None:
                errors.append({"field": field_name, "error": f"Metric '{field_name}' is None"})

        return errors


class BaseDataset(ABC):
    _config: DatasetConfig = None
    _data: List[DataItem] = None

    def __init__(self, dataset_config: DatasetConfig):
        self._config = dataset_config
        self._database_schema_cache: Dict[str, Any] = {}
        self._data = self._load_data()

    def _load_database_schema(self, database_id: str):
        from app.services import get_schema_service

        database_path = self._get_database_path(database_id)
        cache_key = str(Path(database_path).resolve())
        if cache_key in self._database_schema_cache:
            return self._database_schema_cache[cache_key]

        database_schema = get_schema_service().load_sqlite_schema(database_path)
        self._database_schema_cache[cache_key] = database_schema
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
        
        if self._config.max_samples is not None:
            data_list = data_list[:self._config.max_samples]
            
        data = []
        db_sample_count = {}  # Track samples per database
        
        for data_item in tqdm(data_list, desc="Loading data"):
            question_id = data_item.get("question_id")
            question = data_item.get("question")
            evidence = data_item.get("evidence")
            gold_sql = data_item.get("SQL")
            difficulty = data_item.get("difficulty")
            database_id = data_item.get("db_id")
            
            # Check if we've reached the max samples per database limit
            if self._config.max_samples_per_db is not None:
                if db_sample_count.get(database_id, 0) >= self._config.max_samples_per_db:
                    continue  # Skip this sample
            
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
            
            # Increment the count for this database
            db_sample_count[database_id] = db_sample_count.get(database_id, 0) + 1
        
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
        
        if self._config.max_samples is not None:
            data_list = data_list[:self._config.max_samples]
            
        data = []
        db_sample_count = {}  # Track samples per database
        question_id = 0
        
        for data_item in tqdm(data_list, desc="Loading data"):
            question = data_item.get("question")
            evidence = ""
            gold_sql = data_item.get("query")
            difficulty = ""
            database_id = data_item.get("db_id")
            
            # Check if we've reached the max samples per database limit
            if self._config.max_samples_per_db is not None:
                if db_sample_count.get(database_id, 0) >= self._config.max_samples_per_db:
                    continue  # Skip this sample
            
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
            
            # Increment the count for this database
            db_sample_count[database_id] = db_sample_count.get(database_id, 0) + 1
            question_id += 1
        
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
        elif dataset_config.type == "spider2":
            from .spider2_dataset import Spider2LiteDataset, Spider2SnowDataset
            if dataset_config.split == "lite":
                return Spider2LiteDataset(dataset_config)
            elif dataset_config.split == "snow":
                return Spider2SnowDataset(dataset_config)
            else:
                raise ValueError(f"Invalid spider2 split: {dataset_config.split}. Expected 'lite' or 'snow'")
        else:
            raise ValueError(f"Invalid dataset type: {dataset_config.type}")
