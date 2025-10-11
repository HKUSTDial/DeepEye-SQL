import tomllib
import threading
from typing import Dict, List, Optional, Literal, Any
from pathlib import Path
from pydantic import BaseModel, Field, model_validator


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


PROJECT_ROOT = get_project_root()
WORKSPACE_ROOT = PROJECT_ROOT / "workspace"

if not Path(WORKSPACE_ROOT).exists():
    Path(WORKSPACE_ROOT).mkdir(parents=True, exist_ok=True)


class LLMConfig(BaseModel):
    model: str = Field(..., description="The model name")
    base_url: str = Field(..., description="The base url of the model service")
    api_key: str = Field(..., description="The api key of the model service")
    max_tokens: int = Field(default=4096, description="The maximum number of tokens to generate per request")
    temperature: float = Field(default=0.7, description="The temperature of the model")
    api_type: Literal["openai", "azure"] = Field(default="openai", description="The type of the api")
    api_version: Optional[str] = Field(default=None, description="The version of the Azure API")


class DatasetConfig(BaseModel):
    type: Literal["spider", "bird"] = Field(..., description="The type of the dataset")
    split: Literal["dev", "test"] = Field(..., description="The split of the dataset")
    root_path: Optional[str] = Field(..., description="The root path of the dataset")
    save_path: str = Field(default=WORKSPACE_ROOT / "dataset" / f"{type}" / f"{split}.pkl", description="The save path of the dataset")
    
    @model_validator(mode="after")
    def validate_split(self):
        if self.type == "spider":
            if self.split not in ["dev", "test"]:
                raise ValueError(f"Invalid split: {self.split}")
        elif self.type == "bird":
            # only dev split is supported for bird dataset
            if self.split not in ["dev"]:
                raise ValueError(f"Invalid split: {self.split}")
        else:
            raise ValueError(f"Invalid dataset type: {self.type}")
        return self


class VectorDatabaseConfig(BaseModel):
    embedding_model_name_or_path: str = Field(..., description="The embedding model name or path")
    use_qwen3_embedding: bool = Field(default=False, description="Whether to use Qwen3 embedding")
    local_files_only: bool = Field(default=False, description="Whether to use local files only")
    normalize_embeddings: bool = Field(default=False, description="Whether to normalize embeddings")
    base_url: Optional[str] = Field(default=None, description="The base url of the embedding model service")
    api_key: Optional[str] = Field(default=None, description="The api key of the embedding model service")
    store_root_path: str = Field(default=WORKSPACE_ROOT / "vector_store", description="The root path of the vector database")
    max_value_length: int = Field(default=100, description="The maximum length of the value")
    lower_meta_data: bool = Field(default=True, description="Whether to lower the meta data")


class ValueRetrievalConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to extract keywords")
    n_results: int = Field(default=5, description="The number of results to retrieve")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    save_path: str = Field(default=WORKSPACE_ROOT / "value_retrieval", description="The save path of the value retrieval result")


class SchemaLinkingConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to link tables and columns")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    save_path: str = Field(default=WORKSPACE_ROOT / "schema_linking", description="The save path of the schema linking result")
    direct_linking_sampling_budget: int = Field(default=5, description="The sampling budget of the direct linking")
    reversed_linking_sampling_budget: int = Field(default=5, description="The sampling budget of the reversed linking")
    value_distance_threshold: float = Field(default=0.05, description="The threshold of the value distance in value linking")
    

class SQLGenerationConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to generate sql")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    save_path: str = Field(default=WORKSPACE_ROOT / "sql_generation", description="The save path of the sql generation result")
    dc_sampling_budget: int = Field(default=5, description="The sampling budget of the dc generation")
    skeleton_sampling_budget: int = Field(default=5, description="The sampling budget of the skeleton generation")
    icl_sampling_budget: int = Field(default=5, description="The sampling budget of the icl generation")
    icl_few_shot_examples_path: str = Field(..., description="The path of the icl few shot examples")


class SQLRevisionConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to revise sql")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    save_path: str = Field(default=WORKSPACE_ROOT / "sql_revision", description="The save path of the sql revision result")
    checker_sampling_budget: int = Field(default=5, description="The sampling budget of the checker")


class SQLSelectionConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to select sql")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    save_path: str = Field(default=WORKSPACE_ROOT / "sql_selection", description="The save path of the sql selection result")
    filter_top_k_sql: int = Field(default=2, description="The number of top k sql to filter")
    evaluator_sampling_budget: int = Field(default=1, description="The sampling budget of the evaluator")
    shortcut_consistency_score_threshold: float = Field(default=0.8, description="The threshold of the consistency score to shortcut")


class AppConfig(BaseModel):
    dataset: DatasetConfig = Field(default_factory=DatasetConfig, description="The config of the dataset")
    vector_database: VectorDatabaseConfig = Field(default_factory=VectorDatabaseConfig, description="The config of the vector database")
    value_retrieval: ValueRetrievalConfig = Field(default_factory=ValueRetrievalConfig, description="The config of the value retrieval")
    schema_linking: SchemaLinkingConfig = Field(default_factory=SchemaLinkingConfig, description="The config of the schema linking")
    sql_generation: SQLGenerationConfig = Field(default_factory=SQLGenerationConfig, description="The config of the sql generation")
    sql_revision: SQLRevisionConfig = Field(default_factory=SQLRevisionConfig, description="The config of the sql revision")
    sql_selection: SQLSelectionConfig = Field(default_factory=SQLSelectionConfig, description="The config of the sql selection")
    
    
class Config:
    _app_config: AppConfig = None
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self._initialize_config()

    @staticmethod
    def _get_config_path():
        config_path = PROJECT_ROOT / "config" / "config.toml"
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found at {config_path}")
        return config_path
    
    @staticmethod
    def _load_config():
        with open(Config._get_config_path(), "rb") as f:
            return tomllib.load(f)

    def _initialize_config(self):
        config = Config._load_config()
        
        # llm config
        llm_config_list = config.get("llm_list", [])
        llm_settings = []
        for llm_config in llm_config_list:
            llm_settings.append({
                "model": llm_config.get("model"),
                "base_url": llm_config.get("base_url"),
                "api_key": llm_config.get("api_key"),
                "max_tokens": llm_config.get("max_tokens", 4096),
                "temperature": llm_config.get("temperature", 0.7),
                "api_type": llm_config.get("api_type", "openai"),
                "api_version": llm_config.get("api_version", None),
            })
        
        # dataset config
        dataset_config = config.get("dataset", {})
        dataset_settings = {
            "type": dataset_config.get("type"),
            "split": dataset_config.get("split"),
            "root_path": dataset_config.get("root_path"),
            "save_path": dataset_config.get("save_path", WORKSPACE_ROOT / "dataset" / f"{dataset_config.get('type')}" / f"{dataset_config.get('split')}.pkl"),
        }
        
        # vector database config
        vector_database_config = config.get("vector_database", {})
        vector_database_settings = {
            "embedding_model_name_or_path": vector_database_config.get("embedding_model_name_or_path"),
            "store_root_path": vector_database_config.get("store_root_path", WORKSPACE_ROOT / "vector_store"),
            "use_qwen3_embedding": vector_database_config.get("use_qwen3_embedding", False),
            "local_files_only": vector_database_config.get("local_files_only", False),
            "normalize_embeddings": vector_database_config.get("normalize_embeddings", False),
            "base_url": vector_database_config.get("base_url", None),
            "api_key": vector_database_config.get("api_key", None),
        }
        
        # value retrieval config
        value_retrieval_config = config.get("value_retrieval", {})
        value_retrieval_settings = {
            "llm": LLMConfig(**value_retrieval_config.get("llm")),
            "n_results": value_retrieval_config.get("n_results", 5),
            "n_parallel": value_retrieval_config.get("n_parallel", 16),
            "save_path": value_retrieval_config.get("save_path", WORKSPACE_ROOT / "value_retrieval"),
        }
        
        # schema linking config
        schema_linking_config = config.get("schema_linking", {})
        schema_linking_settings = {
            "llm": LLMConfig(**schema_linking_config.get("llm")),
            "n_parallel": schema_linking_config.get("n_parallel", 16),
            "save_path": schema_linking_config.get("save_path", WORKSPACE_ROOT / "schema_linking"),
            "direct_linking_sampling_budget": schema_linking_config.get("direct_linking_sampling_budget", 5),
            "reversed_linking_sampling_budget": schema_linking_config.get("reversed_linking_sampling_budget", 5),
            "value_distance_threshold": schema_linking_config.get("value_distance_threshold", 0.05),
        }
        
        # sql generation config
        sql_generation_config = config.get("sql_generation", {})
        sql_generation_settings = {
            "llm": LLMConfig(**sql_generation_config.get("llm")),
            "n_parallel": sql_generation_config.get("n_parallel", 16),
            "save_path": sql_generation_config.get("save_path", WORKSPACE_ROOT / "sql_generation"),
            "dc_sampling_budget": sql_generation_config.get("dc_sampling_budget", 5),
            "skeleton_sampling_budget": sql_generation_config.get("skeleton_sampling_budget", 5),
            "icl_sampling_budget": sql_generation_config.get("icl_sampling_budget", 5),
            "icl_few_shot_examples_path": sql_generation_config.get("icl_few_shot_examples_path"),
        }
        
        # sql revision config
        sql_revision_config = config.get("sql_revision", {})
        sql_revision_settings = {
            "llm": LLMConfig(**sql_revision_config.get("llm")),
            "n_parallel": sql_revision_config.get("n_parallel", 16),
            "save_path": sql_revision_config.get("save_path", WORKSPACE_ROOT / "sql_revision"),
            "checker_sampling_budget": sql_revision_config.get("checker_sampling_budget", 5),
        }
        
        # sql selection config
        sql_selection_config = config.get("sql_selection", {})
        sql_selection_settings = {
            "llm": LLMConfig(**sql_selection_config.get("llm")),
            "n_parallel": sql_selection_config.get("n_parallel", 16),
            "save_path": sql_selection_config.get("save_path", WORKSPACE_ROOT / "sql_selection"),
            "filter_top_k_sql": sql_selection_config.get("filter_top_k_sql", 10),
            "evaluator_sampling_budget": sql_selection_config.get("evaluator_sampling_budget", 1),
            "shortcut_consistency_score_threshold": sql_selection_config.get("shortcut_consistency_score_threshold", 0.8),
        }
        
        self._app_config = AppConfig(
            dataset=DatasetConfig(**dataset_settings),
            vector_database=VectorDatabaseConfig(**vector_database_settings),
            value_retrieval=ValueRetrievalConfig(**value_retrieval_settings),
            schema_linking=SchemaLinkingConfig(**schema_linking_settings),
            sql_generation=SQLGenerationConfig(**sql_generation_settings),
            sql_revision=SQLRevisionConfig(**sql_revision_settings),
            sql_selection=SQLSelectionConfig(**sql_selection_settings)
        )

    @property
    def app_config(self):
        return self._app_config
    
    @property
    def dataset_config(self):
        return self._app_config.dataset

    @property
    def vector_database_config(self):
        return self._app_config.vector_database

    @property
    def value_retrieval_config(self):
        return self._app_config.value_retrieval
    
    @property
    def schema_linking_config(self):
        return self._app_config.schema_linking

    @property
    def sql_generation_config(self):
        return self._app_config.sql_generation

    @property
    def sql_revision_config(self):
        return self._app_config.sql_revision

    @property
    def sql_selection_config(self):
        return self._app_config.sql_selection
    
# global config instance
config = Config()