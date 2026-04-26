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


def _path_to_str(path: str | Path) -> str:
    return str(path)


class LLMConfig(BaseModel):
    model: str = Field(..., description="The model name")
    base_url: str = Field(..., description="The base url of the model service")
    api_key: str = Field(..., description="The api key of the model service")
    max_tokens: int = Field(default=4096, description="The maximum number of tokens to generate per request")
    max_request_n: Optional[int] = Field(default=None, ge=1, description="The maximum number of choices (n) per request; None means no limit")
    temperature: float = Field(default=0.7, description="The temperature of the model")
    api_type: Literal["openai", "azure"] = Field(default="openai", description="The type of the api")
    api_version: Optional[str] = Field(default=None, description="The version of the Azure API")
    fix_end_token: bool = Field(default=False, description="Whether to fix the end token to the LLM response")
    reasoning_effort: Optional[Literal["low", "medium", "high"]] = Field(default=None, description="The reasoning effort for the model (only for reasoning models like o1, o3)")
    max_model_len: int = Field(default=128000, description="The maximum context length of the model")


class DatasetConfig(BaseModel):
    type: Literal["spider", "bird", "spider2"] = Field(..., description="The type of the dataset")
    split: Optional[str] = Field(default="", description="The split of the dataset")
    root_path: Optional[str] = Field(..., description="The root path of the dataset")
    save_path: Optional[str] = Field(default=None, description="The save path of the dataset snapshot manifest")
    max_samples: Optional[int] = Field(default=None, description="The maximum number of samples to load")
    max_samples_per_db: Optional[int] = Field(default=None, description="The maximum number of samples to load per database")
    
    # Spider2 specific configurations
    snowflake_credential_path: Optional[str] = Field(default=None, description="Path to Snowflake credential JSON file")
    bigquery_credential_path: Optional[str] = Field(default=None, description="Path to BigQuery credential JSON file")
    
    sql_execution_timeout: int = Field(default=600, description="The timeout for SQL execution in seconds")
    max_value_example_length: int = Field(default=100, description="The maximum length of the value examples in the schema")
    
    @model_validator(mode="after")
    def validate_split_and_defaults(self):
        if self.type == "spider":
            if self.split not in ["dev", "test"]:
                raise ValueError(f"Invalid split: {self.split}")
        elif self.type == "bird":
            # only dev split is supported for bird dataset
            if self.split not in ["dev"]:
                raise ValueError(f"Invalid split: {self.split}")
        elif self.type == "spider2":
            # Spider2 supports lite and snow splits
            if self.split not in ["lite", "snow"]:
                raise ValueError(f"Invalid split for spider2: {self.split}. Expected 'lite' or 'snow'")
        else:
            raise ValueError(f"Invalid dataset type: {self.type}")
        if self.save_path is None:
            self.save_path = _path_to_str(WORKSPACE_ROOT / "dataset" / self.type / f"{self.split}.snapshot")
        else:
            self.save_path = _path_to_str(self.save_path)
        return self


class VectorDatabaseConfig(BaseModel):
    api_type: Literal["local", "openai"] = Field(default="local", description="The type of the embedding api")
    embedding_model_name_or_path: str = Field(..., description="The embedding model name or path")
    use_qwen3_embedding: bool = Field(default=False, description="Whether to use Qwen3 embedding")
    local_files_only: bool = Field(default=False, description="Whether to use local files only")
    normalize_embeddings: bool = Field(default=False, description="Whether to normalize embeddings")
    base_url: Optional[str] = Field(default=None, description="The base url of the embedding model service")
    api_key: Optional[str] = Field(default=None, description="The api key of the embedding model service")
    store_root_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "vector_store"), description="The root path of the vector database")
    embedding_device: str = Field(default="auto", description="Execution device for local embedding models, e.g. auto, cpu, cuda, cuda:0")
    max_value_length: int = Field(default=100, description="The maximum length of the value")
    batch_size: int = Field(default=1024, description="The batch size for adding documents to the vector database")
    db_parallel: int = Field(default=1, ge=1, description="The number of databases to process in parallel")
    column_parallel: int = Field(default=1, ge=1, description="The number of columns to scan in parallel within a single database")
    lower_meta_data: bool = Field(default=True, description="Whether to lower the meta data")
    build_backend: Literal["chroma", "local_index", "both"] = Field(default="both", description="Which retrieval index artifacts to build")


class ValueRetrievalConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to extract keywords")
    n_results: int = Field(default=5, description="The number of results to retrieve")
    n_parallel: int = Field(default=16, description="The number of samples to process in parallel")
    query_parallel_per_sample: int = Field(default=4, ge=1, description="Maximum concurrent Chroma column queries within a single sample")
    backend: Literal["chroma", "local_index"] = Field(default="chroma", description="The retrieval backend to use for value retrieval")
    local_index_device: str = Field(default="auto", description="Execution device for the local index backend, e.g. auto, cpu, cuda, cuda:0, cuda:1")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "value_retrieval"), description="The save path of the value retrieval result")


class SchemaLinkingConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to link tables and columns")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    n_internal_parallel: int = Field(default=3, description="Max parallel workers within a single sample (direct/reversed/value linkers)")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "schema_linking"), description="The save path of the schema linking result")
    direct_linking_sampling_budget: int = Field(default=5, description="The sampling budget of the direct linking")
    reversed_linking_sampling_budget: int = Field(default=5, description="The sampling budget of the reversed linking")
    value_distance_threshold: float = Field(default=0.05, description="The threshold of the value distance in value linking")
    

class SQLGenerationConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to generate sql")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    n_internal_parallel: int = Field(default=3, description="Max parallel workers within a single sample (dc/skeleton/icl generators)")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "sql_generation"), description="The save path of the sql generation result")
    dc_sampling_budget: int = Field(default=5, description="The sampling budget of the dc generation")
    skeleton_sampling_budget: int = Field(default=5, description="The sampling budget of the skeleton generation")
    icl_sampling_budget: int = Field(default=5, description="The sampling budget of the icl generation")
    icl_few_shot_examples_path: Optional[str] = Field(default=None, description="The path of the icl few shot examples")


class SQLRevisionConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to revise sql")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    n_internal_parallel: int = Field(default=16, description="Max parallel workers within a single sample (revising unique candidates)")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "sql_revision"), description="The save path of the sql revision result")
    checker_sampling_budget: int = Field(default=5, description="The sampling budget of the checker")
    checkers: List[str] = Field(default=[], description="The list of checkers to enable")


class SQLSelectionConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to select sql")
    n_parallel: int = Field(default=16, description="The number of parallel threads to use")
    n_internal_parallel: int = Field(default=8, description="Max parallel workers within a single sample (pairwise SQL comparison)")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "sql_selection"), description="The save path of the sql selection result")
    filter_top_k_sql: int = Field(default=2, description="The number of top k sql to filter")
    evaluator_sampling_budget: int = Field(default=1, description="The sampling budget of the evaluator")
    shortcut_consistency_score_threshold: float = Field(default=0.8, description="The threshold of the consistency score to shortcut")


class LLMExtractorConfig(BaseModel):
    max_retry: int = Field(default=3, description="Maximum retry attempts for parsing LLM responses")


class LoggerConfig(BaseModel):
    print_level: str = Field(default="INFO", description="The log level for the console")


class AppConfig(BaseModel):
    dataset: DatasetConfig = Field(default_factory=DatasetConfig, description="The config of the dataset")
    vector_database: VectorDatabaseConfig = Field(default_factory=VectorDatabaseConfig, description="The config of the vector database")
    value_retrieval: ValueRetrievalConfig = Field(default_factory=ValueRetrievalConfig, description="The config of the value retrieval")
    schema_linking: SchemaLinkingConfig = Field(default_factory=SchemaLinkingConfig, description="The config of the schema linking")
    sql_generation: SQLGenerationConfig = Field(default_factory=SQLGenerationConfig, description="The config of the sql generation")
    sql_revision: SQLRevisionConfig = Field(default_factory=SQLRevisionConfig, description="The config of the sql revision")
    sql_selection: SQLSelectionConfig = Field(default_factory=SQLSelectionConfig, description="The config of the sql selection")
    llm_extractor: LLMExtractorConfig = Field(default_factory=LLMExtractorConfig, description="The config of the LLM extractor for fallback parsing")
    logger: LoggerConfig = Field(default_factory=LoggerConfig, description="The config of the logger")
    
    
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
        import os
        env_config_path = os.environ.get("CONFIG_PATH")
        if env_config_path:
            config_path = Path(env_config_path)
        else:
            config_path = PROJECT_ROOT / "config" / "config.toml"
            
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found at {config_path}")
        return config_path
    
    @staticmethod
    def _load_config(config_path: Path):
        with open(config_path, "rb") as f:
            return tomllib.load(f)

    def _initialize_config(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = Config._get_config_path()
        config = Config._load_config(config_path)
        
        # llm config
        llm_config_list = config.get("llm_list", [])
        llm_settings = []
        for llm_config in llm_config_list:
            llm_settings.append({
                "model": llm_config.get("model"),
                "base_url": llm_config.get("base_url"),
                "api_key": llm_config.get("api_key"),
                "max_tokens": llm_config.get("max_tokens", 4096),
                "max_request_n": llm_config.get("max_request_n", None),
                "temperature": llm_config.get("temperature", 0.7),
                "api_type": llm_config.get("api_type", "openai"),
                "api_version": llm_config.get("api_version", None),
            })
        
        # dataset config
        dataset_config = config.get("dataset", {})
        dataset_type = dataset_config.get("type")
        dataset_split = dataset_config.get("split", "")
        dataset_settings = {
            "type": dataset_type,
            "split": dataset_split,
            "root_path": dataset_config.get("root_path"),
            "save_path": dataset_config.get("save_path"),
            "max_samples": dataset_config.get("max_samples", None),
            "max_samples_per_db": dataset_config.get("max_samples_per_db", None),
            # Spider2 specific configurations
            "snowflake_credential_path": dataset_config.get("snowflake_credential_path", None),
            "bigquery_credential_path": dataset_config.get("bigquery_credential_path", None),
            "sql_execution_timeout": dataset_config.get("sql_execution_timeout", 600),
            "max_value_example_length": dataset_config.get("max_value_example_length", 100),
        }
        
        # vector database config
        vector_database_config = config.get("vector_database", {})
        vector_database_settings = {
            "api_type": vector_database_config.get("api_type", "local"),
            "embedding_model_name_or_path": vector_database_config.get("embedding_model_name_or_path"),
            "store_root_path": _path_to_str(vector_database_config.get("store_root_path", WORKSPACE_ROOT / "vector_store")),
            "embedding_device": vector_database_config.get("embedding_device", "auto"),
            "use_qwen3_embedding": vector_database_config.get("use_qwen3_embedding", False),
            "local_files_only": vector_database_config.get("local_files_only", False),
            "normalize_embeddings": vector_database_config.get("normalize_embeddings", False),
            "base_url": vector_database_config.get("base_url", None),
            "api_key": vector_database_config.get("api_key", None),
            "max_value_length": vector_database_config.get("max_value_length", 100),
            "batch_size": vector_database_config.get("batch_size", 1024),
            "db_parallel": vector_database_config.get("db_parallel", 1),
            "column_parallel": vector_database_config.get("column_parallel", 1),
            "lower_meta_data": vector_database_config.get("lower_meta_data", True),
            "build_backend": vector_database_config.get("build_backend", "both"),
        }
        
        # value retrieval config
        value_retrieval_config = config.get("value_retrieval", {})
        value_retrieval_settings = {
            "llm": LLMConfig(**value_retrieval_config.get("llm")),
            "n_results": value_retrieval_config.get("n_results", 5),
            "n_parallel": value_retrieval_config.get("n_parallel", 16),
            "query_parallel_per_sample": value_retrieval_config.get("query_parallel_per_sample", 4),
            "backend": value_retrieval_config.get("backend", "chroma"),
            "local_index_device": value_retrieval_config.get("local_index_device", "auto"),
            "save_path": _path_to_str(value_retrieval_config.get("save_path", WORKSPACE_ROOT / "value_retrieval")),
        }
        
        # schema linking config
        schema_linking_config = config.get("schema_linking", {})
        schema_linking_settings = {
            "llm": LLMConfig(**schema_linking_config.get("llm")),
            "n_parallel": schema_linking_config.get("n_parallel", 16),
            "n_internal_parallel": schema_linking_config.get("n_internal_parallel", 3),
            "save_path": _path_to_str(schema_linking_config.get("save_path", WORKSPACE_ROOT / "schema_linking")),
            "direct_linking_sampling_budget": schema_linking_config.get("direct_linking_sampling_budget", 5),
            "reversed_linking_sampling_budget": schema_linking_config.get("reversed_linking_sampling_budget", 5),
            "value_distance_threshold": schema_linking_config.get("value_distance_threshold", 0.05),
        }
        
        # sql generation config
        sql_generation_config = config.get("sql_generation", {})
        sql_generation_settings = {
            "llm": LLMConfig(**sql_generation_config.get("llm")),
            "n_parallel": sql_generation_config.get("n_parallel", 16),
            "n_internal_parallel": sql_generation_config.get("n_internal_parallel", 3),
            "save_path": _path_to_str(sql_generation_config.get("save_path", WORKSPACE_ROOT / "sql_generation")),
            "dc_sampling_budget": sql_generation_config.get("dc_sampling_budget", 5),
            "skeleton_sampling_budget": sql_generation_config.get("skeleton_sampling_budget", 5),
            "icl_sampling_budget": sql_generation_config.get("icl_sampling_budget", 5),
            "icl_few_shot_examples_path": sql_generation_config.get("icl_few_shot_examples_path", None),
        }
        
        # sql revision config
        sql_revision_config = config.get("sql_revision", {})
        sql_revision_settings = {
            "llm": LLMConfig(**sql_revision_config.get("llm")),
            "n_parallel": sql_revision_config.get("n_parallel", 16),
            "n_internal_parallel": sql_revision_config.get("n_internal_parallel", 16),
            "save_path": _path_to_str(sql_revision_config.get("save_path", WORKSPACE_ROOT / "sql_revision")),
            "checker_sampling_budget": sql_revision_config.get("checker_sampling_budget", 5),
            "checkers": sql_revision_config.get("checkers", []),
        }
        
        # sql selection config
        sql_selection_config = config.get("sql_selection", {})
        sql_selection_settings = {
            "llm": LLMConfig(**sql_selection_config.get("llm")),
            "n_parallel": sql_selection_config.get("n_parallel", 16),
            "n_internal_parallel": sql_selection_config.get("n_internal_parallel", 8),
            "save_path": _path_to_str(sql_selection_config.get("save_path", WORKSPACE_ROOT / "sql_selection")),
            "filter_top_k_sql": sql_selection_config.get("filter_top_k_sql", 10),
            "evaluator_sampling_budget": sql_selection_config.get("evaluator_sampling_budget", 1),
            "shortcut_consistency_score_threshold": sql_selection_config.get("shortcut_consistency_score_threshold", 0.8),
        }
        
        # llm extractor config (retry settings for parsing)
        llm_extractor_config = config.get("llm_extractor", {})
        llm_extractor_settings = {
            "max_retry": llm_extractor_config.get("max_retry", 3),
        }
        
        # logger config
        logger_config = config.get("logger", {})
        logger_settings = {
            "print_level": logger_config.get("print_level", "INFO"),
        }
        
        self._app_config = AppConfig(
            dataset=DatasetConfig(**dataset_settings),
            vector_database=VectorDatabaseConfig(**vector_database_settings),
            value_retrieval=ValueRetrievalConfig(**value_retrieval_settings),
            schema_linking=SchemaLinkingConfig(**schema_linking_settings),
            sql_generation=SQLGenerationConfig(**sql_generation_settings),
            sql_revision=SQLRevisionConfig(**sql_revision_settings),
            sql_selection=SQLSelectionConfig(**sql_selection_settings),
            llm_extractor=LLMExtractorConfig(**llm_extractor_settings),
            logger=LoggerConfig(**logger_settings)
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
    
    @property
    def llm_extractor_config(self):
        return self._app_config.llm_extractor
    
    @property
    def logger_config(self):
        return self._app_config.logger


_config_instance: Optional[Config] = None


def get_config() -> Config:
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance


class _ConfigProxy:
    def __getattr__(self, name: str):
        return getattr(get_config(), name)

    def __repr__(self) -> str:
        return "LazyConfigProxy()"


config = _ConfigProxy()
