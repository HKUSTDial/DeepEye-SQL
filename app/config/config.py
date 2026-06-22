import shutil
import threading
import tomllib
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
    n_call_strategy: Literal["single", "split"] = Field(default="single", description="How to request multiple choices: single uses n>1 in one request, split sends multiple n=1 requests")
    temperature: float = Field(default=0.7, description="The temperature of the model")
    api_type: Literal["openai", "azure"] = Field(default="openai", description="The type of the api")
    api_version: Optional[str] = Field(default=None, description="The version of the Azure API")
    fix_end_token: bool = Field(default=False, description="Whether to fix the end token to the LLM response")
    reasoning_effort: Optional[Literal["low", "medium", "high"]] = Field(default=None, description="The reasoning effort for the model (only for reasoning models like o1, o3)")
    max_model_len: int = Field(default=128000, description="The maximum context length of the model")
    extra_body: Dict[str, Any] = Field(default_factory=dict, description="Additional request body fields passed to the chat completion API")


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
            if self.split not in ["dev", "test"]:
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
    embedding_model_name_or_path: Optional[str] = Field(default=None, description="The embedding model name or path")
    use_qwen3_embedding: bool = Field(default=False, description="Whether to use Qwen3 embedding")
    local_files_only: bool = Field(default=False, description="Whether to use local files only")
    normalize_embeddings: bool = Field(default=False, description="Whether to normalize embeddings")
    base_url: Optional[str] = Field(default=None, description="The base url of the embedding model service")
    api_key: Optional[str] = Field(default=None, description="The api key of the embedding model service")
    store_root_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "vector_store"), description="The root path of the vector database")
    embedding_device: str = Field(default="auto", description="Execution device for local embedding models, e.g. auto, cpu, cuda, cuda:0")
    max_value_length: int = Field(default=100, description="The maximum length of the value")
    lower_meta_data: bool = Field(default=True, description="Whether to lower the meta data")
    build_backend: Literal["chroma", "local_index", "both"] = Field(default="both", description="Which retrieval index artifacts to build")


class EmbeddingConfig(BaseModel):
    api_type: Literal["local", "openai"] = Field(default="local", description="The type of the embedding api")
    embedding_model_name_or_path: str = Field(..., description="The embedding model name or path")
    use_qwen3_embedding: bool = Field(default=False, description="Whether to use Qwen3 embedding")
    local_files_only: bool = Field(default=False, description="Whether to use local files only")
    normalize_embeddings: bool = Field(default=False, description="Whether to normalize embeddings")
    base_url: Optional[str] = Field(default=None, description="The base url of the embedding model service")
    api_key: Optional[str] = Field(default=None, description="The api key of the embedding model service")
    embedding_device: str = Field(default="auto", description="Execution device for local embedding models, e.g. auto, cpu, cuda, cuda:0")


def _deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _merge_section_defaults(
    default_config: Optional[Dict[str, Any]],
    section_config: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if default_config is None and section_config is None:
        return None
    return _deep_merge_dicts(default_config or {}, section_config or {})


def _resolve_llm_config(
    default_config: Optional[Dict[str, Any]],
    section_config: Optional[Dict[str, Any]],
    section_name: str,
    *,
    required: bool,
    llm_profiles: Optional[Dict[str, Dict[str, Any]]] = None,
    default_profile: Optional[str] = None,
    section_profile: Optional[str] = None,
) -> Optional[LLMConfig]:
    profile_name = section_profile or default_profile
    base_config = default_config
    if profile_name:
        if not llm_profiles or profile_name not in llm_profiles:
            raise ValueError(f"Unknown llm_profile '{profile_name}' for {section_name}")
        base_config = llm_profiles[profile_name]

    merged = _merge_section_defaults(base_config, section_config)
    if merged is None:
        if required:
            raise ValueError(
                f"{section_name}.llm or {section_name}.llm_profile is required "
                "when neither [llm] nor [run].default_llm_profile is configured"
            )
        return None
    try:
        return LLMConfig(**merged)
    except Exception as exc:
        raise ValueError(
            f"Invalid LLM config for {section_name}. "
            "Provide a complete [llm], a valid llm_profile, or a complete section-specific .llm override."
        ) from exc


def _resolve_embedding_config(
    default_config: Optional[Dict[str, Any]],
    section_config: Optional[Dict[str, Any]],
    section_name: str,
    *,
    required: bool,
) -> Optional[EmbeddingConfig]:
    merged = _merge_section_defaults(default_config, section_config)
    if merged is None:
        if required:
            raise ValueError(f"{section_name}.embedding is required when top-level [embedding] is not configured")
        return None
    try:
        return EmbeddingConfig(**merged)
    except Exception as exc:
        raise ValueError(
            f"Invalid embedding config for {section_name}. "
            "Provide a complete top-level [embedding] section or a complete section-specific embedding override."
        ) from exc


def _get_path_value(
    section_config: Dict[str, Any],
    key: str,
    managed_paths: bool,
    managed_default: str | Path,
    legacy_default: str | Path,
) -> str:
    if section_config.get(key) is not None:
        return _path_to_str(section_config.get(key))
    if managed_paths:
        return _path_to_str(managed_default)
    return _path_to_str(legacy_default)


def _get_optional_path_value(
    section_config: Dict[str, Any],
    key: str,
    managed_paths: bool,
    managed_default: str | Path,
) -> Optional[str]:
    if section_config.get(key) is not None:
        return _path_to_str(section_config.get(key))
    if managed_paths:
        return _path_to_str(managed_default)
    return None


class ValueRetrievalConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to extract keywords")
    max_values_per_column: int = Field(default=5, ge=1, description="Maximum number of retrieved value examples to keep per column")
    backend: Literal["chroma", "local_index"] = Field(default="chroma", description="The retrieval backend to use for value retrieval")
    local_index_device: str = Field(default="auto", description="Execution device for the local index backend, e.g. auto, cpu, cuda, cuda:0, cuda:1")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "value_retrieval"), description="The save path of the value retrieval result")


class PreliminarySQLConfig(BaseModel):
    enabled: bool = Field(default=False, description="Whether to generate preliminary SQL before few-shot retrieval")
    dc_sampling_budget: int = Field(default=2, ge=0, description="Sampling budget for preliminary divide-and-conquer SQL generation")
    skeleton_sampling_budget: int = Field(default=2, ge=0, description="Sampling budget for preliminary skeleton SQL generation")
    llm: Optional[LLMConfig] = Field(default=None, description="The LLM config used for preliminary SQL generation")

    @model_validator(mode="after")
    def validate_enabled_config(self):
        if self.enabled:
            if self.llm is None:
                raise ValueError("[few_shot_index.preliminary_sql.llm] is required when preliminary SQL is enabled")
            if self.dc_sampling_budget + self.skeleton_sampling_budget <= 0:
                raise ValueError("preliminary SQL generation requires at least one positive sampling budget")
        return self


class FewShotIndexConfig(BaseModel):
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "few_shot_index"), description="The save path of the few-shot training index")
    prepared_save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "few_shot_preparation"), description="The save path of the dataset snapshot with prepared few-shot examples")
    mask_cache_path: Optional[str] = Field(default=None, description="The JSONL cache path for LLM-masked training examples")
    target_mask_cache_path: Optional[str] = Field(default=None, description="The JSONL cache path for LLM-masked target items")
    similarity_device: str = Field(default="cpu", description="Execution device for few-shot similarity scoring, e.g. cpu, auto, cuda, cuda:0")
    num_examples: int = Field(default=5, ge=1, description="The number of few-shot examples to retrieve")
    question_weight: float = Field(default=0.5, ge=0.0, description="The retrieval weight for masked question similarity")
    sql_weight: float = Field(default=0.5, ge=0.0, description="The retrieval weight for masked SQL similarity")
    max_samples: Optional[int] = Field(default=None, ge=1, description="The maximum number of training examples to index")
    max_samples_per_db: Optional[int] = Field(default=None, ge=1, description="The maximum number of training examples to index per source database")
    force_rebuild: bool = Field(default=False, description="Whether to overwrite an existing few-shot index")
    llm: Optional[LLMConfig] = Field(default=None, description="The LLM config used for question/SQL masking")
    embedding: Optional[EmbeddingConfig] = Field(default=None, description="The embedding config used for few-shot index vectors")
    preliminary_sql: PreliminarySQLConfig = Field(default_factory=PreliminarySQLConfig, description="Preliminary SQL generation config used before few-shot retrieval")

    @model_validator(mode="after")
    def validate_retrieval_weights(self):
        if self.question_weight + self.sql_weight <= 0:
            raise ValueError("question_weight and sql_weight cannot both be 0")
        return self


class SchemaLinkingConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to link tables and columns")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "schema_linking"), description="The save path of the schema linking result")
    direct_linking_sampling_budget: int = Field(default=5, description="The sampling budget of the direct linking")
    reversed_linking_sampling_budget: int = Field(default=5, description="The sampling budget of the reversed linking")
    value_distance_threshold: float = Field(default=0.05, description="The threshold of the value distance in value linking")
    

class SQLGenerationConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to generate sql")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "sql_generation"), description="The save path of the sql generation result")
    dc_sampling_budget: int = Field(default=5, description="The sampling budget of the dc generation")
    skeleton_sampling_budget: int = Field(default=5, description="The sampling budget of the skeleton generation")
    icl_sampling_budget: int = Field(default=5, description="The sampling budget of the icl generation")
    icl_few_shot_examples_path: Optional[str] = Field(default=None, description="The path of the icl few shot examples")


class SQLRevisionConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to revise sql")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "sql_revision"), description="The save path of the sql revision result")
    checker_sampling_budget: int = Field(default=5, description="The sampling budget of the checker")
    checkers: List[str] = Field(default=[], description="The list of checkers to enable")


class SQLSelectionConfig(BaseModel):
    llm: LLMConfig = Field(..., description="The llm config, used to select sql")
    save_path: str = Field(default=_path_to_str(WORKSPACE_ROOT / "sql_selection"), description="The save path of the sql selection result")
    filter_top_k_sql: int = Field(default=2, description="The number of top k sql to filter")
    evaluator_sampling_budget: int = Field(default=1, description="The sampling budget of the evaluator")
    shortcut_consistency_score_threshold: float = Field(default=0.8, description="The threshold of the consistency score to shortcut")


class LLMExtractorConfig(BaseModel):
    max_retry: int = Field(default=3, description="Maximum retry attempts for parsing LLM responses")


class LoggerConfig(BaseModel):
    print_level: str = Field(default="INFO", description="The log level for the console")


class RunConfig(BaseModel):
    save_root: str = Field(default=_path_to_str(WORKSPACE_ROOT / "runs"), description="Root directory for managed run outputs")
    exp_name: str = Field(default="default", description="Experiment name used under save_root")
    save_dir: str = Field(default=_path_to_str(WORKSPACE_ROOT / "runs" / "default"), description="Resolved directory for this run")
    shared_dir: str = Field(default=_path_to_str(WORKSPACE_ROOT / "runs" / "_shared"), description="Resolved directory for reusable artifacts shared across runs")
    default_llm_profile: Optional[str] = Field(default=None, description="Default LLM profile name used by stages without an explicit llm_profile")
    parallelism: int = Field(default=16, ge=1, description="Global parallelism for LLM-heavy pipeline stages")
    embedding_batch_size: int = Field(default=128, ge=1, description="Global batch size for embedding requests and local embedding forwards")
    llm_timeout: int = Field(default=300, ge=1, description="Global timeout for explicit LLM requests in seconds")
    progress_log_interval: int = Field(default=50, ge=1, description="Log long-running progress every N completed items")
    checkpoint_interval: int = Field(default=20, ge=1, description="Persist intermediate pipeline checkpoints every N completed items")


class AppConfig(BaseModel):
    run: RunConfig = Field(default_factory=RunConfig, description="The config of managed run output directories")
    dataset: DatasetConfig = Field(default_factory=DatasetConfig, description="The config of the dataset")
    vector_database: VectorDatabaseConfig = Field(default_factory=VectorDatabaseConfig, description="The config of the vector database")
    value_retrieval: ValueRetrievalConfig = Field(default_factory=ValueRetrievalConfig, description="The config of the value retrieval")
    few_shot_index: FewShotIndexConfig = Field(default_factory=FewShotIndexConfig, description="The config of the few-shot training index")
    schema_linking: SchemaLinkingConfig = Field(default_factory=SchemaLinkingConfig, description="The config of the schema linking")
    sql_generation: SQLGenerationConfig = Field(default_factory=SQLGenerationConfig, description="The config of the sql generation")
    sql_revision: SQLRevisionConfig = Field(default_factory=SQLRevisionConfig, description="The config of the sql revision")
    sql_selection: SQLSelectionConfig = Field(default_factory=SQLSelectionConfig, description="The config of the sql selection")
    llm_extractor: LLMExtractorConfig = Field(default_factory=LLMExtractorConfig, description="The config of the LLM extractor for fallback parsing")
    logger: LoggerConfig = Field(default_factory=LoggerConfig, description="The config of the logger")
    
    
class Config:
    _app_config: AppConfig = None
    _config_path: Optional[Path] = None
    _run_config_path: Optional[Path] = None
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
            config_path = PROJECT_ROOT / "config" / "local" / "config.toml"
            
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
        config_path = config_path.resolve()
        self._config_path = config_path
        config = Config._load_config(config_path)
        
        default_llm_config = config.get("llm")
        llm_profiles_config = config.get("llm_profiles", {})
        default_embedding_config = config.get("embedding")
        
        # dataset config
        dataset_config = config.get("dataset", {})
        dataset_type = dataset_config.get("type")
        dataset_split = dataset_config.get("split", "")
        run_config = config.get("run")
        default_llm_profile = (run_config or {}).get("default_llm_profile")
        managed_paths = run_config is not None
        default_exp_name = f"{dataset_type}-{dataset_split}".strip("-") if dataset_type else "default"
        if managed_paths:
            run_save_root = Path(run_config.get("save_root", WORKSPACE_ROOT / "runs"))
            run_exp_name = run_config.get("exp_name", default_exp_name)
            run_save_dir = Path(run_config.get("save_dir", run_save_root / run_exp_name))
            run_shared_dir = Path(run_config.get("shared_dir", run_save_root / "_shared" / str(dataset_type)))
        else:
            run_save_root = WORKSPACE_ROOT / "runs"
            run_exp_name = default_exp_name
            run_save_dir = run_save_root / run_exp_name
            run_shared_dir = run_save_root / "_shared" / str(dataset_type)
        run_settings = {
            "save_root": _path_to_str(run_save_root),
            "exp_name": str(run_exp_name),
            "save_dir": _path_to_str(run_save_dir),
            "shared_dir": _path_to_str(run_shared_dir),
            "default_llm_profile": default_llm_profile,
            "parallelism": run_config.get("parallelism", 16) if managed_paths else 16,
            "embedding_batch_size": run_config.get("embedding_batch_size", 128) if managed_paths else 128,
            "llm_timeout": run_config.get("llm_timeout", 300) if managed_paths else 300,
            "progress_log_interval": run_config.get("progress_log_interval", 50) if managed_paths else 50,
            "checkpoint_interval": run_config.get("checkpoint_interval", 20) if managed_paths else 20,
        }
        dataset_settings = {
            "type": dataset_type,
            "split": dataset_split,
            "root_path": dataset_config.get("root_path"),
            "save_path": _get_path_value(
                dataset_config,
                "save_path",
                managed_paths,
                run_save_dir / "dataset.snapshot",
                WORKSPACE_ROOT / "dataset" / str(dataset_type) / f"{dataset_split}.snapshot",
            ),
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
        vector_database_embedding_config = _merge_section_defaults(default_embedding_config, vector_database_config)
        if (
            dataset_type != "spider2"
            and not (vector_database_embedding_config or {}).get("embedding_model_name_or_path")
        ):
            raise ValueError("[embedding].embedding_model_name_or_path is required for non-Spider2 datasets")
        vector_database_settings = {
            "api_type": (vector_database_embedding_config or {}).get("api_type", "local"),
            "embedding_model_name_or_path": (vector_database_embedding_config or {}).get("embedding_model_name_or_path"),
            "store_root_path": _get_path_value(
                vector_database_config,
                "store_root_path",
                managed_paths,
                run_save_dir / "vector_database",
                WORKSPACE_ROOT / "vector_store",
            ),
            "embedding_device": (vector_database_embedding_config or {}).get("embedding_device", "auto"),
            "use_qwen3_embedding": (vector_database_embedding_config or {}).get("use_qwen3_embedding", False),
            "local_files_only": (vector_database_embedding_config or {}).get("local_files_only", False),
            "normalize_embeddings": (vector_database_embedding_config or {}).get("normalize_embeddings", False),
            "base_url": (vector_database_embedding_config or {}).get("base_url", None),
            "api_key": (vector_database_embedding_config or {}).get("api_key", None),
            "max_value_length": vector_database_config.get("max_value_length", 100),
            "lower_meta_data": vector_database_config.get("lower_meta_data", True),
            "build_backend": vector_database_config.get("build_backend", "both"),
        }
        
        # value retrieval config
        value_retrieval_config = config.get("value_retrieval", {})
        value_retrieval_settings = {
            "llm": _resolve_llm_config(
                default_llm_config,
                value_retrieval_config.get("llm"),
                "[value_retrieval]",
                required=True,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=value_retrieval_config.get("llm_profile"),
            ),
            "max_values_per_column": value_retrieval_config.get("max_values_per_column", 5),
            "backend": value_retrieval_config.get("backend", "chroma"),
            "local_index_device": value_retrieval_config.get("local_index_device", "auto"),
            "save_path": _get_path_value(
                value_retrieval_config,
                "save_path",
                managed_paths,
                run_save_dir / "value_retrieval.snapshot",
                WORKSPACE_ROOT / "value_retrieval",
            ),
        }

        # few-shot index config
        few_shot_index_config = config.get("few_shot_index", {})
        few_shot_index_llm_config = few_shot_index_config.get("llm")
        few_shot_index_embedding_config = few_shot_index_config.get("embedding")
        preliminary_sql_config = few_shot_index_config.get("preliminary_sql", {})
        preliminary_sql_llm_config = preliminary_sql_config.get("llm")
        preliminary_sql_settings = {
            "enabled": preliminary_sql_config.get("enabled", False),
            "dc_sampling_budget": preliminary_sql_config.get("dc_sampling_budget", 2),
            "skeleton_sampling_budget": preliminary_sql_config.get("skeleton_sampling_budget", 2),
            "llm": _resolve_llm_config(
                default_llm_config,
                preliminary_sql_llm_config,
                "[few_shot_index.preliminary_sql]",
                required=False,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=preliminary_sql_config.get("llm_profile"),
            ),
        }
        few_shot_index_settings = {
            "save_path": _get_path_value(
                few_shot_index_config,
                "save_path",
                managed_paths,
                run_shared_dir / "train_full_index",
                WORKSPACE_ROOT / "few_shot_index" / str(dataset_type) / "train",
            ),
            "prepared_save_path": _get_path_value(
                few_shot_index_config,
                "prepared_save_path",
                managed_paths,
                run_save_dir / "few_shot_preparation.snapshot",
                WORKSPACE_ROOT / "few_shot_preparation" / str(dataset_type) / f"{dataset_split}.snapshot",
            ),
            "mask_cache_path": _get_optional_path_value(
                few_shot_index_config,
                "mask_cache_path",
                managed_paths,
                run_shared_dir / "train_full_mask_cache.jsonl",
            ),
            "target_mask_cache_path": _get_optional_path_value(
                few_shot_index_config,
                "target_mask_cache_path",
                managed_paths,
                run_save_dir / "few_shot_target_mask_cache.jsonl",
            ),
            "similarity_device": few_shot_index_config.get("similarity_device", "cpu"),
            "num_examples": few_shot_index_config.get("num_examples", 5),
            "question_weight": few_shot_index_config.get("question_weight", 0.5),
            "sql_weight": few_shot_index_config.get("sql_weight", 0.5),
            "max_samples": few_shot_index_config.get("max_samples", None),
            "max_samples_per_db": few_shot_index_config.get("max_samples_per_db", None),
            "force_rebuild": few_shot_index_config.get("force_rebuild", False),
            "llm": _resolve_llm_config(
                default_llm_config,
                few_shot_index_llm_config,
                "[few_shot_index]",
                required=False,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=few_shot_index_config.get("llm_profile"),
            ),
            "embedding": _resolve_embedding_config(
                default_embedding_config,
                few_shot_index_embedding_config,
                "[few_shot_index]",
                required=False,
            ),
            "preliminary_sql": PreliminarySQLConfig(**preliminary_sql_settings),
        }
        
        # schema linking config
        schema_linking_config = config.get("schema_linking", {})
        schema_linking_settings = {
            "llm": _resolve_llm_config(
                default_llm_config,
                schema_linking_config.get("llm"),
                "[schema_linking]",
                required=True,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=schema_linking_config.get("llm_profile"),
            ),
            "save_path": _get_path_value(
                schema_linking_config,
                "save_path",
                managed_paths,
                run_save_dir / "schema_linking.snapshot",
                WORKSPACE_ROOT / "schema_linking",
            ),
            "direct_linking_sampling_budget": schema_linking_config.get("direct_linking_sampling_budget", 5),
            "reversed_linking_sampling_budget": schema_linking_config.get("reversed_linking_sampling_budget", 5),
            "value_distance_threshold": schema_linking_config.get("value_distance_threshold", 0.05),
        }
        
        # sql generation config
        sql_generation_config = config.get("sql_generation", {})
        sql_generation_settings = {
            "llm": _resolve_llm_config(
                default_llm_config,
                sql_generation_config.get("llm"),
                "[sql_generation]",
                required=True,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=sql_generation_config.get("llm_profile"),
            ),
            "save_path": _get_path_value(
                sql_generation_config,
                "save_path",
                managed_paths,
                run_save_dir / "sql_generation.snapshot",
                WORKSPACE_ROOT / "sql_generation",
            ),
            "dc_sampling_budget": sql_generation_config.get("dc_sampling_budget", 5),
            "skeleton_sampling_budget": sql_generation_config.get("skeleton_sampling_budget", 5),
            "icl_sampling_budget": sql_generation_config.get("icl_sampling_budget", 5),
            "icl_few_shot_examples_path": sql_generation_config.get("icl_few_shot_examples_path", None),
        }
        
        # sql revision config
        sql_revision_config = config.get("sql_revision", {})
        sql_revision_settings = {
            "llm": _resolve_llm_config(
                default_llm_config,
                sql_revision_config.get("llm"),
                "[sql_revision]",
                required=True,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=sql_revision_config.get("llm_profile"),
            ),
            "save_path": _get_path_value(
                sql_revision_config,
                "save_path",
                managed_paths,
                run_save_dir / "sql_revision.snapshot",
                WORKSPACE_ROOT / "sql_revision",
            ),
            "checker_sampling_budget": sql_revision_config.get("checker_sampling_budget", 5),
            "checkers": sql_revision_config.get("checkers", []),
        }
        
        # sql selection config
        sql_selection_config = config.get("sql_selection", {})
        sql_selection_settings = {
            "llm": _resolve_llm_config(
                default_llm_config,
                sql_selection_config.get("llm"),
                "[sql_selection]",
                required=True,
                llm_profiles=llm_profiles_config,
                default_profile=default_llm_profile,
                section_profile=sql_selection_config.get("llm_profile"),
            ),
            "save_path": _get_path_value(
                sql_selection_config,
                "save_path",
                managed_paths,
                run_save_dir / "sql_selection.snapshot",
                WORKSPACE_ROOT / "sql_selection",
            ),
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
            run=RunConfig(**run_settings),
            dataset=DatasetConfig(**dataset_settings),
            vector_database=VectorDatabaseConfig(**vector_database_settings),
            value_retrieval=ValueRetrievalConfig(**value_retrieval_settings),
            few_shot_index=FewShotIndexConfig(**few_shot_index_settings),
            schema_linking=SchemaLinkingConfig(**schema_linking_settings),
            sql_generation=SQLGenerationConfig(**sql_generation_settings),
            sql_revision=SQLRevisionConfig(**sql_revision_settings),
            sql_selection=SQLSelectionConfig(**sql_selection_settings),
            llm_extractor=LLMExtractorConfig(**llm_extractor_settings),
            logger=LoggerConfig(**logger_settings)
        )
        self._copy_config_to_run_dir()

    def _copy_config_to_run_dir(self) -> None:
        if self._config_path is None or self._app_config is None:
            return

        run_config_dir = Path(self._app_config.run.save_dir)
        run_config_dir.mkdir(parents=True, exist_ok=True)
        run_config_path = run_config_dir / "config.toml"
        shutil.copy2(self._config_path, run_config_path)
        self._run_config_path = run_config_path

    @property
    def app_config(self):
        return self._app_config
    
    @property
    def config_path(self):
        return self._config_path

    @property
    def run_config_path(self):
        return self._run_config_path

    @property
    def workspace_config_path(self):
        return self._run_config_path

    @property
    def dataset_config(self):
        return self._app_config.dataset

    @property
    def run_config(self):
        return self._app_config.run

    @property
    def vector_database_config(self):
        return self._app_config.vector_database

    @property
    def value_retrieval_config(self):
        return self._app_config.value_retrieval

    @property
    def few_shot_index_config(self):
        return self._app_config.few_shot_index
    
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
