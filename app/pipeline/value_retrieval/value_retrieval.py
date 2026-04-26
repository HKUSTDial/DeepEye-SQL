from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import extract_keywords, retrieve_values_for_one_column, embed_keywords
from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from app.vector_db import (
    LocalValueIndex,
    get_embedding_function,
    get_collection_name,
    get_local_index_path,
    local_index_exists,
)
from app.db_utils import map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from app.pipeline.validation import validate_pipeline_step
from chromadb.api import ClientAPI
from chromadb import PersistentClient
from chromadb.types import Collection
from typing import Dict, List, Any
import time
import threading
from collections import defaultdict
from app.logger import logger
from app.services import ArtifactStore, STAGE_ARTIFACT_FIELDS, configure_schema_service, get_schema_service, load_stage_dataset, reset_schema_service
from app.llm_extractor import LLMExtractor


def _is_spider2_item(data_item: DataItem) -> bool:
    """Check if the data item belongs to a Spider2 series dataset."""
    return hasattr(data_item, "instance_id")


DEFAULT_QUERY_PARALLEL_PER_SAMPLE = 4
MAX_CHROMA_POOL_TIMEOUT_RETRIES = 3


class ValueRetrievalRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _vector_db_client_dict: Dict[str, ClientAPI]
    _vector_db_collection_dict: Dict[str, Collection]
    _local_value_index_dict: Dict[str, LocalValueIndex]
    _prepared_sqlite_schema_dict: Dict[str, Dict[str, Any]]
    _embedding_function: Any = None # Shared embedding function
    _thread_pool_executor: ThreadPoolExecutor = None
    _db_lock: threading.Lock
    _artifact_store: ArtifactStore = None
    _extractor_max_retry: int = 3
    _stage_config = None
    _dataset_config = None
    _vector_database_config = None
    _keyword_extractor: LLMExtractor = None
    _query_parallel_per_sample: int = 1
    _retrieval_backend: str = "chroma"
    _local_index_device: str = "auto"
    
    def __init__(
        self,
        stage_config,
        dataset_config,
        vector_database_config,
        extractor_max_retry: int,
    ):
        self._stage_config = stage_config
        self._dataset_config = dataset_config
        self._vector_database_config = vector_database_config
        self._extractor_max_retry = extractor_max_retry
        self._vector_db_client_dict = {}
        self._vector_db_collection_dict = {}
        self._local_value_index_dict = {}
        self._prepared_sqlite_schema_dict = {}
        self._db_lock = threading.Lock()
        self._artifact_store = ArtifactStore(
            self._stage_config.save_path,
            "value_retrieval",
            STAGE_ARTIFACT_FIELDS["value_retrieval"],
        )
        self._dataset, checkpoint_source = load_stage_dataset(
            load_dataset_fn=load_dataset,
            current_save_path=self._stage_config.save_path,
            fallback_load_path=self._dataset_config.save_path,
            artifact_store=self._artifact_store,
            stage_name="value_retrieval",
        )
        logger.info(f"Initialized value retrieval dataset from {checkpoint_source}")
        configure_schema_service(max_value_example_length=self._dataset_config.max_value_example_length)
        self._llm = LLM(self._stage_config.llm)
        
        # Initialize the shared embedding function once - ONLY if not Spider2
        if not self._dataset_config.type.startswith("spider2"):
            self._embedding_function = get_embedding_function(
                model_name_or_path=self._vector_database_config.embedding_model_name_or_path,
                api_type=self._vector_database_config.api_type,
                use_qwen3_embedding=self._vector_database_config.use_qwen3_embedding,
                local_files_only=self._vector_database_config.local_files_only,
                normalize_embeddings=self._vector_database_config.normalize_embeddings,
                base_url=self._vector_database_config.base_url,
                api_key=self._vector_database_config.api_key,
                embedding_device=self._vector_database_config.embedding_device,
            )
        else:
            logger.info("Skipping embedding function initialization for Spider2 dataset")
            self._embedding_function = None

        self._query_parallel_per_sample = max(
            1,
            getattr(
                self._stage_config,
                "query_parallel_per_sample",
                DEFAULT_QUERY_PARALLEL_PER_SAMPLE,
            ),
        )
        self._retrieval_backend = getattr(self._stage_config, "backend", "chroma")
        self._local_index_device = getattr(self._stage_config, "local_index_device", "auto")
        logger.info(
            "Using up to "
            f"{self._query_parallel_per_sample} concurrent column retrieval tasks per sample "
            f"with n_parallel={self._stage_config.n_parallel}"
        )
        logger.info(
            f"Using value retrieval backend={self._retrieval_backend} "
            f"(local_index_device={self._local_index_device})"
        )

        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self._stage_config.n_parallel)
        self._keyword_extractor = LLMExtractor(max_retry=self._extractor_max_retry)

    @classmethod
    def from_config(cls, app_config=None) -> "ValueRetrievalRunner":
        if app_config is None:
            from app.config import get_config

            app_config = get_config()
        return cls(
            stage_config=app_config.value_retrieval_config,
            dataset_config=app_config.dataset_config,
            vector_database_config=app_config.vector_database_config,
            extractor_max_retry=app_config.llm_extractor_config.max_retry,
        )
    
    def _get_vector_collection(self, db_id: str) -> Collection:
        """Lazy initialization of vector database collection with thread safety."""
        with self._db_lock:
            if db_id not in self._vector_db_collection_dict:
                vector_db_path = Path(self._vector_database_config.store_root_path) / db_id
                client = PersistentClient(path=vector_db_path)
                self._vector_db_client_dict[db_id] = client
                self._vector_db_collection_dict[db_id] = client.get_collection(
                    name=get_collection_name(db_id),
                    embedding_function=self._embedding_function # Use shared instance
                )
            return self._vector_db_collection_dict[db_id]

    def _get_local_value_index(self, db_id: str) -> LocalValueIndex:
        with self._db_lock:
            if db_id not in self._local_value_index_dict:
                vector_db_path = Path(self._vector_database_config.store_root_path) / db_id
                index_path = get_local_index_path(vector_db_path)
                if not local_index_exists(vector_db_path):
                    raise FileNotFoundError(
                        f"Local index not found for {db_id}: {index_path}. "
                        "Rebuild the vector database with local index enabled."
                    )
                self._local_value_index_dict[db_id] = LocalValueIndex(
                    index_path=index_path,
                    device=self._local_index_device,
                )
            return self._local_value_index_dict[db_id]

    def _get_prepared_sqlite_schema(self, data_item: DataItem) -> Dict[str, Any]:
        if data_item.database_schema.get("db_type", "sqlite") != "sqlite":
            return data_item.database_schema

        with self._db_lock:
            prepared_schema = self._prepared_sqlite_schema_dict.get(data_item.database_id)
            if prepared_schema is None:
                schema_service = get_schema_service()
                prepared_schema = schema_service.load_sqlite_schema(data_item.database_path)
                schema_service.ensure_schema_features(
                    prepared_schema,
                    include_value_examples=True,
                )
                self._prepared_sqlite_schema_dict[data_item.database_id] = prepared_schema
            return prepared_schema

    def _retrieve_values_for_column(
        self,
        query_embeddings: List[List[float]],
        collection_or_index: Collection | LocalValueIndex,
        db_id: str,
        table_name: str,
        column_name: str,
    ) -> Dict[str, Any]:
        if self._retrieval_backend == "local_index":
            return collection_or_index.retrieve_values_for_column(
                query_embeddings=query_embeddings,
                table_name=table_name,
                column_name=column_name,
                n_results=self._stage_config.n_results,
                lower_meta_data=self._vector_database_config.lower_meta_data,
            )

        for attempt_idx in range(MAX_CHROMA_POOL_TIMEOUT_RETRIES):
            try:
                return retrieve_values_for_one_column(
                    query_embeddings,
                    collection_or_index,
                    table_name,
                    column_name,
                    self._stage_config.n_results,
                    self._vector_database_config.lower_meta_data,
                )
            except Exception as exc:
                error_message = str(exc).lower()
                is_pool_timeout = "pool timed out" in error_message and "open connection" in error_message
                is_segment_error = "failed to get segments" in error_message
                if (not is_pool_timeout and not is_segment_error) or attempt_idx == MAX_CHROMA_POOL_TIMEOUT_RETRIES - 1:
                    raise

                sleep_seconds = min(2 ** attempt_idx, 8)
                logger.warning(
                    "Retrying Chroma column query for "
                    f"{db_id}.{table_name}.{column_name} "
                    f"(attempt {attempt_idx + 1}/{MAX_CHROMA_POOL_TIMEOUT_RETRIES}); "
                    f"reason={str(exc)}; retrying in {sleep_seconds}s"
                )
                time.sleep(sleep_seconds)

    def _extract_keywords(self, data_item: DataItem) -> tuple[List[str], Dict[str, int]]:
        return extract_keywords(
            data_item.question,
            data_item.evidence,
            self._llm,
            fix_end_token=self._llm.llm_config.fix_end_token,
            extractor_max_retry=self._extractor_max_retry,
            extractor=self._keyword_extractor,
        )

    @staticmethod
    def _get_item_log_prefix(data_item: DataItem) -> str:
        return f"[value_retrieval][item {data_item.get_item_id()}][db {data_item.database_id}]"

    @staticmethod
    def _get_progress_markers(total_steps: int) -> set[int]:
        if total_steps <= 0:
            return set()
        return {
            1,
            max(1, total_steps // 4),
            max(1, total_steps // 2),
            max(1, (total_steps * 3) // 4),
            total_steps,
        }
    
    def _retrieve_values_for_item(self, data_item: DataItem):
        """Processes a single data item: keyword extraction + vector retrieval."""
        start_time = time.time()
        item_log_prefix = self._get_item_log_prefix(data_item)
        logger.info(f"{item_log_prefix} started")
        
        # 1. LLM Keyword Extraction
        keyword_start_time = time.time()
        keywords, token_usage = self._extract_keywords(data_item)
        data_item.question_keywords = keywords
        data_item.value_retrieval_llm_cost = token_usage
        logger.info(
            f"{item_log_prefix} extracted {len(keywords)} keywords "
            f"in {time.time() - keyword_start_time:.2f}s"
        )
        
        # 2. Independent Keyword Embedding
        # Get embeddings once for all columns in this item
        embedding_start_time = time.time()
        query_embeddings = embed_keywords(
            keywords,
            self._embedding_function,
            batch_size=self._vector_database_config.batch_size,
        )
        logger.info(
            f"{item_log_prefix} embedded {len(keywords)} keywords "
            f"into {len(query_embeddings)} vectors in {time.time() - embedding_start_time:.2f}s"
        )

        base_schema_start_time = time.time()
        prepared_schema = self._get_prepared_sqlite_schema(data_item)
        logger.info(
            f"{item_log_prefix} prepared base schema value examples in "
            f"{time.time() - base_schema_start_time:.2f}s"
        )
        
        # 3. Vector Retrieval for each text column (Parallelized within the item)
        retrieval_start_time = time.time()
        retrieval_resource = (
            self._get_local_value_index(data_item.database_id)
            if self._retrieval_backend == "local_index"
            else self._get_vector_collection(data_item.database_id)
        )
        data_item.retrieved_values = defaultdict(dict)
        
        # Prepare all text column tasks
        column_tasks = []
        table_names = data_item.database_schema["tables"].keys()
        for table_name in table_names:
            columns = data_item.database_schema["tables"][table_name]["columns"].items()
            for column_name, column_dict in columns:
                column_type = column_dict["column_type"]
                if column_type.upper() == "TEXT" or column_type.upper().startswith("VARCHAR") or column_type.upper().startswith("CHAR"):
                    column_tasks.append((table_name, column_name))

        total_column_tasks = len(column_tasks)
        if total_column_tasks == 0:
            logger.info(f"{item_log_prefix} found no text columns; skipping vector retrieval")
        else:
            logger.info(
                f"{item_log_prefix} retrieving values from {total_column_tasks} text columns "
                f"with query_parallel_per_sample={self._query_parallel_per_sample}"
            )

        if column_tasks:
            progress_markers = self._get_progress_markers(total_column_tasks)
            completed_columns = 0
            with ThreadPoolExecutor(max_workers=min(total_column_tasks, self._query_parallel_per_sample)) as col_executor:
                future_to_col = {
                    col_executor.submit(
                        self._retrieve_values_for_column,
                        query_embeddings,
                        retrieval_resource,
                        data_item.database_id,
                        t_name,
                        c_name,
                    ): (t_name, c_name) for t_name, c_name in column_tasks
                }
                for future in as_completed(future_to_col):
                    result = future.result()
                    original_table_name = map_lower_table_name_to_original_table_name(result["table_name"], data_item.database_schema)
                    original_column_name = map_lower_column_name_to_original_column_name(result["table_name"], result["column_name"], data_item.database_schema)
                    data_item.retrieved_values[original_table_name][original_column_name] = result["values"]
                    completed_columns += 1
                    if completed_columns in progress_markers:
                        logger.info(
                            f"{item_log_prefix} column retrieval progress "
                            f"{completed_columns}/{total_column_tasks} "
                            f"({time.time() - retrieval_start_time:.2f}s elapsed)"
                        )
        
        data_item.retrieved_values = dict(data_item.retrieved_values)
        schema_update_start_time = time.time()
        self._update_database_schema(data_item, prepared_schema)
        logger.info(
            f"{item_log_prefix} schema updated in {time.time() - schema_update_start_time:.2f}s"
        )
        
        # 3. Update Metrics
        data_item.value_retrieval_time = time.time() - start_time
        data_item.total_time = (data_item.total_time or 0) + data_item.value_retrieval_time
        
        # Merge LLM cost
        if data_item.total_llm_cost is None:
            data_item.total_llm_cost = data_item.value_retrieval_llm_cost
        else:
            for k, v in data_item.value_retrieval_llm_cost.items():
                data_item.total_llm_cost[k] += v
        logger.info(
            f"{item_log_prefix} completed in {data_item.value_retrieval_time:.2f}s "
            f"(text_columns={total_column_tasks})"
        )

    def _update_database_schema(self, data_item: DataItem, prepared_schema: Dict[str, Any]):
        database_schema_after_value_retrieval = self._clone_database_schema(data_item.database_schema)
        for table_name, column_dict in data_item.retrieved_values.items():
            for column_name, values in column_dict.items():
                original_values = data_item.database_schema["tables"][table_name]["columns"][column_name].get("value_examples")
                if original_values is None:
                    original_values = prepared_schema["tables"][table_name]["columns"][column_name].get("value_examples") or []
                new_values = [value["value"] for value in values] + original_values
                new_values = new_values[:self._stage_config.n_results]
                database_schema_after_value_retrieval["tables"][table_name]["columns"][column_name]["value_examples"] = new_values
        data_item.database_schema_after_value_retrieval = database_schema_after_value_retrieval

    @staticmethod
    def _clone_database_schema(database_schema: Dict[str, Any]) -> Dict[str, Any]:
        cloned_schema = {
            key: value
            for key, value in database_schema.items()
            if key != "tables"
        }
        cloned_tables = {}
        for table_name, table_schema in database_schema.get("tables", {}).items():
            cloned_table = {
                key: value
                for key, value in table_schema.items()
                if key not in {"columns", "nested_columns"}
            }
            cloned_columns = {}
            for column_name, column_schema in table_schema.get("columns", {}).items():
                cloned_column = dict(column_schema)
                foreign_keys = cloned_column.get("foreign_keys")
                if foreign_keys is not None:
                    cloned_column["foreign_keys"] = list(foreign_keys)
                value_examples = cloned_column.get("value_examples")
                if isinstance(value_examples, list):
                    cloned_column["value_examples"] = list(value_examples)
                value_statistics = cloned_column.get("value_statistics")
                if isinstance(value_statistics, dict):
                    cloned_column["value_statistics"] = dict(value_statistics)
                cloned_columns[column_name] = cloned_column
            cloned_table["columns"] = cloned_columns
            nested_columns = table_schema.get("nested_columns")
            if isinstance(nested_columns, dict):
                cloned_table["nested_columns"] = {
                    nested_name: dict(nested_info) if isinstance(nested_info, dict) else nested_info
                    for nested_name, nested_info in nested_columns.items()
                }
            cloned_tables[table_name] = cloned_table
        cloned_schema["tables"] = cloned_tables
        return cloned_schema
    
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._vector_db_client_dict = {}
        self._vector_db_collection_dict = {}
        self._local_value_index_dict = {}
        self._prepared_sqlite_schema_dict = {}
        self._keyword_extractor = None
        if self._artifact_store is not None:
            self._artifact_store.close()
        reset_schema_service()
    
    def save_result(self, materialize_snapshot: bool = False):
        self._artifact_store.flush()
        if materialize_snapshot:
            save_dataset(self._dataset, self._stage_config.save_path)
            self._artifact_store.cleanup()
    
    def _skip_value_retrieval_for_item(self, data_item: DataItem):
        """
        Handle items by skipping value retrieval.
        Used for Spider2 datasets where value retrieval is not required.
        """
        # Set empty values for value retrieval fields
        data_item.question_keywords = []
        data_item.value_retrieval_llm_cost = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        data_item.retrieved_values = {}
        data_item.value_retrieval_time = 0.0
        data_item.total_time = 0.0
        data_item.total_llm_cost = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        # Copy original schema as-is (no value retrieval enhancement)
        data_item.database_schema_after_value_retrieval = self._clone_database_schema(data_item.database_schema)
        
        logger.info(f"{self._get_item_log_prefix(data_item)} skipped")

    def run(self):
        future_to_item = {}
        skipped_spider2_count = 0
        
        for data_item in self._dataset:
            if data_item.is_stage_complete("value_retrieval"):
                logger.info(f"Skipping data item {data_item.question_id} because it has already been retrieved")
                continue
            
            # Skip Spider2 datasets - Vector DB and Value Retrieval not needed
            if _is_spider2_item(data_item):
                self._skip_value_retrieval_for_item(data_item)
                self._artifact_store.record_item(data_item)
                skipped_spider2_count += 1
                continue
            
            # Submit each item to the thread pool (SQLite only)
            future = self._thread_pool_executor.submit(self._retrieve_values_for_item, data_item)
            future_to_item[future] = data_item
        
        if skipped_spider2_count > 0:
            logger.info(f"Skipped {skipped_spider2_count} Spider2 items (Value Retrieval not required)")
            
        for idx, future in tqdm(enumerate(as_completed(future_to_item), start=1), total=len(future_to_item), desc="Value Retrieval"):
            data_item = future_to_item[future]
            try:
                future.result()
                self._artifact_store.record_item(data_item)
            except Exception as e:
                logger.exception(f"Error processing data item {data_item.get_item_id()}: {e}")
            
            if idx % 5 == 0:
                logger.info(f"Value Retrieval {idx} / {len(future_to_item)} completed")
                self.save_result()
            
        # Validate that all required fields are filled
        self._artifact_store.flush()
        validate_pipeline_step(self._dataset, "value_retrieval")
        self.save_result(materialize_snapshot=True)
        
        self._clean_up()
