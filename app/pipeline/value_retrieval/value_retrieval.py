from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import extract_keywords, retrieve_values_for_one_column, embed_keywords
from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from app.vector_db import get_embedding_function, get_collection_name
from app.db_utils import map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from app.pipeline.validation import validate_pipeline_step
from chromadb.api import ClientAPI
from chromadb import PersistentClient
from chromadb.types import Collection
from typing import Dict, List, Any
import copy
import time
import threading
from collections import defaultdict
from app.logger import logger
import traceback
from app.services import ArtifactStore, STAGE_ARTIFACT_FIELDS, configure_schema_service, get_schema_service, load_stage_dataset, reset_schema_service


def _is_spider2_item(data_item: DataItem) -> bool:
    """Check if the data item belongs to a Spider2 series dataset."""
    return hasattr(data_item, "instance_id")


class ValueRetrievalRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _vector_db_client_dict: Dict[str, ClientAPI]
    _vector_db_collection_dict: Dict[str, Collection]
    _embedding_function: Any = None # Shared embedding function
    _thread_pool_executor: ThreadPoolExecutor = None
    _db_lock: threading.Lock
    _artifact_store: ArtifactStore = None
    _extractor_max_retry: int = 3
    _stage_config = None
    _dataset_config = None
    _vector_database_config = None
    
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
        self._llm = LLM(self._stage_config.llm)
        configure_schema_service(max_value_example_length=self._dataset_config.max_value_example_length)
        self._vector_db_client_dict = {}
        self._vector_db_collection_dict = {}
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
            )
        else:
            logger.info("Skipping embedding function initialization for Spider2 dataset")
            self._embedding_function = None
        
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self._stage_config.n_parallel)

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

    def _extract_keywords(self, data_item: DataItem) -> tuple[List[str], Dict[str, int]]:
        return extract_keywords(
            data_item.question,
            data_item.evidence,
            self._llm,
            fix_end_token=self._llm.llm_config.fix_end_token,
            extractor_max_retry=self._extractor_max_retry,
        )
    
    def _retrieve_values_for_item(self, data_item: DataItem):
        """Processes a single data item: keyword extraction + vector retrieval."""
        start_time = time.time()
        
        # 1. LLM Keyword Extraction
        keywords, token_usage = self._extract_keywords(data_item)
        data_item.question_keywords = keywords
        data_item.value_retrieval_llm_cost = token_usage
        
        # 2. Independent Keyword Embedding
        # Get embeddings once for all columns in this item
        query_embeddings = embed_keywords(
            keywords,
            self._embedding_function,
            batch_size=self._vector_database_config.batch_size,
        )
        
        # 3. Vector Retrieval for each text column (Parallelized within the item)
        collection = self._get_vector_collection(data_item.database_id)
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
        
        if column_tasks:
            # Use a local executor to avoid deadlock with the main thread pool
            with ThreadPoolExecutor(max_workers=min(len(column_tasks), self._stage_config.n_internal_parallel)) as col_executor:
                future_to_col = {
                    col_executor.submit(
                        retrieve_values_for_one_column,
                        query_embeddings, # Pass pre-computed embeddings
                        collection,
                        t_name,
                        c_name,
                        self._stage_config.n_results,
                        self._vector_database_config.lower_meta_data
                    ): (t_name, c_name) for t_name, c_name in column_tasks
                }
                
                for future in tqdm(as_completed(future_to_col), total=len(future_to_col), desc=f"Retrieving values for item {data_item.question_id}", leave=False):
                    result = future.result()
                    original_table_name = map_lower_table_name_to_original_table_name(result["table_name"], data_item.database_schema)
                    original_column_name = map_lower_column_name_to_original_column_name(result["table_name"], result["column_name"], data_item.database_schema)
                    data_item.retrieved_values[original_table_name][original_column_name] = result["values"]
        
        data_item.retrieved_values = dict(data_item.retrieved_values)
        self._update_database_schema(data_item)
        
        # 3. Update Metrics
        data_item.value_retrieval_time = time.time() - start_time
        data_item.total_time = (data_item.total_time or 0) + data_item.value_retrieval_time
        
        # Merge LLM cost
        if data_item.total_llm_cost is None:
            data_item.total_llm_cost = data_item.value_retrieval_llm_cost
        else:
            for k, v in data_item.value_retrieval_llm_cost.items():
                data_item.total_llm_cost[k] += v

    def _update_database_schema(self, data_item: DataItem):
        database_schema_after_value_retrieval = copy.deepcopy(data_item.database_schema)
        schema_service = get_schema_service()
        for table_name, column_dict in data_item.retrieved_values.items():
            for column_name, values in column_dict.items():
                schema_service.ensure_column_features(
                    data_item.database_schema,
                    table_name,
                    column_name,
                    include_value_examples=True,
                )
                original_values = data_item.database_schema["tables"][table_name]["columns"][column_name].get("value_examples") or []
                new_values = [value["value"] for value in values] + original_values
                new_values = new_values[:self._stage_config.n_results]
                database_schema_after_value_retrieval["tables"][table_name]["columns"][column_name]["value_examples"] = new_values
        data_item.database_schema_after_value_retrieval = database_schema_after_value_retrieval
    
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._vector_db_client_dict = {}
        self._vector_db_collection_dict = {}
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
        data_item.database_schema_after_value_retrieval = copy.deepcopy(data_item.database_schema)
        
        logger.info(f"Skipping value retrieval for item {data_item.question_id}")

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
                logger.error(f"Error processing data item: {e}")
                traceback.format_exc()
            
            if idx % 5 == 0:
                logger.info(f"Value Retrieval {idx} / {len(future_to_item)} completed")
                self.save_result()
            
        self.save_result(materialize_snapshot=True)
        
        # Validate that all required fields are filled
        validate_pipeline_step(self._dataset, "value_retrieval")
        
        self._clean_up()
