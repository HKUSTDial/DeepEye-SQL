from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from app.db_utils import filter_used_database_schema
from .linkers import DirectLinker, ReversedLinker, ValueLinker
from .utils import merge_schema_linking_results
from app.pipeline.validation import validate_pipeline_step
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from app.logger import logger
import time
import traceback
from app.services import ArtifactStore, STAGE_ARTIFACT_FIELDS, configure_schema_service, load_stage_dataset, reset_schema_service

class SchemaLinkingRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    _inner_thread_pool_executor: ThreadPoolExecutor = None
    
    _direct_linker: DirectLinker = None
    _reversed_linker: ReversedLinker = None
    _value_linker: ValueLinker = None
    _artifact_store: ArtifactStore = None
    _extractor_max_retry: int = 3
    _stage_config = None
    _input_save_path: str = ""
    _few_shot_examples_path: str | None = None
    _dataset_config = None
    
    def __init__(
        self,
        stage_config,
        dataset_config,
        input_save_path: str,
        few_shot_examples_path: str | None,
        extractor_max_retry: int,
    ):
        self._stage_config = stage_config
        self._dataset_config = dataset_config
        self._input_save_path = input_save_path
        self._few_shot_examples_path = few_shot_examples_path
        self._extractor_max_retry = extractor_max_retry
        self._artifact_store = ArtifactStore(
            self._stage_config.save_path,
            "schema_linking",
            STAGE_ARTIFACT_FIELDS["schema_linking"],
        )
        self._dataset, checkpoint_source = load_stage_dataset(
            load_dataset_fn=load_dataset,
            current_save_path=self._stage_config.save_path,
            fallback_load_path=self._input_save_path,
            artifact_store=self._artifact_store,
            stage_name="schema_linking",
        )
        logger.info(f"Initialized schema linking dataset from {checkpoint_source}")
        configure_schema_service(max_value_example_length=self._dataset_config.max_value_example_length)
        self._llm = LLM(self._stage_config.llm)
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self._stage_config.n_parallel)
        self._inner_thread_pool_executor = ThreadPoolExecutor(max_workers=max(1, self._stage_config.n_internal_parallel))
        self._direct_linker = DirectLinker(extractor_max_retry=self._extractor_max_retry)
        self._reversed_linker = ReversedLinker(
            few_shot_examples_path=self._few_shot_examples_path,
            extractor_max_retry=self._extractor_max_retry,
        )
        self._value_linker = ValueLinker(
            value_distance_threshold=self._stage_config.value_distance_threshold,
            extractor_max_retry=self._extractor_max_retry,
        )

    @classmethod
    def from_config(cls, app_config=None) -> "SchemaLinkingRunner":
        if app_config is None:
            from app.config import get_config

            app_config = get_config()
        return cls(
            stage_config=app_config.schema_linking_config,
            dataset_config=app_config.dataset_config,
            input_save_path=app_config.value_retrieval_config.save_path,
            few_shot_examples_path=app_config.sql_generation_config.icl_few_shot_examples_path,
            extractor_max_retry=app_config.llm_extractor_config.max_retry,
        )
    
    def _link_tables_and_columns(self, data_item: DataItem) -> None:
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Parallelize different linking methods within a single data item
        linker_tasks = {
            "direct": self._inner_thread_pool_executor.submit(self._direct_linker.link, data_item, self._llm, self._stage_config.direct_linking_sampling_budget),
            "reversed": self._inner_thread_pool_executor.submit(self._reversed_linker.link, data_item, self._llm, self._stage_config.reversed_linking_sampling_budget),
            "value": self._inner_thread_pool_executor.submit(self._value_linker.link, data_item, self._llm)
        }
        
        results = {}
        for name, future in linker_tasks.items():
            try:
                results[name] = future.result()
            except Exception as e:
                logger.error(f"Error in {name} linking for item {data_item.question_id}: {e}")
                traceback.print_exc()
                # Set to None instead of empty dict to indicate failure
                results[name] = (None, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})

        direct_linked_tables_and_columns, direct_tokens = results["direct"]
        reversed_linked_tables_and_columns, reversed_tokens = results["reversed"]
        value_linked_tables_and_columns, value_tokens = results["value"]
            
        # Accumulate token usage
        for tokens in [direct_tokens, reversed_tokens, value_tokens]:
            total_token_usage["prompt_tokens"] += tokens["prompt_tokens"]
            total_token_usage["completion_tokens"] += tokens["completion_tokens"]
            total_token_usage["total_tokens"] += tokens["total_tokens"]
        
        # Check if any linker failed (returned None) before merging
        if direct_linked_tables_and_columns is None or reversed_linked_tables_and_columns is None or value_linked_tables_and_columns is None:
            failed_linkers = []
            if direct_linked_tables_and_columns is None:
                failed_linkers.append("direct")
            if reversed_linked_tables_and_columns is None:
                failed_linkers.append("reversed")
            if value_linked_tables_and_columns is None:
                failed_linkers.append("value")
            logger.error(f"Linker(s) {failed_linkers} failed for item {data_item.question_id}, setting all linking results to None")
            data_item.direct_linked_tables_and_columns = direct_linked_tables_and_columns
            data_item.reversed_linked_tables_and_columns = reversed_linked_tables_and_columns
            data_item.value_linked_tables_and_columns = value_linked_tables_and_columns
            data_item.final_linked_tables_and_columns = None
            data_item.database_schema_after_schema_linking = None
        else:
            merged_linked_tables_and_columns = merge_schema_linking_results([
                direct_linked_tables_and_columns, 
                reversed_linked_tables_and_columns, 
                value_linked_tables_and_columns
            ])
            data_item.direct_linked_tables_and_columns = direct_linked_tables_and_columns
            data_item.reversed_linked_tables_and_columns = reversed_linked_tables_and_columns
            data_item.value_linked_tables_and_columns = value_linked_tables_and_columns
            data_item.final_linked_tables_and_columns = merged_linked_tables_and_columns
            data_item.database_schema_after_schema_linking = filter_used_database_schema(data_item.database_schema_after_value_retrieval, merged_linked_tables_and_columns)
        
        end_time = time.time()
        data_item.schema_linking_time = end_time - start_time
        data_item.schema_linking_llm_cost = total_token_usage
        data_item.total_time += data_item.schema_linking_time
        data_item.total_llm_cost = {
            "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.schema_linking_llm_cost["prompt_tokens"],
            "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.schema_linking_llm_cost["completion_tokens"],
            "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.schema_linking_llm_cost["total_tokens"],
        }
        self._eval_schema_linking_recall(data_item)
        
    def _is_linking_complete(self, data_item: DataItem) -> bool:
        """Check if schema linking step completed successfully."""
        return data_item.is_stage_complete("schema_linking")
    
    def run(self):
        future_to_item = {}
        for data_item in self._dataset:
            if self._is_linking_complete(data_item):
                # If already linked but recall is missing, evaluate it now
                if data_item.get_stage_artifact("schema_linking").final_linking_recall is None:
                    self._eval_schema_linking_recall(data_item)
                logger.info(f"Skipping data item {data_item.question_id} because it has already been linked")
                continue
            future = self._thread_pool_executor.submit(self._link_tables_and_columns, data_item)
            future_to_item[future] = data_item
        for idx, future in tqdm(enumerate(as_completed(future_to_item), start=1), total=len(future_to_item), desc="Linking tables and columns"):
            future.result()
            self._artifact_store.record_item(future_to_item[future])
            if idx % 5 == 0:
                logger.info(f"Linking tables and columns {idx} / {len(future_to_item)} completed")
                self.save_result()
        logger.info("Linking tables and columns completed")
        
        # Validate that all required fields are filled
        self._artifact_store.flush()
        validate_pipeline_step(self._dataset, "schema_linking")
        self.save_result(materialize_snapshot=True)
        
        self._clean_up()
        
    
    def save_result(self, materialize_snapshot: bool = False):
        self._artifact_store.flush()
        if materialize_snapshot:
            save_dataset(self._dataset, self._stage_config.save_path)
            self._artifact_store.cleanup()
        
    def _eval_schema_linking_recall(self, data_item: DataItem):
        # Skip recall calculation if gold_sql is missing or empty (typical for Spider2 inference)
        if not hasattr(data_item, "gold_sql") or not data_item.gold_sql or not data_item.gold_sql.strip():
            # Initialize with default zero recall or None
            default_recall = {"table_recall": 0.0, "column_recall": 0.0}
            data_item.direct_linking_recall = default_recall
            data_item.reversed_linking_recall = default_recall
            data_item.value_linking_recall = default_recall
            data_item.final_linking_recall = default_recall
            return

        gold_tables_and_columns = self._reversed_linker._extract_tables_and_columns(data_item.gold_sql, data_item.database_schema_after_value_retrieval)
        
        def _calc_recall(linked_tables_and_columns):
            """Calculate table and column recall for a linking result."""
            if linked_tables_and_columns is None:
                return 0, 0
            table_recall = 0
            column_recall = 0
            for table_name, columns in linked_tables_and_columns.items():
                if table_name in gold_tables_and_columns:
                    table_recall += 1
                    for column_name in columns:
                        if column_name in gold_tables_and_columns[table_name]:
                            column_recall += 1
            table_recall /= len(gold_tables_and_columns.keys()) if len(gold_tables_and_columns.keys()) > 0 else 1
            column_recall /= sum(len(columns) for columns in gold_tables_and_columns.values()) if sum(len(columns) for columns in gold_tables_and_columns.values()) > 0 else 1
            return table_recall, column_recall
        
        direct_table_recall, direct_column_recall = _calc_recall(data_item.direct_linked_tables_and_columns)
        reversed_table_recall, reversed_column_recall = _calc_recall(data_item.reversed_linked_tables_and_columns)
        value_table_recall, value_column_recall = _calc_recall(data_item.value_linked_tables_and_columns)
        final_table_recall, final_column_recall = _calc_recall(data_item.final_linked_tables_and_columns)
        
        data_item.direct_linking_recall = {"table_recall": direct_table_recall, "column_recall": direct_column_recall}
        data_item.reversed_linking_recall = {"table_recall": reversed_table_recall, "column_recall": reversed_column_recall}
        data_item.value_linking_recall = {"table_recall": value_table_recall, "column_recall": value_column_recall}
        data_item.final_linking_recall = {"table_recall": final_table_recall, "column_recall": final_column_recall}

    
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        if self._inner_thread_pool_executor is not None:
            self._inner_thread_pool_executor.shutdown(wait=True)
            self._inner_thread_pool_executor = None
        self._llm = None
        self._dataset = None
        self._direct_linker = None
        self._reversed_linker = None
        self._value_linker = None
        if self._artifact_store is not None:
            self._artifact_store.close()
        reset_schema_service()
