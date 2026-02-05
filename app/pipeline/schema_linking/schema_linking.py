from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.config import config, LLMConfig
from app.llm import LLM
from app.db_utils import filter_used_database_schema
from .linkers import DirectLinker, ReversedLinker, ValueLinker
from .utils import merge_schema_linking_results
from app.pipeline.validation import validate_pipeline_step
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from app.logger import logger
from pathlib import Path
import time
import traceback

class SchemaLinkingRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    _direct_linker: DirectLinker = None
    _reversed_linker: ReversedLinker = None
    _value_linker: ValueLinker = None
    
    def __init__(self):
        self._llm = LLM(config.schema_linking_config.llm)
        if Path(config.schema_linking_config.save_path).exists():
            logger.info(f"Resuming schema linking checkpoint from {config.schema_linking_config.save_path}")
            self._dataset = load_dataset(config.schema_linking_config.save_path)
        else:
            logger.info(f"Loading dataset from {config.value_retrieval_config.save_path}")
            self._dataset = load_dataset(config.value_retrieval_config.save_path)
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=config.schema_linking_config.n_parallel)
        self._direct_linker = DirectLinker()
        self._reversed_linker = ReversedLinker()
        self._value_linker = ValueLinker()
    
    def _link_tables_and_columns(self, data_item: DataItem) -> None:
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Parallelize different linking methods within a single data item
        with ThreadPoolExecutor(max_workers=min(config.schema_linking_config.n_internal_parallel, 3)) as executor:
            linker_tasks = {
                "direct": executor.submit(self._direct_linker.link, data_item, self._llm, config.schema_linking_config.direct_linking_sampling_budget),
                "reversed": executor.submit(self._reversed_linker.link, data_item, self._llm, config.schema_linking_config.reversed_linking_sampling_budget),
                "value": executor.submit(self._value_linker.link, data_item, self._llm)
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
        # Check database_schema_after_schema_linking - the final output of this step
        if not hasattr(data_item, "database_schema_after_schema_linking") or data_item.database_schema_after_schema_linking is None:
            return False
        return True
    
    def run(self):
        all_futures = []
        for data_item in self._dataset:
            if self._is_linking_complete(data_item):
                # If already linked but recall is missing, evaluate it now
                if not hasattr(data_item, "final_linking_recall") or data_item.final_linking_recall is None:
                    self._eval_schema_linking_recall(data_item)
                logger.info(f"Skipping data item {data_item.question_id} because it has already been linked")
                continue
            future = self._thread_pool_executor.submit(self._link_tables_and_columns, data_item)
            all_futures.append(future)
        for idx, future in tqdm(enumerate(as_completed(all_futures), start=1), total=len(all_futures), desc="Linking tables and columns"):
            future.result()
            if idx % 5 == 0:
                logger.info(f"Linking tables and columns {idx} / {len(all_futures)} completed")
                self.save_result()
        logger.info("Linking tables and columns completed")
        self.save_result()
        
        # Validate that all required fields are filled
        validate_pipeline_step(self._dataset, "schema_linking")
        
        self._clean_up()
        
    
    def save_result(self):
        save_dataset(self._dataset, config.schema_linking_config.save_path)
        
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
        self._llm = None
        self._dataset = None
        self._direct_linker = None
        self._reversed_linker = None
        self._value_linker = None