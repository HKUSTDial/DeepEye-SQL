from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from concurrent.futures import ThreadPoolExecutor, as_completed
from .checkers import BaseChecker, ResultChecker, SyntaxChecker, SelectChecker, MaxMinChecker, OrderByLimitChecker, OrderByNullChecker, JoinChecker, TimeChecker
from app.config import config
from app.pipeline.validation import validate_pipeline_step
import time
from app.logger import logger
from tqdm import tqdm
from typing import List, Dict
import traceback
from app.services import ArtifactStore, STAGE_ARTIFACT_FIELDS, configure_execution_service, configure_schema_service, load_stage_dataset, reset_execution_service, reset_schema_service

class SQLRevisionRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    _checkers: List[BaseChecker] = None
    _artifact_store: ArtifactStore = None
    _extractor_max_retry: int = 3
    _stage_config = None
    _input_save_path: str = ""
    _dataset_config = None
    
    def __init__(self, stage_config=None, dataset_config=None, input_save_path: str | None = None, extractor_max_retry: int | None = None):
        self._stage_config = stage_config or config.sql_revision_config
        self._dataset_config = dataset_config or config.dataset_config
        self._input_save_path = input_save_path or config.sql_generation_config.save_path
        self._extractor_max_retry = config.llm_extractor_config.max_retry if extractor_max_retry is None else extractor_max_retry
        self._llm = LLM(self._stage_config.llm)
        configure_schema_service(max_value_example_length=self._dataset_config.max_value_example_length)
        configure_execution_service(
            default_timeout=self._dataset_config.sql_execution_timeout,
            bigquery_credential_path=self._dataset_config.bigquery_credential_path,
            snowflake_credential_path=self._dataset_config.snowflake_credential_path,
        )
        self._artifact_store = ArtifactStore(
            self._stage_config.save_path,
            "sql_revision",
            STAGE_ARTIFACT_FIELDS["sql_revision"],
        )
        self._dataset, checkpoint_source = load_stage_dataset(
            load_dataset_fn=load_dataset,
            current_save_path=self._stage_config.save_path,
            fallback_load_path=self._input_save_path,
            artifact_store=self._artifact_store,
            stage_name="sql_revision",
        )
        logger.info(f"Initialized SQL revision dataset from {checkpoint_source}")
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self._stage_config.n_parallel)
        extractor_max_retry = self._extractor_max_retry
        
        # Initialize checkers based on config or default list
        checker_map = {
            "SyntaxChecker": lambda: SyntaxChecker(extractor_max_retry=extractor_max_retry),
            "JoinChecker": lambda: JoinChecker(extractor_max_retry=extractor_max_retry),
            "OrderByLimitChecker": lambda: OrderByLimitChecker(extractor_max_retry=extractor_max_retry),
            "TimeChecker": lambda: TimeChecker(extractor_max_retry=extractor_max_retry),
            "SelectChecker": lambda: SelectChecker(extractor_max_retry=extractor_max_retry),
            "MaxMinChecker": lambda: MaxMinChecker(extractor_max_retry=extractor_max_retry),
            "OrderByNullChecker": lambda: OrderByNullChecker(extractor_max_retry=extractor_max_retry),
            "ResultChecker": lambda: ResultChecker(extractor_max_retry=extractor_max_retry),
        }
        
        if self._stage_config.checkers:
            self._checkers = []
            for checker_name in self._stage_config.checkers:
                if checker_name in checker_map:
                    self._checkers.append(checker_map[checker_name]())
                else:
                    logger.warning(f"Unknown checker name in config: {checker_name}")
        else:
            # Default checkers if none specified in config
            self._checkers: List[BaseChecker] = [
                SyntaxChecker(extractor_max_retry=extractor_max_retry),
                JoinChecker(extractor_max_retry=extractor_max_retry),
                OrderByLimitChecker(extractor_max_retry=extractor_max_retry),
                TimeChecker(extractor_max_retry=extractor_max_retry),
                SelectChecker(extractor_max_retry=extractor_max_retry),
                MaxMinChecker(extractor_max_retry=extractor_max_retry),
                OrderByNullChecker(extractor_max_retry=extractor_max_retry),
                ResultChecker(extractor_max_retry=extractor_max_retry),
            ]
        
        logger.info(f"Using checkers: {[checker.__class__.__name__ for checker in self._checkers]}")
        
    def _normalize_sql(self, sql: str) -> str:
        """Simple normalization to handle whitespace and case differences."""
        if not sql:
            return ""
        return " ".join(sql.split()).strip().lower()

    def _revise_one_candidate(self, sql: str, data_item: DataItem) -> tuple[str, Dict[str, int]]:
        """Run all checkers sequentially for a single SQL candidate."""
        total_tokens = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        current_sql = sql
        for checker in self._checkers:
            current_sql, tokens = checker.check_and_revise(
                current_sql, data_item, self._llm, self._stage_config.checker_sampling_budget
            )
            total_tokens["prompt_tokens"] += tokens["prompt_tokens"]
            total_tokens["completion_tokens"] += tokens["completion_tokens"]
            total_tokens["total_tokens"] += tokens["total_tokens"]
        return current_sql, total_tokens

    def _revise_sql(self, data_item: DataItem) -> None:
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        sql_candidates = data_item.sql_candidates
        
        # If sql_candidates is empty or None, skip revision and set result to None
        if not sql_candidates:
            logger.error(f"sql_candidates is empty or None for item {data_item.question_id}, setting sql_candidates_after_revision to None")
            data_item.sql_candidates_after_revision = None
            data_item.sql_revision_time = time.time() - start_time
            data_item.sql_revision_llm_cost = total_token_usage
            data_item.total_time += data_item.sql_revision_time
            data_item.total_llm_cost = {
                "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_revision_llm_cost["prompt_tokens"],
                "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_revision_llm_cost["completion_tokens"],
                "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_revision_llm_cost["total_tokens"],
            }
            return
        
        # Deduplicate candidates using normalized SQL as key
        # normalized_sql -> original_sql
        unique_candidates_map = {}
        for sql in sql_candidates:
            norm_sql = self._normalize_sql(sql)
            if norm_sql not in unique_candidates_map:
                unique_candidates_map[norm_sql] = sql
        
        # Parallelize the revision of UNIQUE candidates only
        unique_norms = list(unique_candidates_map.keys())
        with ThreadPoolExecutor(max_workers=min(len(unique_norms), self._stage_config.n_internal_parallel)) as executor:
            future_to_norm = {
                executor.submit(self._revise_one_candidate, unique_candidates_map[norm], data_item): norm 
                for norm in unique_norms
            }
            
            # normalized_sql -> (revised_sql, tokens)
            norm_to_result = {}
            has_failure = False
            
            for future in tqdm(as_completed(future_to_norm), total=len(future_to_norm), desc=f"Revising unique candidates for item {data_item.question_id}", disable=True):
                norm = future_to_norm[future]
                try:
                    revised_sql, tokens = future.result()
                    norm_to_result[norm] = (revised_sql, tokens)
                except Exception as e:
                    logger.error(f"Error revising SQL candidate for item {data_item.question_id}: {e}")
                    traceback.print_exc()
                    # Mark as failed instead of fallback
                    norm_to_result[norm] = (None, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
                    has_failure = True

        # Accumulate tokens only from the actual unique API calls
        for _, tokens in norm_to_result.values():
            total_token_usage["prompt_tokens"] += tokens["prompt_tokens"]
            total_token_usage["completion_tokens"] += tokens["completion_tokens"]
            total_token_usage["total_tokens"] += tokens["total_tokens"]
        
        # If any revision failed, set entire result to None
        if has_failure:
            logger.error(f"Some SQL revisions failed for item {data_item.question_id}, setting sql_candidates_after_revision to None")
            data_item.sql_candidates_after_revision = None
        else:
            # Map results back to the original candidates list (preserving order and duplicates)
            final_revised_candidates = []
            for sql in sql_candidates:
                norm = self._normalize_sql(sql)
                revised_sql, _ = norm_to_result[norm]
                final_revised_candidates.append(revised_sql)
            data_item.sql_candidates_after_revision = final_revised_candidates
        data_item.sql_revision_time = time.time() - start_time
        data_item.sql_revision_llm_cost = total_token_usage
        data_item.total_time += data_item.sql_revision_time
        data_item.total_llm_cost = {
            "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_revision_llm_cost["prompt_tokens"],
            "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_revision_llm_cost["completion_tokens"],
            "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_revision_llm_cost["total_tokens"],
        }
        
    def run(self):
        future_to_item = {}
        for data_item in self._dataset:
            if data_item.is_stage_complete("sql_revision"):
                logger.info(f"Skipping data item {data_item.question_id} because it has already been revised")
                continue
            future = self._thread_pool_executor.submit(self._revise_sql, data_item)
            future_to_item[future] = data_item
        for idx, future in tqdm(enumerate(as_completed(future_to_item), start=1), total=len(future_to_item), desc="Revising SQL"):
            future.result()
            self._artifact_store.record_item(future_to_item[future])
            if idx % 5 == 0:
                logger.info(f"Revising SQL {idx} / {len(future_to_item)} completed")
                self.save_result()
        logger.info("Revising SQL completed")
        self.save_result(materialize_snapshot=True)
        
        # Validate that all required fields are filled
        validate_pipeline_step(self._dataset, "sql_revision")
        
        self._clean_up()
        
    def save_result(self, materialize_snapshot: bool = False):
        self._artifact_store.flush()
        if materialize_snapshot:
            save_dataset(self._dataset, self._stage_config.save_path)
            self._artifact_store.cleanup()
        
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        reset_execution_service()
        reset_schema_service()
        self._llm = None
        self._dataset = None
        self._checkers = None
