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
from pathlib import Path
import traceback

class SQLRevisionRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    _checkers: List[BaseChecker] = None
    
    def __init__(self):
        self._llm = LLM(config.sql_revision_config.llm)
        if Path(config.sql_revision_config.save_path).exists():
            logger.info(f"Resuming SQL revision checkpoint from {config.sql_revision_config.save_path}")
            self._dataset = load_dataset(config.sql_revision_config.save_path)
        else:
            logger.info(f"Loading dataset from {config.sql_generation_config.save_path}")
            self._dataset = load_dataset(config.sql_generation_config.save_path)
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=config.sql_revision_config.n_parallel)
        self._checkers: List[BaseChecker] = [
            SyntaxChecker(),
            JoinChecker(),
            OrderByLimitChecker(),
            TimeChecker(),
            SelectChecker(),
            MaxMinChecker(),
            OrderByNullChecker(),
            ResultChecker(),
        ]
        
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
                current_sql, data_item, self._llm, config.sql_revision_config.checker_sampling_budget
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
        with ThreadPoolExecutor(max_workers=min(len(unique_norms), 8)) as executor:
            future_to_norm = {
                executor.submit(self._revise_one_candidate, unique_candidates_map[norm], data_item): norm 
                for norm in unique_norms
            }
            
            # normalized_sql -> (revised_sql, tokens)
            norm_to_result = {}
            has_failure = False
            
            for future in tqdm(as_completed(future_to_norm), total=len(future_to_norm), desc=f"Revising unique candidates for item {data_item.question_id}", leave=False):
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
        all_futures = []
        for data_item in self._dataset:
            if hasattr(data_item, "sql_candidates_after_revision") and data_item.sql_candidates_after_revision is not None:
                logger.info(f"Skipping data item {data_item.question_id} because it has already been revised")
                continue
            future = self._thread_pool_executor.submit(self._revise_sql, data_item)
            all_futures.append(future)
        for idx, future in tqdm(enumerate(as_completed(all_futures), start=1), total=len(all_futures), desc="Revising SQL"):
            future.result()
            if idx % 5 == 0:
                logger.info(f"Revising SQL {idx} / {len(all_futures)} completed")
                self.save_result()
        logger.info("Revising SQL completed")
        self.save_result()
        
        # Validate that all required fields are filled
        validate_pipeline_step(self._dataset, "sql_revision")
        
        self._clean_up()
        
    def save_result(self):
        save_dataset(self._dataset, config.sql_revision_config.save_path)
        
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._llm = None
        self._dataset = None
        self._checkers = None