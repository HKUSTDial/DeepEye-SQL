from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from concurrent.futures import ThreadPoolExecutor, as_completed
from .checkers import BaseChecker, ResultChecker, SyntaxChecker, SelectChecker, MaxMinChecker, OrderByLimitChecker, OrderByNullChecker, JoinChecker, TimeChecker
from app.config import config
import time
from app.logger import logger
from tqdm import tqdm
from typing import List


class SQLRevisionRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    _checkers: List[BaseChecker] = None
    
    def __init__(self):
        self._llm = LLM(config.sql_revision_config.llm)
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
        
    def _revise_sql(self, data_item: DataItem) -> None:
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        sql_candidates = data_item.sql_candidates
        revised_sql_candidates = []
        for sql in sql_candidates:
            for checker in self._checkers:
                sql, tokens = checker.check_and_revise(sql, data_item, self._llm, config.sql_revision_config.checker_sampling_budget)
                total_token_usage["prompt_tokens"] += tokens["prompt_tokens"]
                total_token_usage["completion_tokens"] += tokens["completion_tokens"]
                total_token_usage["total_tokens"] += tokens["total_tokens"]
            revised_sql_candidates.append(sql)
        
        data_item.sql_candidates_after_revision = revised_sql_candidates
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
            future = self._thread_pool_executor.submit(self._revise_sql, data_item)
            all_futures.append(future)
        for idx, future in tqdm(enumerate(as_completed(all_futures), start=1), total=len(all_futures), desc="Revising SQL"):
            future.result()
            if idx % 20 == 0:
                logger.info(f"Revising SQL {idx} / {len(all_futures)} completed")
                self.save_result()
        logger.info("Revising SQL completed")
        self.save_result()
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