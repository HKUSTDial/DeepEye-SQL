import token
from tenacity import retry
from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import execute_sql, get_database_schema_profile, measure_execution_time
from .utils import geometric_median
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.config import config
import numpy as np
import re
import json
from collections import Counter
from tqdm import tqdm
import time


class BRSelectionRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    def __init__(self):
        self._llm = LLM(config.sql_selection_config.llm)
        self._dataset = load_dataset(config.sql_revision_config.save_path)
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=config.sql_selection_config.n_parallel)
    
    def _parse_llm_response(self, response: str) -> Optional[List[Dict[str, Any]]]:
        """
        Parse the llm response and return the eval scores.
        """
        # restore the stop token: </result>
        response += "</result>"
        
        try:
            answer_match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
            if not answer_match:
                logger.warning("No <result> tag found in LLM response")
                return None
            answer_content = answer_match.group(1).strip().upper()
            logger.info(f"Parsed LLM response: {answer_content}")
            # check the answer content is 'A', 'B', or 'TIE'
            if answer_content not in ["A", "B", "TIE"]:
                logger.error("Answer content is not 'A', 'B', or 'TIE'")
                return None
            return answer_content
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            logger.debug(f"Response content: {response}")
            return None
        
    def _get_top_k_sql_candidates(self, data_item: DataItem) -> List[Tuple[str, str]]:
        valid_sql_candidates = []
        sql_map_to_result_str = {}
        for sql_candidate in data_item.sql_candidates_after_revision:
            execution_result = execute_sql(data_item.database_path, sql_candidate)
            if execution_result.result_rows is not None and len(execution_result.result_rows) > 0:
                valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
                sql_map_to_result_str[sql_candidate] = execution_result.result_table_str
        
        if len(valid_sql_candidates) == 0:
            logger.warning("No successful SQL candidates, backing to SQL candidates with not none result_rows")
            for sql_candidate in data_item.sql_candidates_after_revision:
                execution_result = execute_sql(data_item.database_path, sql_candidate)
                if execution_result.result_rows is not None:
                    valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
                    sql_map_to_result_str[sql_candidate] = execution_result.result_table_str
                    
        if len(valid_sql_candidates) == 0:
            return []
        
        counter = Counter(execution_result for _, execution_result in valid_sql_candidates)
        
        deduplicated_valid_sql_candidates = []
        seen_result_set = set()
        for sql_candidate, execution_result in valid_sql_candidates:
            if execution_result not in seen_result_set:
                execution_time = measure_execution_time(data_item.database_path, sql_candidate)
                deduplicated_valid_sql_candidates.append((sql_candidate, sql_map_to_result_str[sql_candidate], counter[execution_result] / len(valid_sql_candidates), execution_time))
                seen_result_set.add(execution_result)
        valid_sql_candidates = deduplicated_valid_sql_candidates
        
        top_k_sql_candidates = sorted(valid_sql_candidates, key=lambda x: (x[2], -x[3]), reverse=True)[:config.sql_selection_config.filter_top_k_sql]
        
        return top_k_sql_candidates
    
    def _get_pair_sqls_to_eval(self, top_k_sql_candidates: List[Tuple[str, str, float, float]]) -> List[Tuple[Tuple[str, str, float, float], Tuple[str, str, float, float]]]:
        """
        Get the pair of sqls to eval.
        """
        pair_sqls_to_eval = []
        for first_idx, first_sql in enumerate(top_k_sql_candidates):
            for second_idx, second_sql in enumerate(top_k_sql_candidates):
                if first_idx < second_idx:
                    pair_sqls_to_eval.append(
                        (first_sql, second_sql)
                    )
        return pair_sqls_to_eval
    
    def _compare_sqls(self, sql_a: str, execution_result_table_str_a: str, sql_b: str, execution_result_table_str_b: str, data_item: DataItem) -> Tuple[List[str], Dict[str, int]]:
        """
        Compare the two sqls.
        """
        votes = []
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
        prompt = PromptFactory.format_br_pair_selection_prompt(database_schema_profile, data_item.question, data_item.evidence, sql_a, execution_result_table_str_a, sql_b, execution_result_table_str_b)
        
        
        # Previous implementation
        # while len(votes) < config.sql_selection_config.evaluator_sampling_budget:
        #     try:
        #         responses, token_usage = self._llm.ask([{"role": "user", "content": prompt}], n=config.sql_selection_config.evaluator_sampling_budget - len(votes), stop=["</result>"])
        #         for response in responses:
        #             parsed_response = self._parse_llm_response(response.content)
        #             if parsed_response:
        #                 votes.append(parsed_response)
        #         total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
        #         total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
        #         total_token_usage["total_tokens"] += token_usage["total_tokens"]
        #     except Exception as e:
        #         logger.error(f"Error parsing LLM response: {e}")
        #         logger.debug(f"Response content: {response.content}")
        #         continue
        
        # New implementation
        # First, we calculate a temperatures list with length of config.sql_selection_config.evaluator_sampling_budget
        temperatures = np.linspace(0.1, 1.0, config.sql_selection_config.evaluator_sampling_budget).tolist()
        temperature_map_to_votes = {}
        while len(temperature_map_to_votes) < config.sql_selection_config.evaluator_sampling_budget:
            temperature = temperatures.pop(0)
            try:
                responses, token_usage = self._llm.ask([{"role": "user", "content": prompt}], n=1, stop=["</result>"], temperature=temperature)
                parsed_response = self._parse_llm_response(responses[0].content)
                if parsed_response:
                    temperature_map_to_votes[temperature] = parsed_response
                    total_token_usage["prompt_tokens"] = token_usage["prompt_tokens"]   # the prompt tokens is the same for all temperatures
                    total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
                else:
                    temperatures.insert(0, temperature)
            except Exception as e:
                logger.error(f"Error parsing LLM response: {e}")
                logger.debug(f"Response content: {responses[0].content}")
                temperatures.insert(0, temperature)
                continue
        total_token_usage["total_tokens"] = total_token_usage["prompt_tokens"] + total_token_usage["completion_tokens"]
        votes = [temperature_map_to_votes[temperature] for temperature in sorted(temperature_map_to_votes.keys())]
        
        return votes, total_token_usage
    
    def _update_win_matrix(self, sql_a_idx: int, sql_b_idx: int, votes: List[str], win_matrix: np.ndarray) -> None:
        """
        Update the win matrix.
        """
        for voter_idx, vote in enumerate(votes):
            if vote == "A":
                win_matrix[sql_a_idx, sql_b_idx, voter_idx] = 1
                win_matrix[sql_b_idx, sql_a_idx, voter_idx] = 0
            elif vote == "B":
                win_matrix[sql_b_idx, sql_a_idx, voter_idx] = 1
                win_matrix[sql_a_idx, sql_b_idx, voter_idx] = 0
            elif vote == "TIE":
                win_matrix[sql_a_idx, sql_b_idx, voter_idx] = 0.5
                win_matrix[sql_b_idx, sql_a_idx, voter_idx] = 0.5
            else:
                logger.error(f"Invalid vote: {vote}")
                raise ValueError(f"Invalid vote: {vote}")
    
    def _compute_robust_win_matrix(self, win_matrix: np.ndarray) -> float:
        """
        Calculate the robust vote.
        """
        sql_count, _, _ = win_matrix.shape
        robust_win_matrix = np.zeros((sql_count, sql_count))
        for sql_a_idx in range(sql_count):
            for sql_b_idx in range(sql_count):
                if sql_a_idx != sql_b_idx:
                    win_prob = np.mean(win_matrix[sql_a_idx, sql_b_idx, :])
                    robust_win_matrix[sql_a_idx, sql_b_idx] = win_prob
        return robust_win_matrix
    
    def _select_best_sql(self, data_item: DataItem) -> None:
        """
        Select the best sql based on the eval scores.
        """
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        data_item.top_k_sql_eval_scores = {}
        
        top_k_sql_candidates = self._get_top_k_sql_candidates(data_item)
        
        if len(top_k_sql_candidates) == 0:
            logger.warning("No valid SQL candidates, backing to top-1 SQL")
            data_item.final_selected_sql = data_item.sql_candidates_after_revision[0] if data_item.sql_candidates_after_revision else "Error"
            data_item.sql_selection_time = time.time() - start_time
            data_item.sql_selection_llm_cost = total_token_usage
            data_item.total_time += data_item.sql_selection_time
            data_item.total_llm_cost = {
                "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_selection_llm_cost["prompt_tokens"],
                "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_selection_llm_cost["completion_tokens"],
                "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_selection_llm_cost["total_tokens"],
            }
            return
        
        if len(top_k_sql_candidates) == 1:
            logger.info("Only one valid SQL candidate, directly select it")
            data_item.final_selected_sql = top_k_sql_candidates[0][0]
            data_item.sql_selection_time = time.time() - start_time
            data_item.sql_selection_llm_cost = total_token_usage
            data_item.total_time += data_item.sql_selection_time
            data_item.total_llm_cost = {
                "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_selection_llm_cost["prompt_tokens"],
                "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_selection_llm_cost["completion_tokens"],
                "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_selection_llm_cost["total_tokens"],
            }
            return
        
        # shortcut case
        # if the consistency score of top-1 SQL is larger than a threshold, directly select it
        if top_k_sql_candidates[0][2] >= config.sql_selection_config.shortcut_consistency_score_threshold:
        # if top_k_sql_candidates[0][2] - top_k_sql_candidates[1][2] >= config.sql_selection_config.shortcut_consistency_score_threshold:
            logger.info(f"Top-1 SQL candidate has a large consistency score: {top_k_sql_candidates[0][2]}, directly select it")
            # logger.info(f"Top-1 SQL candidate has a larger consistency score than top-2 SQL candidate ({top_k_sql_candidates[0][2]} vs. {top_k_sql_candidates[1][2]}), directly select the top-1 SQL")
            data_item.final_selected_sql = top_k_sql_candidates[0][0]
            data_item.sql_selection_time = time.time() - start_time
            data_item.sql_selection_llm_cost = total_token_usage
            data_item.total_time += data_item.sql_selection_time
            data_item.total_llm_cost = {
                "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_selection_llm_cost["prompt_tokens"],
                "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_selection_llm_cost["completion_tokens"],
                "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_selection_llm_cost["total_tokens"],
            }
            return
        
        # using pair-wise comparison to select the best sql
        win_matrix = np.zeros((len(top_k_sql_candidates), len(top_k_sql_candidates), config.sql_selection_config.evaluator_sampling_budget))
        sql_to_idx = {sql[0]: idx for idx, sql in enumerate(top_k_sql_candidates)}
        pair_sqls_to_eval = self._get_pair_sqls_to_eval(top_k_sql_candidates)
        for sql_a, sql_b in pair_sqls_to_eval:
            votes, token_usage = self._compare_sqls(sql_a[0], sql_a[1], sql_b[0], sql_b[1], data_item)
            self._update_win_matrix(sql_to_idx[sql_a[0]], sql_to_idx[sql_b[0]], votes, win_matrix)
            total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
            total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
            total_token_usage["total_tokens"] += token_usage["total_tokens"]
        
        robust_win_matrix = self._compute_robust_win_matrix(win_matrix)
        ranking_scores = np.mean(robust_win_matrix, axis=1)
        score_weights = np.array([sql[2] for sql in top_k_sql_candidates]) / np.sum(np.array([sql[2] for sql in top_k_sql_candidates]))
        ranking_scores = ranking_scores * score_weights
        ranking = np.argsort(-ranking_scores)
        data_item.final_selected_sql = top_k_sql_candidates[ranking[0]][0]
        
        data_item.sql_selection_time = time.time() - start_time
        data_item.sql_selection_llm_cost = total_token_usage
        data_item.total_time += data_item.sql_selection_time
        data_item.total_llm_cost = {
            "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_selection_llm_cost["prompt_tokens"],
            "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_selection_llm_cost["completion_tokens"],
            "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_selection_llm_cost["total_tokens"],
        }
    
    def run(self):
        all_futures = []
        for data_item in self._dataset:
            future = self._thread_pool_executor.submit(self._select_best_sql, data_item)
            all_futures.append(future)
        for idx, future in tqdm(enumerate(as_completed(all_futures), start=1), total=len(all_futures), desc="Selecting Best SQL"):
            future.result()
            if idx % 20 == 0:
                logger.info(f"Selecting Best SQL {idx} / {len(all_futures)} completed")
                self.save_result()
        logger.info("Selecting Best SQL completed")
        self.save_result()
        self._clean_up()

    def save_result(self):
        save_dataset(self._dataset, config.sql_selection_config.save_path)

    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._llm = None
        self._dataset = None