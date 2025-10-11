from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import execute_sql, get_database_schema_profile
from typing import Dict, List, Any, Optional, Tuple
import re
from collections import Counter


class SyntaxChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        # execute the sql
        execution_result = execute_sql(data_item.database_path, sql)
        if execution_result.result_type in ["success", "empty_result", "all_null_result"]:
            return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        else:
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            prompt = PromptFactory.format_execution_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, execution_result.result_table_str)
            total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            all_sql_candidates = []
            while len(all_sql_candidates) < sampling_budget:
                responses, token_usage = llm.ask([{"role": "user", "content": prompt}], n=sampling_budget - len(all_sql_candidates), stop=["</result>"])
                for response in responses:
                    response = response.content.strip()
                    try:
                        parsed_sql_candidate = self._parse_llm_response(response)
                        if parsed_sql_candidate:
                            all_sql_candidates.append(parsed_sql_candidate)
                    except Exception as e:
                        logger.error(f"Error parsing LLM response: {e}")
                        logger.debug(f"Response content: {response}")
                        continue
                total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
                total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
                total_token_usage["total_tokens"] += token_usage["total_tokens"]
            selected_sql_candidate = self._select_sql_candidate(all_sql_candidates, data_item)
            if selected_sql_candidate:
                return selected_sql_candidate, total_token_usage
            else:
                return sql, total_token_usage
    
    def _select_sql_candidate(self, all_sql_candidates: List[str], data_item: DataItem) -> str:
        valid_sql_candidates = []
        for sql_candidate in all_sql_candidates:
            execution_result = execute_sql(data_item.database_path, sql_candidate)
            if execution_result.result_type in ["success", "empty_result", "all_null_result"]:
                valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
        
        if len(valid_sql_candidates) == 0:
            return None
        
        counter = Counter(execution_result for _, execution_result in valid_sql_candidates)
        return max(valid_sql_candidates, key=lambda x: counter[x[1]])[0]
            