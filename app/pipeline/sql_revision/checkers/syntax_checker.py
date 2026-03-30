from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.llm_extractor import LLMExtractor
from app.config import config
from typing import Dict, List, Tuple
from collections import Counter
from app.services import get_execution_service


class SyntaxChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        execution_service = get_execution_service()
        execution_result = execution_service.execute(data_item, sql)
        if execution_result.result_type in ["success", "empty_result", "all_null_result"]:
            return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        else:
            db_type = getattr(data_item, "db_type", None)
            
            # Define prompt format function for SyntaxChecker
            def prompt_format_func(schema_profile: str) -> str:
                return PromptFactory.format_execution_checker_prompt(
                    schema_profile, 
                    data_item.question, 
                    data_item.evidence, 
                    sql, 
                    execution_result.result_table_str, 
                    db_type=db_type
                )
            
            final_prompt, level = self._check_and_revise_with_progressive_stripping(data_item, llm, prompt_format_func)
            
            if final_prompt is None:
                logger.error(f"CRITICAL: Even minimal SyntaxChecker prompt for item {data_item.question_id} exceeds token limit. Returning original SQL.")
                return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            
            extractor = LLMExtractor()
            all_sql_candidates, total_token_usage = extractor.extract_with_retry(
                llm=llm,
                messages=[{"role": "user", "content": final_prompt}],
                rule_parser=self._parse_llm_response,
                fix_end_token=config.sql_revision_config.llm.fix_end_token,
                end_token="</result>",
                n=sampling_budget
            )
            
            selected_sql_candidate = self._select_sql_candidate(all_sql_candidates, data_item)
            if selected_sql_candidate:
                return selected_sql_candidate, total_token_usage
            else:
                return sql, total_token_usage
    
    def _select_sql_candidate(self, all_sql_candidates: List[str], data_item: DataItem) -> str:
        execution_service = get_execution_service()
        valid_sql_candidates = []
        for sql_candidate in all_sql_candidates:
            execution_result = execution_service.execute(data_item, sql_candidate)
            if execution_result.result_type in ["success", "empty_result", "all_null_result"]:
                valid_sql_candidates.append((sql_candidate, execution_service.hash_result(data_item, execution_result.result_rows)))
        
        if len(valid_sql_candidates) == 0:
            return None
        
        counter = Counter(execution_result for _, execution_result in valid_sql_candidates)
        return max(valid_sql_candidates, key=lambda x: counter[x[1]])[0]
            
