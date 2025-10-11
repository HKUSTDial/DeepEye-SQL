from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import execute_sql, get_database_schema_profile
from typing import Dict, List, Any, Optional, Tuple
import re


class OrderByNullChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        order_by_null_suggestion = self._check_order_by_null(sql)
        if order_by_null_suggestion:
            logger.info(f"[OrderByNullChecker] Found order-by-null errors in SQL: {sql}")
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            prompt = PromptFactory.format_common_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, order_by_null_suggestion)
            parsed_sql_candidate = None
            total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            while not parsed_sql_candidate and sampling_budget > 0:
                responses, token_usage = llm.ask([{"role": "user", "content": prompt}], n=1, stop=["</result>"])
                response = responses[0].content.strip()
                total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
                total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
                total_token_usage["total_tokens"] += token_usage["total_tokens"]
                try:
                    parsed_sql_candidate = self._parse_llm_response(response)
                    if parsed_sql_candidate:
                        return parsed_sql_candidate, total_token_usage
                except Exception as e:
                    logger.error(f"Error parsing LLM response: {e}")
                    logger.debug(f"Response content: {response}")
                sampling_budget -= 1
                continue
            return sql, total_token_usage
        else:
            return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _check_order_by_null(self, sql: str) -> Optional[str]:
        suggestion = None
        inn = re.findall(r"ORDER BY .*?(?<!DESC )LIMIT +\d+;{0,1}", sql)
        if not inn:
            return None
        
        for x in inn:
            if re.findall(r"SUM\(|COUNT\(", x):
                return None
        suggestion = ""
        for x in inn:
            suggestion += f"Please add `IS NOT NULL` condition **in the WHERE clause** for the ORDER BY column: {x}\n"
        return suggestion
