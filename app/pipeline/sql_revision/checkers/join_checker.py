from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import execute_sql, get_database_schema_profile
from typing import Dict, List, Any, Optional, Tuple
import re


class JoinChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        join_suggestion = self._check_join(sql)
        if join_suggestion:
            logger.info(f"[JoinChecker] Found join errors in SQL: {sql}")
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            prompt = PromptFactory.format_common_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, join_suggestion)
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

    def _check_join(self, sql: str) -> Optional[str]:
        suggestion = None
        identifier = r'(?:`[^`]+`|\[[^\]]+\]|"[^"]+"|[\w\.]+)'
        join_pattern = re.compile(
            rf"JOIN\s+{identifier}(\s+AS\s+{identifier}){{0,1}}\s+ON(\s+{identifier}\.{identifier}\s*(=\s*{identifier}\.{identifier}(?:\s+OR\s+{identifier}\.{identifier}\s*=\s*{identifier}\.{identifier})+|IN\s+\(.*?\)))",
            re.IGNORECASE | re.DOTALL
        )
        if join_pattern.findall(sql):
            suggestion = "The SQL uses the JOIN function incorrectly, due to using `JOIN table AS T ON Ta.column1 = Tb.column2 OR Ta.column1 = Tb.column3` or `JOIN table AS T ON Ta.column1 IN`, please only keep the highest priority group of `Ta.column = Tb.column` in `OR`."
        return suggestion
