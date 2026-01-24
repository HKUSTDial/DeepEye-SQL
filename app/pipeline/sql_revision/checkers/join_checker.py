from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.llm_extractor import LLMExtractor
from app.db_utils import execute_sql, get_database_schema_profile
from app.config import config
from typing import Dict, List, Any, Optional, Tuple
import re


class JoinChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        join_suggestion = self._check_join(sql)
        if join_suggestion:
            logger.info(f"[JoinChecker] Found join errors in SQL: {sql}")
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            prompt = PromptFactory.format_common_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, join_suggestion)
            
            extractor = LLMExtractor()
            results, total_token_usage = extractor.extract_with_retry(
                llm=llm,
                messages=[{"role": "user", "content": prompt}],
                rule_parser=self._parse_llm_response,
                fix_end_token=config.sql_revision_config.llm.fix_end_token,
                end_token="</result>",
                n=1
            )
            
            if results:
                return results[0], total_token_usage
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
