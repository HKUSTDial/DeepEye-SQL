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


class OrderByLimitChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        order_by_limit_suggestion = self._check_order_by_limit(sql)
        if order_by_limit_suggestion:
            logger.info(f"[OrderByLimitChecker] Found order-by-limit errors in SQL: {sql}")
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            prompt = PromptFactory.format_common_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, order_by_limit_suggestion)
            
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

    def _check_order_by_limit(self, sql: str) -> Optional[str]:
        suggestion = None
        identifier = r'(?:`[^`]+`|\[[^\]]+\]|"[^"]+"|[\w\.]+)'
        order_by_pattern = re.compile(
            rf"ORDER BY ((MIN|MAX)\(\s*({identifier})\s*\)).*? LIMIT \d+",
            re.IGNORECASE | re.DOTALL
        )
        res = order_by_pattern.search(sql)
        if res:
            suggestion = f"The SQL uses the ORDER BY function incorrectly, using MIN/MAX in ORDER BY caluse is incrorrect (`{res.group()}`), please correct the SQL. If the SQL contains GROUP BY, please judge whether the content of `{res.groups()[0]}` needs to use `SUM({res.groups()[2]})`."
        return suggestion
