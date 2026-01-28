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


class OrderByNullChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        order_by_null_suggestion = self._check_order_by_null(sql)
        if order_by_null_suggestion:
            logger.info(f"[OrderByNullChecker] Found order-by-null errors in SQL: {sql}")
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            db_type = getattr(data_item, "db_type", None)
            prompt = PromptFactory.format_common_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, order_by_null_suggestion, db_type=db_type)
            
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
