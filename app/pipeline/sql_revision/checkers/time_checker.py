from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import execute_sql, get_database_schema_profile
from typing import Dict, List, Any, Optional, Tuple
import re


class TimeChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        time_revised_sql = self._check_time(sql)
        if time_revised_sql:
            logger.info(f"[TimeChecker] Found time errors in SQL: {sql}")
            return time_revised_sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _check_time(self, sql: str) -> Optional[str]:
        res = re.sub(
            r"(strftime *\([^\(]*?\) *[>=<]+ *)(\d{4,})",
            r"\1'\2'",
            sql
        )
        if res != sql:
            return res
        return None