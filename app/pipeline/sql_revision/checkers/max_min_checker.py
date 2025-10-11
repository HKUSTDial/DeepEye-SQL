from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import execute_sql, get_database_schema_profile
from typing import Dict, List, Any, Optional, Tuple
import re


class MaxMinChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        max_min_suggestion = self._check_max_min(sql)
        if max_min_suggestion:
            logger.info(f"[MaxMinChecker] Found max-min errors in SQL: {sql}")
            database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
            prompt = PromptFactory.format_common_checker_prompt(database_schema_profile, data_item.question, data_item.evidence, sql, max_min_suggestion)
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

    def _check_max_min(self, sql: str) -> Optional[str]:
        identifier = r'(?:`[^`]+`|\[[^\]]+\]|"[^"]+"|[\w\.]+)'
        max_min_pattern = re.compile(
            rf"=\s*\(\s*SELECT\s*(MAX|MIN)\s*\(\s*({identifier})\s*\)\s*FROM\s*({identifier})",
            re.IGNORECASE | re.DOTALL
        )
        fun_amb = max_min_pattern.findall(sql)
        order_amb = set(re.findall(r"= (\(SELECT .* LIMIT \d\))", sql, re.IGNORECASE | re.DOTALL))
        select_amb_pattern = re.compile(
            rf"^SELECT[^\(\)]*? ((MIN|MAX)\(\s*{identifier}\s*\)).*?LIMIT 1",
            re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        select_amb = set(select_amb_pattern.findall(sql))
        
        suggestions = []
        
        for fun in fun_amb:
            fuc, col, table = fun
            order = "DESC" if fuc == "MAX" else "ASC"
            suggestion = f"WHERE {col} = (SELECT {fuc}({col}) FROM {table}): Please use ORDER BY {table}.{col} {order} LIMIT 1 instead of nested SQL"
            suggestions.append(suggestion)
            
        for fun in order_amb:
            suggestions.append(f"{fun}: Please use JOIN instead of nested SQL")
        
        for fun in select_amb:
            suggestions.append(f"{fun[0]}: {fun[1]} function is redundant due to LIMIT clause, please use ORDER BY + LIMIT instead")
        
        if len(suggestions) > 0:
            return "\n".join([ f"{idx+1}. {suggestion}" for idx, suggestion in enumerate(suggestions)])
        return None
         
