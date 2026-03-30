from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.llm_extractor import LLMExtractor
from app.config import config
from typing import Dict, Optional, Tuple
import re


class MaxMinChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        max_min_suggestion = self._check_max_min(sql)
        if max_min_suggestion:
            logger.info(f"[MaxMinChecker] Found max-min errors in SQL: {sql}")
            db_type = getattr(data_item, "db_type", None)
            
            # Define prompt format function for MaxMinChecker
            def prompt_format_func(schema_profile: str) -> str:
                return PromptFactory.format_common_checker_prompt(
                    schema_profile, 
                    data_item.question, 
                    data_item.evidence, 
                    sql, 
                    max_min_suggestion, 
                    db_type=db_type
                )
            
            final_prompt, level = self._check_and_revise_with_progressive_stripping(data_item, llm, prompt_format_func)
            
            if final_prompt is None:
                logger.error(f"CRITICAL: Even minimal MaxMinChecker prompt for item {data_item.question_id} exceeds token limit. Returning original SQL.")
                return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            
            extractor = LLMExtractor()
            results, total_token_usage = extractor.extract_with_retry(
                llm=llm,
                messages=[{"role": "user", "content": final_prompt}],
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
         
