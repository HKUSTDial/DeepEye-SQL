from .base import BaseChecker
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from typing import Dict, Optional, Tuple
import re


class SelectChecker(BaseChecker):
    
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        select = re.findall(r"^SELECT.*?\|\| ' ' \|\| .*?FROM", sql, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if select:
            sql = sql.replace("|| ' ' ||", ', ')
            sql = sql.replace("|| ', ' ||", ', ')
            
        select_suggestion = self._check_select(sql)
        if select_suggestion:
            logger.info(f"[SelectChecker] Found select errors in SQL: {sql}")
            db_type = getattr(data_item, "db_type", None)
            
            # Define prompt format function for SelectChecker
            def prompt_format_func(schema_profile: str) -> str:
                return PromptFactory.format_common_checker_prompt(
                    schema_profile, 
                    data_item.question, 
                    data_item.evidence, 
                    sql, 
                    select_suggestion, 
                    db_type=db_type
                )
            
            final_prompt, level = self._check_and_revise_with_progressive_stripping(data_item, llm, prompt_format_func)
            
            if final_prompt is None:
                logger.error(f"CRITICAL: Even minimal SelectChecker prompt for item {data_item.question_id} exceeds token limit. Returning original SQL.")
                return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            
            extractor = self._get_extractor()
            results, total_token_usage = extractor.extract_with_retry(
                llm=llm,
                messages=[{"role": "user", "content": final_prompt}],
                rule_parser=self._parse_llm_response,
                fix_end_token=llm.llm_config.fix_end_token,
                end_token="</result>",
                n=1
            )
            
            if results:
                return results[0], total_token_usage
            return sql, total_token_usage
        else:
            return sql, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _check_select(self, sql: str) -> Optional[str]:
        suggestion = None
        identifier = r'(?:`[^`]+`|\[[^\]]+\]|"[^"]+"|[\w\.]+)'
        select_amb = re.findall(
            rf"^SELECT.*? ({identifier}\.\*).*?FROM", 
            sql, 
            re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        if select_amb:
            suggestion = ""
            for idx, x in enumerate(select_amb, 1):
                suggestion += f"{idx}. We have specified that the ambiguous query is the corresponding id column, please replace {x} with the corresponding id column in the above SQL\n"
        return suggestion
