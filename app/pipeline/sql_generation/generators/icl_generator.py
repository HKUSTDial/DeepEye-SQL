from app.config import config
from .base import BaseSQLGenerator
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.llm_extractor import LLMExtractor
from app.db_utils import get_database_schema_profile
from typing import Dict, List, Any, Optional, Tuple
import re
import json


class ICLGenerator(BaseSQLGenerator):
    
    _few_shot_examples: Dict[str, List[Dict[str, str]]] = None
    
    def __init__(self) -> None:
        super().__init__()
        few_shot_examples_path = config.sql_generation_config.icl_few_shot_examples_path
        with open(few_shot_examples_path, "r") as f:
            self._few_shot_examples = json.load(f)
    
    def generate(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[List[str], Dict[str, int]]:
        if sampling_budget == 0:
            return [], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        database_schema_profile = get_database_schema_profile(data_item.database_schema_after_schema_linking)
        few_shot_examples = self._few_shot_examples[str(data_item.question_id)]
        db_type = getattr(data_item, "db_type", None)
        prompt = PromptFactory.format_icl_sql_generation_prompt(few_shot_examples, database_schema_profile, data_item.question, data_item.evidence, db_type=db_type).strip()
        
        extractor = LLMExtractor()
        all_sql_candidates, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": prompt}],
            rule_parser=self._parse_llm_response,
            fix_end_token=config.sql_generation_config.llm.fix_end_token,
            end_token="</result>",
            n=sampling_budget
        )
        
        return all_sql_candidates, total_token_usage
    