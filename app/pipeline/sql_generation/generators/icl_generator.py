from .base import BaseSQLGenerator
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import json


class ICLGenerator(BaseSQLGenerator):
    
    _few_shot_examples: Dict[str, List[Dict[str, str]]] = None
    
    def __init__(self, few_shot_examples_path: Optional[str] = None, extractor_max_retry: Optional[int] = None) -> None:
        super().__init__(extractor_max_retry=extractor_max_retry)
        self._few_shot_examples = {}
        if few_shot_examples_path:
            if Path(few_shot_examples_path).exists():
                try:
                    with open(few_shot_examples_path, "r") as f:
                        self._few_shot_examples = json.load(f)
                    logger.info(f"Successfully loaded ICL few-shot examples from {few_shot_examples_path}")
                except Exception as e:
                    logger.warning(f"Failed to load ICL few-shot examples from {few_shot_examples_path}: {e}")
            else:
                logger.warning(f"ICL few-shot examples path does not exist: {few_shot_examples_path}")
        else:
            logger.info("ICL few-shot examples path is not provided. ICL Generator will be disabled.")
    
    def generate(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[List[str], Dict[str, int]]:
        if sampling_budget == 0 or not self._few_shot_examples:
            return [], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Determine question_id or instance_id (Spider2)
        question_id = str(data_item.question_id) if hasattr(data_item, 'question_id') else None
        instance_id = str(data_item.instance_id) if hasattr(data_item, 'instance_id') else None
        
        few_shot_examples = None
        if question_id and question_id in self._few_shot_examples:
            few_shot_examples = self._few_shot_examples[question_id]
        elif instance_id and instance_id in self._few_shot_examples:
            few_shot_examples = self._few_shot_examples[instance_id]
            
        if not few_shot_examples:
            logger.warning(f"Few-shot examples not found for {question_id or instance_id}, returning empty result.")
            return [], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        db_type = getattr(data_item, "db_type", None)
        
        # Define the prompt format function for ICL generator
        def prompt_format_func(schema_profile: str) -> str:
            return PromptFactory.format_icl_sql_generation_prompt(
                few_shot_examples, 
                schema_profile, 
                data_item.question, 
                data_item.evidence, 
                db_type=db_type
            )
            
        final_prompt, level = self._generate_with_progressive_stripping(data_item, llm, prompt_format_func)
        
        if final_prompt is None:
            logger.error(f"CRITICAL: Even minimal ICL prompt for item {data_item.question_id} exceeds token limit. Returning empty result.")
            return [], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            
        extractor = self._get_extractor()
        all_sql_candidates, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": final_prompt}],
            rule_parser=self._parse_llm_response,
            fix_end_token=llm.llm_config.fix_end_token,
            end_token="</result>",
            n=sampling_budget
        )
        
        return all_sql_candidates, total_token_usage
