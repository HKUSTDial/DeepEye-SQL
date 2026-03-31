from .base import BaseSQLGenerator
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from typing import Dict, List, Tuple


class SkeletonGenerator(BaseSQLGenerator):
    
    def generate(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[List[str], Dict[str, int]]:
        if sampling_budget == 0:
            return [], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        db_type = getattr(data_item, "db_type", None)
        
        # Define the prompt format function for Skeleton generator
        def prompt_format_func(schema_profile: str) -> str:
            return PromptFactory.format_skeleton_sql_generation_prompt(
                schema_profile, 
                data_item.question, 
                data_item.evidence, 
                db_type=db_type
            )
            
        final_prompt, level = self._generate_with_progressive_stripping(data_item, llm, prompt_format_func)
        
        if final_prompt is None:
            logger.error(f"CRITICAL: Even minimal Skeleton prompt for item {data_item.question_id} exceeds token limit. Returning empty result.")
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
