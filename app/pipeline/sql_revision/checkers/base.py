from abc import ABC, abstractmethod
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from typing import Dict, Tuple, Optional, Callable
from app.services import get_schema_service
import re


class BaseChecker(ABC):
    
    @abstractmethod
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        pass
    
    def _check_and_revise_with_progressive_stripping(
        self,
        data_item: DataItem,
        llm: LLM,
        prompt_format_func: Callable[[str], str],
    ) -> Tuple[str, int]:
        """
        Helper method to generate prompt with progressive schema stripping for checkers.
        Returns (final_prompt, compressed_level).
        """
        max_prompt_len = llm.llm_config.max_model_len - llm.llm_config.max_tokens
        schema_service = get_schema_service()
        schema_to_use = getattr(data_item, "database_schema_after_schema_linking", data_item.database_schema)
        prompt, level_idx = schema_service.build_prompt_with_progressive_schema_stripping(
            schema_to_use,
            encoding_model_name=llm.llm_config.model,
            max_prompt_len=max_prompt_len,
            prompt_format_func=prompt_format_func,
            item_id=data_item.question_id,
            log_prefix="Revision Checker",
        )
        if prompt is not None and level_idx > 0:
            logger.warning(f"Revision Checker prompt for item {data_item.question_id} was too large. Compressed using level {level_idx}")
        return prompt, level_idx

    def _parse_llm_response(self, response: str) -> Optional[str]:
        try:
            answer_match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
            if not answer_match:
                logger.warning("No <result> tag found in LLM response")
                logger.warning(f"Response content: {response}")
                return None
            answer_content = answer_match.group(1).strip()
            # strip ```sql```
            if answer_content.startswith("```sql") and answer_content.endswith("```"):
                answer_content = answer_content[len("```sql"):-len("```")].strip()
            
            if not answer_content or not answer_content.strip():
                logger.warning("Parsed SQL content is empty")
                return None
                
            return answer_content
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            logger.debug(f"Response content: {response}")
            return None
