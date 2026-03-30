from abc import ABC, abstractmethod
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.db_utils import get_database_schema_profile
from app.config import config
from typing import Dict, List, Any, Tuple, Optional, Callable
from app.services import get_schema_service
import re
import tiktoken

class BaseSQLGenerator(ABC):

    @abstractmethod
    def generate(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[List[str], Dict[str, int]]:
        pass
        
    def _generate_with_progressive_stripping(
        self,
        data_item: DataItem,
        llm: LLM,
        prompt_format_func: Callable[[str], str],
        sampling_budget: int = 1
    ) -> Tuple[str, int]:
        """
        Helper method to generate prompt with progressive schema stripping.
        Returns (final_prompt, compressed_level).
        """
        stripping_levels = [
            {"include_description": True, "include_value_statistics": True, "include_value_examples": True, "include_nested_columns": True},
            {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": True},
            {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
            {"include_description": False, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
        ]
        
        try:
            encoding = tiktoken.encoding_for_model(llm.llm_config.model)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
            
        max_prompt_len = llm.llm_config.max_model_len - llm.llm_config.max_tokens
        schema_service = get_schema_service()
        
        for level_idx, levels in enumerate(stripping_levels):
            # Use database_schema_after_schema_linking if available, otherwise fallback
            schema_to_use = getattr(data_item, "database_schema_after_schema_linking", data_item.database_schema)
            schema_service.ensure_schema_features(
                schema_to_use,
                include_value_statistics=levels["include_value_statistics"],
                include_value_examples=levels["include_value_examples"],
            )
            
            database_schema_profile = get_database_schema_profile(
                schema_to_use, 
                **levels
            )
            prompt = prompt_format_func(database_schema_profile).strip()
            
            token_count = len(encoding.encode(prompt))
            if token_count <= max_prompt_len:
                if level_idx > 0:
                    logger.warning(f"SQL Generation prompt for item {data_item.question_id} was too large. Compressed using level {level_idx} (tokens: {token_count})")
                return prompt, level_idx
            else:
                logger.info(f"Level {level_idx} SQL Generation prompt for item {data_item.question_id} too large ({token_count} tokens). Trying next level...")
        
        return None, len(stripping_levels)

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
            return None
