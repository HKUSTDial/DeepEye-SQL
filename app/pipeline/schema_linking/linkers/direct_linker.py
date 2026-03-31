from .base import BaseSchemaLinker
from ..utils import merge_schema_linking_results
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from app.services import get_schema_service
from typing import Dict, List, Optional, Any
import re


class DirectLinker(BaseSchemaLinker):
    
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        if sampling_budget == 0:
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        db_type = getattr(data_item, "db_type", None)
        
        max_prompt_len = llm.llm_config.max_model_len - llm.llm_config.max_tokens
        schema_service = get_schema_service()
        final_prompt, level_idx = schema_service.build_prompt_with_progressive_schema_stripping(
            data_item.database_schema_after_value_retrieval,
            encoding_model_name=llm.llm_config.model,
            max_prompt_len=max_prompt_len,
            prompt_format_func=lambda database_schema_profile: PromptFactory.format_direct_linking_prompt(
                database_schema_profile,
                data_item.question,
                data_item.evidence,
                db_type=db_type,
            ),
            item_id=data_item.question_id,
            log_prefix="Schema Linking",
        )
        if final_prompt is not None and level_idx > 0:
            logger.warning(f"Prompt for item {data_item.question_id} was too large. Compressed using level {level_idx}")
        if final_prompt is None:
            logger.error(f"CRITICAL: Even minimal prompt for item {data_item.question_id} exceeds token limit. Returning empty result.")
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        extractor = self._get_extractor()
        all_selections, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": final_prompt}],
            rule_parser=self._parse_llm_response,
            parser_kwargs={"database_schema": data_item.database_schema_after_value_retrieval},
            fix_end_token=llm.llm_config.fix_end_token,
            end_token="</result>",
            n=sampling_budget
        )
        
        return merge_schema_linking_results(all_selections), total_token_usage
    
    def _parse_llm_response(self, response: str, database_schema: Dict[str, Any]) -> Optional[Dict[str, List[str]]]:
        try:
            # 提取<result>标签内的内容
            answer_match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
            if not answer_match:
                logger.warning("No <result> tag found in LLM response")
                logger.warning(f"Response content: {response}")
                return None
            
            answer_content = answer_match.group(1).strip()
            
            result = {}
            
            # More robust regex for table tags: handles spaces and single/double quotes
            table_matches = re.findall(r'<table\s+table_name\s*=\s*["\']([^"\']+)["\'][^>]*>(.*?)</table>', answer_content, re.DOTALL)
            
            if not table_matches:
                logger.warning(f"No <table> tags found in <result> content: {answer_content[:200]}...")
            
            for table_name, table_content in table_matches:
                original_table_name = map_lower_table_name_to_original_table_name(table_name, database_schema)
                if original_table_name is None:
                    logger.warning(f"Could not map table name: {table_name}")
                    continue
                
                result[original_table_name] = []
                
                # More robust regex for column tags
                column_matches = re.findall(r'<column\s+column_name\s*=\s*["\']([^"\']+)["\']\s*/?>', table_content)
                
                for column_name in column_matches:
                    original_column_name = map_lower_column_name_to_original_column_name(original_table_name, column_name, database_schema)
                    if original_column_name is None:
                        logger.warning(f"Could not map column name: {column_name} in table {original_table_name}")
                        continue
                    result[original_table_name].append(original_column_name)
            
            if result:
                # Expand tables with identical schema (for Spider2 cloud databases)
                result = self._expand_identical_schema_tables(result, database_schema)
                return result
            else:
                logger.warning("No valid table-column selections found")
                return None
                
        except Exception as e:
            logger.warning(f"Error parsing LLM response: {e}")
            logger.warning(f"Response content: {response}")
            return None
