from .base import BaseSchemaLinker
from ..utils import merge_schema_linking_results
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.db_utils import get_database_schema_profile, map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from typing import Dict, List, Optional, Any
import re
import json


class DirectLinker(BaseSchemaLinker):
    
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        if sampling_budget == 0:
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        database_schema_profile = get_database_schema_profile(data_item.database_schema_after_value_retrieval)
        prompt = PromptFactory.format_direct_linking_prompt(database_schema_profile, data_item.question, data_item.evidence).strip()
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        all_selections = []
        while len(all_selections) < sampling_budget:
            responses, token_usage = llm.ask([{"role": "user", "content": prompt}], n=sampling_budget - len(all_selections), stop=["</result>"])
            for response in responses:
                response = response.content.strip()
                try:
                    parsed_selection = self._parse_llm_response(response, data_item.database_schema_after_value_retrieval)
                    if parsed_selection:
                        all_selections.append(parsed_selection)
                except Exception as e:
                    logger.warning(f"Error parsing LLM response: {e}")
                    logger.warning(f"Response content: {response}")
                    continue
            total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
            total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
            total_token_usage["total_tokens"] += token_usage["total_tokens"]
        
        return merge_schema_linking_results(all_selections), total_token_usage
    
    def _parse_llm_response(self, response: str, database_schema: Dict[str, Any]) -> Optional[Dict[str, List[str]]]:
        # restore the stop token: </result>
        response += "</result>"
        
        try:
            # 提取<result>标签内的内容
            answer_match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
            if not answer_match:
                logger.warning("No <result> tag found in LLM response")
                logger.warning(f"Response content: {response}")
                return None
            
            answer_content = answer_match.group(1).strip()
            
            result = {}
            
            table_matches = re.findall(r'<table\s+table_name="([^"]+)"[^>]*>(.*?)</table>', answer_content, re.DOTALL)
            
            for table_name, table_content in table_matches:
                original_table_name = map_lower_table_name_to_original_table_name(table_name, database_schema)
                if original_table_name is None:
                    continue
                
                result[original_table_name] = []
                
                column_matches = re.findall(r'<column\s+column_name="([^"]+)"[^>]*/?>', table_content)
                
                for column_name in column_matches:
                    original_column_name = map_lower_column_name_to_original_column_name(original_table_name, column_name, database_schema)
                    if original_column_name is None:
                        continue
                    result[original_table_name].append(original_column_name)
            
            if result:
                # logger.info(f"Successfully parsed selection: {len(result)} tables selected")
                return result
            else:
                logger.warning("No valid table-column selections found")
                return None
                
        except Exception as e:
            logger.warning(f"Error parsing LLM response: {e}")
            logger.warning(f"Response content: {response}")
            return None

            