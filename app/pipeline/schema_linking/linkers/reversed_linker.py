from .base import BaseSchemaLinker
from ..utils import merge_schema_linking_results
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.config import config
from app.db_utils import get_database_schema_profile, map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from typing import Dict, List, Optional, Any
import re
import json


class ReversedLinker(BaseSchemaLinker):
    
    _few_shot_examples: Dict[str, List[Dict[str, str]]] = None
    
    def __init__(self) -> None:
        super().__init__()
        few_shot_examples_path = config.sql_generation_config.icl_few_shot_examples_path
        with open(few_shot_examples_path, "r") as f:
            self._few_shot_examples = json.load(f)
    
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        if sampling_budget == 0:
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        database_schema_profile = get_database_schema_profile(data_item.database_schema_after_value_retrieval)
        few_shot_examples = self._few_shot_examples[str(data_item.question_id)]
        # prompt = PromptFactory.format_dc_sql_generation_prompt(database_schema_profile, data_item.question, data_item.evidence).strip()
        prompt = PromptFactory.format_icl_sql_generation_prompt(few_shot_examples, database_schema_profile, data_item.question, data_item.evidence).strip()
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        all_selections = []
        while len(all_selections) < sampling_budget:
            responses, token_usage = llm.ask([{"role": "user", "content": prompt}], n=sampling_budget - len(all_selections), stop=["</result>"])
            for response in responses:
                
                response = response.content.strip()
                try:
                    parsed_sql_candidate = self._parse_llm_response(response)
                    if parsed_sql_candidate:
                        all_selections.append(self._extract_tables_and_columns(parsed_sql_candidate, data_item.database_schema_after_value_retrieval))
                except Exception as e:
                    logger.error(f"Error parsing LLM response: {e}")
                    logger.debug(f"Response content: {response}")
                    continue
            total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
            total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
            total_token_usage["total_tokens"] += token_usage["total_tokens"]
        return merge_schema_linking_results(all_selections), total_token_usage
    
    def _parse_llm_response(self, response: str) -> Optional[Dict[str, List[str]]]:
        # restore the stop token: </result>
        response += "</result>"
        
        # fix some common format errors
        if "</reasoning>\n[result>" in response:
            response = response.replace("</reasoning>\n[result>", "</reasoning>\n<result>")
        
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
            return answer_content
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return None
    
    def _extract_tables_and_columns(self, sql_candidate: str, database_schema: Dict[str, Any]) -> Dict[str, List[str]]:
        all_table_names = [table_name for table_name in database_schema["tables"].keys()]
        all_column_names = [column_name for table_name in all_table_names for column_name in database_schema["tables"][table_name]["columns"].keys()]
        all_table_names = list(set(all_table_names))
        all_column_names = list(set(all_column_names))
        table_names = list(set([table_name.lower() for table_name in all_table_names if table_name.lower() in sql_candidate.lower()]))
        column_names = list(set([column_name.lower() for column_name in all_column_names if column_name.lower() in sql_candidate.lower()]))
        used_tables_and_columns = {}
        for table_name in table_names:
            table_name = map_lower_table_name_to_original_table_name(table_name, database_schema)
            if table_name is None:
                continue
            used_tables_and_columns[table_name] = []
            for column_name in column_names:
                column_name = map_lower_column_name_to_original_column_name(table_name, column_name, database_schema)
                if column_name is None:
                    continue
                used_tables_and_columns[table_name].append(column_name)
        return used_tables_and_columns