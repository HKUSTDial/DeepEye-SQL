from .base import BaseSchemaLinker
from ..utils import merge_schema_linking_results
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.config import config
from app.llm_extractor import LLMExtractor
from app.db_utils import get_database_schema_profile, map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name, get_identical_schema_table_groups
from typing import Dict, List, Optional, Any
from pathlib import Path
import re
import json


class ReversedLinker(BaseSchemaLinker):
    
    _few_shot_examples: Dict[str, List[Dict[str, str]]] = None
    _few_shot_available: bool = False
    
    def __init__(self) -> None:
        super().__init__()
        few_shot_examples_path = config.sql_generation_config.icl_few_shot_examples_path
        if few_shot_examples_path and Path(few_shot_examples_path).exists():
            try:
                with open(few_shot_examples_path, "r") as f:
                    self._few_shot_examples = json.load(f)
                self._few_shot_available = True
                logger.info(f"Loaded few-shot examples from {few_shot_examples_path}")
            except Exception as e:
                logger.warning(f"Failed to load few-shot examples: {e}")
                self._few_shot_examples = {}
                self._few_shot_available = False
        else:
            logger.warning(f"Few-shot examples file not found: {few_shot_examples_path}")
            self._few_shot_examples = {}
            self._few_shot_available = False
    
    def _has_few_shot_examples(self, data_item: DataItem) -> bool:
        """Check if few-shot examples are available for this data item."""
        if not self._few_shot_available or not self._few_shot_examples:
            return False
        
        # Check by question_id
        question_id = str(data_item.question_id) if hasattr(data_item, 'question_id') else None
        if question_id and question_id in self._few_shot_examples:
            examples = self._few_shot_examples[question_id]
            return isinstance(examples, list) and len(examples) > 0
        
        # Check by instance_id (for Spider2)
        instance_id = str(data_item.instance_id) if hasattr(data_item, 'instance_id') else None
        if instance_id and instance_id in self._few_shot_examples:
            examples = self._few_shot_examples[instance_id]
            return isinstance(examples, list) and len(examples) > 0
        
        return False
    
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        if sampling_budget == 0:
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Check if few-shot examples are available; if not, fallback to DC-based approach
        if not self._has_few_shot_examples(data_item):
            logger.info(f"No few-shot examples available for {getattr(data_item, 'instance_id', data_item.question_id)}, falling back to DC-based SQL generation")
            return self._link_with_dc_fallback(data_item, llm, sampling_budget)
        
        database_schema_profile = get_database_schema_profile(data_item.database_schema_after_value_retrieval)
        
        # Get few-shot examples by question_id or instance_id
        question_id = str(data_item.question_id) if hasattr(data_item, 'question_id') else None
        instance_id = str(data_item.instance_id) if hasattr(data_item, 'instance_id') else None
        
        few_shot_examples = None
        if question_id and question_id in self._few_shot_examples:
            few_shot_examples = self._few_shot_examples[question_id]
        elif instance_id and instance_id in self._few_shot_examples:
            few_shot_examples = self._few_shot_examples[instance_id]
        
        if not few_shot_examples:
            logger.warning(f"Few-shot examples not found, falling back to DC-based SQL generation")
            return self._link_with_dc_fallback(data_item, llm, sampling_budget)
        
        db_type = getattr(data_item, "db_type", None)
        prompt = PromptFactory.format_icl_sql_generation_prompt(few_shot_examples, database_schema_profile, data_item.question, data_item.evidence, db_type=db_type).strip()
        
        # Define a combined parser that parses SQL then extracts tables/columns
        def parse_and_extract(response: str, database_schema: Dict[str, Any] = None) -> Optional[Dict[str, List[str]]]:
            parsed_sql = self._parse_llm_response(response)
            if parsed_sql:
                return self._extract_tables_and_columns(parsed_sql, database_schema)
            return None
        
        extractor = LLMExtractor()
        all_selections, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": prompt}],
            rule_parser=parse_and_extract,
            parser_kwargs={"database_schema": data_item.database_schema_after_value_retrieval},
            fix_end_token=config.schema_linking_config.llm.fix_end_token,
            end_token="</result>",
            n=sampling_budget
        )
        
        return merge_schema_linking_results(all_selections), total_token_usage
    
    def _link_with_dc_fallback(self, data_item: DataItem, llm: LLM, sampling_budget: int) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        """
        Fallback to DC-based SQL generation when few-shot examples are not available.
        This is particularly useful for Spider2 datasets that don't have training data.
        """
        database_schema_profile = get_database_schema_profile(data_item.database_schema_after_value_retrieval)
        db_type = getattr(data_item, "db_type", None)
        
        # Use DC SQL generation prompt instead of ICL
        prompt = PromptFactory.format_dc_sql_generation_prompt(
            database_schema_profile, 
            data_item.question, 
            data_item.evidence,
            db_type=db_type
        ).strip()
        
        # Define a combined parser that parses SQL then extracts tables/columns
        def parse_and_extract(response: str, database_schema: Dict[str, Any] = None) -> Optional[Dict[str, List[str]]]:
            parsed_sql = self._parse_llm_response(response)
            if parsed_sql:
                return self._extract_tables_and_columns(parsed_sql, database_schema)
            return None
        
        extractor = LLMExtractor()
        all_selections, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": prompt}],
            rule_parser=parse_and_extract,
            parser_kwargs={"database_schema": data_item.database_schema_after_value_retrieval},
            fix_end_token=config.schema_linking_config.llm.fix_end_token,
            end_token="</result>",
            n=sampling_budget
        )
        
        return merge_schema_linking_results(all_selections), total_token_usage
    
    def _parse_llm_response(self, response: str) -> Optional[Dict[str, List[str]]]:
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
        
        # Expand tables with identical schema (for Spider2 cloud databases)
        used_tables_and_columns = self._expand_identical_schema_tables(used_tables_and_columns, database_schema)
        
        return used_tables_and_columns
    
    def _expand_identical_schema_tables(self, result: Dict[str, List[str]], database_schema: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Expand the selection to include all tables with identical schema.
        """
        table_groups = get_identical_schema_table_groups(database_schema)
        
        if not table_groups:
            return result
        
        expanded_result = dict(result)
        
        for table_name, columns in list(result.items()):
            if table_name in table_groups:
                for group_table in table_groups[table_name]:
                    if group_table not in expanded_result:
                        expanded_result[group_table] = list(columns)
                        logger.debug(f"Auto-expanded identical schema table: {group_table}")
        
        if len(expanded_result) > len(result):
            logger.info(f"Expanded {len(result)} tables to {len(expanded_result)} tables (identical schema groups)")
        
        return expanded_result