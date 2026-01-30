from .base import BaseSchemaLinker
from ..utils import merge_schema_linking_results
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from app.prompt import PromptFactory
from app.config import config
from app.llm_extractor import LLMExtractor
from app.db_utils import get_database_schema_profile, map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
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

        # Define progressive stripping levels
        stripping_levels = [
            {"include_description": True, "include_value_statistics": True, "include_value_examples": True, "include_nested_columns": True},
            {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": True},
            {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
            {"include_description": False, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
        ]
        
        import tiktoken
        try:
            encoding = tiktoken.encoding_for_model(llm.llm_config.model)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
            
        max_prompt_len = llm.llm_config.max_model_len - llm.llm_config.max_tokens
        
        final_prompt = None
        for level_idx, levels in enumerate(stripping_levels):
            database_schema_profile = get_database_schema_profile(
                data_item.database_schema_after_value_retrieval, 
                **levels
            )
            prompt = PromptFactory.format_icl_sql_generation_prompt(
                few_shot_examples, 
                database_schema_profile, 
                data_item.question, 
                data_item.evidence, 
                db_type=db_type
            ).strip()
            
            token_count = len(encoding.encode(prompt))
            if token_count <= max_prompt_len:
                final_prompt = prompt
                if level_idx > 0:
                    logger.warning(f"Reversed Prompt for item {data_item.question_id} was too large. Compressed using level {level_idx} (tokens: {token_count})")
                break
            else:
                logger.info(f"Level {level_idx} reversed prompt for item {data_item.question_id} too large ({token_count} tokens). Trying next level...")
                
        if final_prompt is None:
            logger.error(f"CRITICAL: Even minimal reversed prompt for item {data_item.question_id} exceeds token limit ({token_count} tokens). Returning empty result.")
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Define a combined parser that parses SQL then extracts tables/columns
        def parse_and_extract(response: str, database_schema: Dict[str, Any] = None) -> Optional[Dict[str, List[str]]]:
            parsed_sql = self._parse_llm_response(response)
            if parsed_sql and parsed_sql.strip():
                return self._extract_tables_and_columns(parsed_sql, database_schema)
            return None
        
        extractor = LLMExtractor()
        all_selections, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": final_prompt}],
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
        db_type = getattr(data_item, "db_type", None)

        # Define progressive stripping levels
        stripping_levels = [
            {"include_description": True, "include_value_statistics": True, "include_value_examples": True, "include_nested_columns": True},
            {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": True},
            {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
            {"include_description": False, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
        ]
        
        import tiktoken
        try:
            encoding = tiktoken.encoding_for_model(llm.llm_config.model)
        except Exception:
            encoding = tiktoken.get_encoding("cl100k_base")
            
        max_prompt_len = llm.llm_config.max_model_len - llm.llm_config.max_tokens
        
        final_prompt = None
        for level_idx, levels in enumerate(stripping_levels):
            database_schema_profile = get_database_schema_profile(
                data_item.database_schema_after_value_retrieval, 
                **levels
            )
            prompt = PromptFactory.format_dc_sql_generation_prompt(
                database_schema_profile, 
                data_item.question, 
                data_item.evidence,
                db_type=db_type
            ).strip()
            
            token_count = len(encoding.encode(prompt))
            if token_count <= max_prompt_len:
                final_prompt = prompt
                if level_idx > 0:
                    logger.warning(f"DC Fallback Prompt for item {data_item.question_id} was too large. Compressed using level {level_idx} (tokens: {token_count})")
                break
            else:
                logger.info(f"Level {level_idx} DC fallback prompt for item {data_item.question_id} too large ({token_count} tokens). Trying next level...")
                
        if final_prompt is None:
            logger.error(f"CRITICAL: Even minimal DC fallback prompt for item {data_item.question_id} exceeds token limit ({token_count} tokens). Returning empty result.")
            return {}, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Define a combined parser that parses SQL then extracts tables/columns
        def parse_and_extract(response: str, database_schema: Dict[str, Any] = None) -> Optional[Dict[str, List[str]]]:
            parsed_sql = self._parse_llm_response(response)
            if parsed_sql and parsed_sql.strip():
                return self._extract_tables_and_columns(parsed_sql, database_schema)
            return None
        
        extractor = LLMExtractor()
        all_selections, total_token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[{"role": "user", "content": final_prompt}],
            rule_parser=parse_and_extract,
            parser_kwargs={"database_schema": data_item.database_schema_after_value_retrieval},
            fix_end_token=config.schema_linking_config.llm.fix_end_token,
            end_token="</result>",
            n=sampling_budget
        )
        
        return merge_schema_linking_results(all_selections), total_token_usage
    
    def _parse_llm_response(self, response: str) -> Optional[str]:
        try:
            answer_match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
            if not answer_match:
                logger.warning("No <result> tag found in LLM response")
                logger.debug(f"Response content: {response}")
                return None
            answer_content = answer_match.group(1).strip()
            # strip ```sql```
            if answer_content.startswith("```sql") and answer_content.endswith("```"):
                answer_content = answer_content[len("```sql"):-len("```")].strip()
            
            if not answer_content or not answer_content.strip():
                logger.warning("Parsed SQL content is empty")
                return None
                
            logger.debug(f"Parsed SQL from LLM: {answer_content}")
            return answer_content
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return None
    
    def _extract_tables_and_columns(self, sql_candidate: str, database_schema: Dict[str, Any]) -> Dict[str, List[str]]:
        import re
        
        if not sql_candidate or not sql_candidate.strip():
            logger.warning(f"Empty SQL candidate received in _extract_tables_and_columns (len={len(sql_candidate) if sql_candidate else 0})")
            return {}

        # 1. Extract potential table names from SQL using regex
        potential_table_names = []
        # Find backticked names
        potential_table_names.extend(re.findall(r'`([^`]+)`', sql_candidate))
        # Find double quoted names
        potential_table_names.extend(re.findall(r'"([^"]+)"', sql_candidate))
        # Find names after FROM/JOIN
        from_join_matches = re.findall(r'(?:FROM|JOIN|UPDATE|INTO)\s+([a-zA-Z0-9._*%-]+)', sql_candidate, re.IGNORECASE)
        potential_table_names.extend(from_join_matches)
        
        # Deduplicate and normalize
        potential_table_names = list(set([name.strip() for name in potential_table_names if name.strip()]))
        logger.debug(f"Potential table names extracted from SQL: {potential_table_names}\nSQL Candidate: {sql_candidate}")

        # 2. Use the robust mapping function for each potential table name
        mapped_tables = []
        for p_name in potential_table_names:
            original_table_name = map_lower_table_name_to_original_table_name(p_name, database_schema)
            if original_table_name:
                mapped_tables.append(original_table_name)
        
        mapped_tables = list(set(mapped_tables))
        logger.debug(f"Successfully mapped tables: {mapped_tables}")

        # 3. Handle Columns
        all_table_names_in_schema = list(database_schema["tables"].keys())
        all_column_names_in_schema = []
        for t_name in all_table_names_in_schema:
            all_column_names_in_schema.extend(database_schema["tables"][t_name]["columns"].keys())
        all_column_names_in_schema = list(set(all_column_names_in_schema))
        
        found_column_names = list(set([col.lower() for col in all_column_names_in_schema if col.lower() in sql_candidate.lower()]))
        logger.debug(f"Candidate columns found in SQL: {found_column_names}")
        
        used_tables_and_columns = {}
        for original_table_name in mapped_tables:
            used_tables_and_columns[original_table_name] = []
            for col_name in found_column_names:
                original_column_name = map_lower_column_name_to_original_column_name(original_table_name, col_name, database_schema)
                if original_column_name:
                    used_tables_and_columns[original_table_name].append(original_column_name)
            
            if not used_tables_and_columns[original_table_name]:
                logger.debug(f"No valid columns mapped for table: {original_table_name}")
        
        if not used_tables_and_columns:
            logger.warning("No tables/columns could be extracted from the generated SQL")
        
        # Expand tables with identical schema (for Spider2 cloud databases)
        used_tables_and_columns = self._expand_identical_schema_tables(used_tables_and_columns, database_schema)
        
        return used_tables_and_columns
