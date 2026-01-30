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
                result = self._extract_tables_and_columns(parsed_sql, database_schema)
                if not result:
                    logger.warning(f"SQL parsed successfully but no tables/columns extracted from it. SQL: {parsed_sql}...")
                return result
            
            logger.warning(f"Failed to parse SQL from LLM response (no result tag or empty content)")
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
                result = self._extract_tables_and_columns(parsed_sql, database_schema)
                if not result:
                    logger.warning(f"SQL parsed successfully but no tables/columns extracted from it. SQL: {parsed_sql}...")
                return result
            
            logger.warning(f"Failed to parse SQL from LLM response (no result tag or empty content)")
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
        if not sql_candidate or not sql_candidate.strip():
            logger.warning(f"Empty SQL candidate received in _extract_tables_and_columns")
            return {}

        sql_candidate_lower = sql_candidate.lower()
        
        # Extract all wildcard patterns from SQL (e.g., table_name_*)
        # Handle backticks, double quotes, and unquoted identifiers containing '*'
        wildcard_candidates = []
        # 1. Backticked: `project.dataset.table_*`
        wildcard_candidates.extend(re.findall(r'`([^`]*\*[^`]*)`', sql_candidate_lower))
        # 2. Double quoted: "table_*"
        wildcard_candidates.extend(re.findall(r'"([^"]*\*[^"]*)"', sql_candidate_lower))
        # 3. Unquoted: table_* (looking for alphanumeric/dots/underscores/hyphens followed by *)
        wildcard_candidates.extend(re.findall(r'(?:\s|^)([a-zA-Z0-9_.-]*\*[a-zA-Z0-9_.-]*)', sql_candidate_lower))
        
        wildcard_regexes = []
        for pat in set(wildcard_candidates):
            try:
                # If the pattern contains dots, we also care about the base name wildcard
                # e.g., "schema.table_*" -> regex for "table_.*"
                patterns_to_compile = [pat]
                if "." in pat:
                    patterns_to_compile.append(pat.split(".")[-1])
                
                for p in patterns_to_compile:
                    reg = re.compile(re.escape(p).replace(r'\*', '.*'))
                    wildcard_regexes.append(reg)
            except Exception:
                continue
                
        used_tables_and_columns = {}
        
        # Iterate through all tables in the schema
        for table_key, table_dict in database_schema["tables"].items():
            table_name = table_dict.get("table_name", "")
            table_fullname = table_dict.get("table_fullname", "")
            
            # Collect all possible string variants for this table to check containment
            check_names = [table_key.lower()]
            if table_name: check_names.append(table_name.lower())
            if table_fullname: check_names.append(table_fullname.lower())
            
            # IMPORTANT: Handle dots. If a name is "SCHEMA.TABLE", also check for "TABLE".
            # This handles cases like "SCHEMA"."TABLE" in SQL where "SCHEMA.TABLE" wouldn't match.
            base_names = []
            for name in check_names:
                if "." in name:
                    base_names.append(name.split(".")[-1])
            check_names.extend(base_names)
            check_names = set([n for n in check_names if n]) # Deduplicate

            # Check for exact string containment OR wildcard pattern match
            is_table_used = False
            # 1. Check if any of our variants exist in the SQL string
            for name_variant in check_names:
                if name_variant in sql_candidate_lower:
                    is_table_used = True
                    break
            
            if not is_table_used:
                # 2. Check wildcard regexes against ALL name variants (including base names)
                for reg in wildcard_regexes:
                    for name_variant in check_names:
                        if reg.fullmatch(name_variant):
                            is_table_used = True
                            break
                    if is_table_used: break
                
            if is_table_used:
                matched_columns = []
                # If table is used, check which of its columns are used
                for col_name in table_dict["columns"]:
                    if col_name.lower() in sql_candidate_lower:
                        matched_columns.append(col_name)
                
                # We include the table even if no columns matched (it might be SELECT *)
                used_tables_and_columns[table_key] = matched_columns

        if not used_tables_and_columns:
            logger.warning(f"No tables/columns could be extracted from the generated SQL using string containment.\nSQL: {sql_candidate}")
        
        # Expand tables with identical schema (for Spider2 cloud databases)
        used_tables_and_columns = self._expand_identical_schema_tables(used_tables_and_columns, database_schema)
        
        return used_tables_and_columns
