from .prompt_template import *
from .spider2_prompt_template import *
from typing import List, Dict, Any, Tuple, Optional


def _is_spider2_db_type(db_type: Optional[str]) -> bool:
    """Check if the database type is a Spider2 database."""
    return db_type is not None and db_type in ("bigquery", "snowflake", "sqlite")


class PromptFactory:
    
    @staticmethod
    def format_keywords_extraction_prompt(question: str, hint: str) -> str:
        return KEYWORDS_EXTRACTION_PROMPT.format(QUESTION=question, HINT=hint)
    
    @staticmethod
    def format_direct_linking_prompt(database_schema: str, question: str, hint: str, db_type: Optional[str] = None) -> str:
        if _is_spider2_db_type(db_type):
            return SPIDER2_DIRECT_LINKING_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, DATABASE_ENGINE=db_type.upper())
        return DIRECT_LINKING_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint)
    
    @staticmethod
    def format_skeleton_sql_generation_prompt(database_schema: str, question: str, hint: str, db_type: Optional[str] = None) -> str:
        if _is_spider2_db_type(db_type):
            return SPIDER2_SKELETON_SQL_GENERATION_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, DATABASE_ENGINE=db_type.upper())
        return SKELETON_SQL_GENERATION_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint)
    
    @staticmethod
    def format_dc_sql_generation_prompt(database_schema: str, question: str, hint: str, db_type: Optional[str] = None) -> str:
        if _is_spider2_db_type(db_type):
            return SPIDER2_DC_SQL_GENERATION_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, DATABASE_ENGINE=db_type.upper())
        return DC_SQL_GENERATION_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint)
    
    @staticmethod
    def format_icl_sql_generation_prompt(few_shot_examples: List[Dict[str, Any]], database_schema: str, question: str, hint: str, db_type: Optional[str] = None) -> str:
        few_shot_examples_str = "\n".join(
            [f"- Example {i+1}:\nQuestion: {example['question']}\nSQL: {example['sql']}" for i, example in enumerate(few_shot_examples)]
        )
        if _is_spider2_db_type(db_type):
            return SPIDER2_ICL_SQL_GENERATION_PROMPT.format(FEW_SHOT_EXAMPLES=few_shot_examples_str, DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, DATABASE_ENGINE=db_type.upper())
        return ICL_SQL_GENERATION_PROMPT.format(FEW_SHOT_EXAMPLES=few_shot_examples_str, DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint)

    @staticmethod
    def format_execution_checker_prompt(database_schema: str, question: str, hint: str, sql: str, execution_result: str, db_type: Optional[str] = None) -> str:
        if _is_spider2_db_type(db_type):
            return SPIDER2_EXECUTION_CHECKER_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, QUERY=sql, RESULT=execution_result, DATABASE_ENGINE=db_type.upper())
        return EXECUTION_CHECKER_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, QUERY=sql, RESULT=execution_result)
    
    @staticmethod
    def format_common_checker_prompt(database_schema: str, question: str, hint: str, sql: str, suggestions: str, db_type: Optional[str] = None) -> str:
        if _is_spider2_db_type(db_type):
            return SPIDER2_COMMON_CHECKER_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, QUERY=sql, SUGGESTIONS=suggestions, DATABASE_ENGINE=db_type.upper())
        return COMMON_CHECKER_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, QUERY=sql, SUGGESTIONS=suggestions)
    
    @staticmethod
    def format_br_pair_selection_prompt(database_schema: str, question: str, hint: str, query_a: str, result_a: str, query_b: str, result_b: str, db_type: Optional[str] = None) -> str:
        if _is_spider2_db_type(db_type):
            return SPIDER2_BR_PAIR_SELECTION_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, QUERY_A=query_a, RESULT_A=result_a, QUERY_B=query_b, RESULT_B=result_b, DATABASE_ENGINE=db_type.upper())
        return BR_PAIR_SELECTION_PROMPT.format(DATABASE_SCHEMA=database_schema, QUESTION=question, HINT=hint, QUERY_A=query_a, RESULT_A=result_a, QUERY_B=query_b, RESULT_B=result_b)