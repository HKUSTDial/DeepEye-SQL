"""
Pipeline validation module.

Provides validation functions to check if all required fields are properly filled
after each pipeline step.
"""

from typing import List, Dict, Any, Optional, Tuple
from app.dataset import BaseDataset, DataItem
from app.logger import logger


# Define required fields for each pipeline step
STEP_REQUIRED_FIELDS = {
    "value_retrieval": [
        "question_keywords",
        "retrieved_values", 
        "database_schema_after_value_retrieval",
        "value_retrieval_time",
        "value_retrieval_llm_cost",
    ],
    "schema_linking": [
        "direct_linked_tables_and_columns",
        "reversed_linked_tables_and_columns",
        "value_linked_tables_and_columns",
        "final_linked_tables_and_columns",
        "database_schema_after_schema_linking",
        "schema_linking_time",
        "schema_linking_llm_cost",
    ],
    "sql_generation": [
        "sql_candidates",
        "sql_generation_time",
        "sql_generation_llm_cost",
    ],
    "sql_revision": [
        "sql_candidates_after_revision",
        "sql_revision_time",
        "sql_revision_llm_cost",
    ],
    "sql_selection": [
        "final_selected_sql",
        "sql_selection_time",
        "sql_selection_llm_cost",
    ],
}


def validate_field(data_item: DataItem, field: str) -> Tuple[bool, str]:
    """
    Validate a single field for a data item.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not hasattr(data_item, field):
        return False, f"Field '{field}' does not exist"
    
    value = getattr(data_item, field)
    
    if value is None:
        return False, f"Field '{field}' is None"
    
    return True, ""


def validate_step(dataset: BaseDataset, step_name: str) -> Dict[str, Any]:
    """
    Validate all data items after a pipeline step.
    
    Args:
        dataset: The dataset to validate
        step_name: The pipeline step name (e.g., "value_retrieval", "schema_linking")
    
    Returns:
        Dictionary with validation results:
        {
            "total_items": int,
            "valid_items": int,
            "invalid_items": int,
            "issues": [
                {"question_id": int, "field": str, "error": str},
                ...
            ]
        }
    """
    if step_name not in STEP_REQUIRED_FIELDS:
        raise ValueError(f"Unknown step: {step_name}. Valid steps: {list(STEP_REQUIRED_FIELDS.keys())}")
    
    required_fields = STEP_REQUIRED_FIELDS[step_name]
    
    total_items = len(dataset)
    valid_items = 0
    issues = []
    
    for data_item in dataset:
        item_valid = True
        for field in required_fields:
            is_valid, error = validate_field(data_item, field)
            if not is_valid:
                item_valid = False
                issues.append({
                    "question_id": data_item.question_id,
                    "field": field,
                    "error": error,
                })
        
        if item_valid:
            valid_items += 1
    
    return {
        "total_items": total_items,
        "valid_items": valid_items,
        "invalid_items": total_items - valid_items,
        "issues": issues,
    }


def log_validation_results(step_name: str, results: Dict[str, Any]) -> bool:
    """
    Log validation results and return whether validation passed.
    
    Args:
        step_name: The pipeline step name
        results: The validation results dictionary
    
    Returns:
        True if all items are valid, False otherwise
    """
    total = results["total_items"]
    valid = results["valid_items"]
    invalid = results["invalid_items"]
    
    if invalid == 0:
        logger.info(f"[{step_name}] Validation PASSED: {valid}/{total} items valid")
        return True
    else:
        logger.error(f"[{step_name}] Validation FAILED: {invalid}/{total} items have issues")
        
        # Group issues by field
        field_counts: Dict[str, int] = {}
        for issue in results["issues"]:
            field = issue["field"]
            field_counts[field] = field_counts.get(field, 0) + 1
        
        # Log summary by field
        for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
            logger.error(f"  - {field}: {count} items affected")
        
        # Log first few specific issues
        logger.error(f"  First 5 issues:")
        for issue in results["issues"][:5]:
            logger.error(f"    - question_id={issue['question_id']}: {issue['error']}")
        
        return False


def validate_pipeline_step(dataset: BaseDataset, step_name: str) -> bool:
    """
    Convenience function to validate and log results for a pipeline step.
    
    Args:
        dataset: The dataset to validate
        step_name: The pipeline step name
    
    Returns:
        True if validation passed, False otherwise
    """
    results = validate_step(dataset, step_name)
    return log_validation_results(step_name, results)
