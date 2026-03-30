"""
Pipeline validation module.

Provides validation functions to check if all required fields are properly filled
after each pipeline step.
"""

from typing import Dict, Any
from app.dataset import BaseDataset
from app.dataset.artifacts import STAGE_VALIDATION_FIELDS
from app.logger import logger


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
    if step_name not in STAGE_VALIDATION_FIELDS:
        raise ValueError(f"Unknown step: {step_name}. Valid steps: {list(STAGE_VALIDATION_FIELDS.keys())}")
    
    total_items = len(dataset)
    valid_items = 0
    issues = []
    
    for data_item in dataset:
        item_issues = data_item.get_stage_validation_errors(step_name)
        if not item_issues:
            valid_items += 1
            continue

        for issue in item_issues:
            issues.append({
                "question_id": data_item.get_item_id(),
                "field": issue["field"],
                "error": issue["error"],
            })
    
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
