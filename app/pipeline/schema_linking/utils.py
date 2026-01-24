from typing import List, Dict, Optional


def merge_schema_linking_results(results: List[Optional[Dict[str, List[str]]]]) -> Dict[str, List[str]]:
    """
    Merge multiple schema linking results into one.
    
    Args:
        results: List of schema linking results. None values are skipped.
    
    Returns:
        Merged result dictionary
    """
    merged_result = {}
    for result in results:
        if result is None:
            continue
        for table_name, columns in result.items():
            if table_name not in merged_result:
                merged_result[table_name] = set()
            merged_result[table_name].update(columns)
    return {table_name: list(columns) for table_name, columns in merged_result.items()}
