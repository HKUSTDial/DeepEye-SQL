from typing import List, Dict


def merge_schema_linking_results(results: List[Dict[str, List[str]]]) -> Dict[str, List[str]]:
    merged_result = {}
    for result in results:
        for table_name, columns in result.items():
            if table_name not in merged_result:
                merged_result[table_name] = set()
            merged_result[table_name].update(columns)
    return {table_name: list(columns) for table_name, columns in merged_result.items()}
