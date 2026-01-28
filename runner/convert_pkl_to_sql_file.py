import sys
sys.path.append(".")

from app.dataset import load_dataset, BaseDataset
from app.config import config
import json
from app.logger import logger
from app.db_utils import execute_sql_for_data_item, measure_execution_time_for_data_item
from collections import Counter
from typing import List, Tuple, Optional

SAVE_PATH = config.sql_selection_config.save_path

def get_best_sql_via_sc(data_item, candidates: List[str]) -> Optional[str]:
    if not candidates:
        return None
        
    valid_sql_candidates = []
    sql_map_to_result_str = {}
    
    # First pass: try to find candidates with non-empty results
    for sql_candidate in candidates:
        if sql_candidate is None:
            continue
        try:
            execution_result = execute_sql_for_data_item(data_item, sql_candidate)
            if execution_result.result_rows is not None and len(execution_result.result_rows) > 0:
                valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
                sql_map_to_result_str[sql_candidate] = execution_result.result_table_str
        except Exception as e:
            logger.warning(f"Error executing SQL {sql_candidate}: {e}")
            continue
            
    # Second pass: if no successful candidates, fallback to candidates with any valid result (even empty)
    if len(valid_sql_candidates) == 0:
        logger.warning(f"No successful SQL candidates for question {data_item.question_id}, backing to SQL candidates with not none result_rows")
        for sql_candidate in candidates:
            if sql_candidate is None:
                continue
            try:
                execution_result = execute_sql_for_data_item(data_item, sql_candidate)
                if execution_result.result_rows is not None:
                    valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
                    sql_map_to_result_str[sql_candidate] = execution_result.result_table_str
            except Exception:
                continue
                
    if len(valid_sql_candidates) == 0:
        return None
        
    counter = Counter(execution_result for _, execution_result in valid_sql_candidates)
    
    deduplicated_valid_sql_candidates = []
    seen_result_set = set()
    for sql_candidate, execution_result in valid_sql_candidates:
        if execution_result not in seen_result_set:
            try:
                execution_time = measure_execution_time_for_data_item(data_item, sql_candidate)
                deduplicated_valid_sql_candidates.append((sql_candidate, sql_map_to_result_str.get(sql_candidate, ""), counter[execution_result] / len(valid_sql_candidates), execution_time))
                seen_result_set.add(execution_result)
            except Exception:
                continue
                
    if not deduplicated_valid_sql_candidates:
        return None

    # Sort by consistency score (descending) and execution time (ascending - represented as negative for reverse sort)
    # x[2] is consistency score, x[3] is execution time
    top_k_sql_candidates = sorted(deduplicated_valid_sql_candidates, key=lambda x: (x[2], -x[3]), reverse=True)
    
    if top_k_sql_candidates:
        return top_k_sql_candidates[0][0]
    return None


def convert_to_sql_file():
    dataset = load_dataset(SAVE_PATH)
    data = {}
    for item in dataset:
        final_sql = None
        
        # 1. Use existing final_selected_sql if available
        if item.final_selected_sql is not None:
            final_sql = item.final_selected_sql
        
        # 2. Fallback: Try SC on original candidates
        else:
            logger.warning(f"Item {item.question_id} has None final_selected_sql, attempting to select from candidates via SC")
            if item.sql_candidates:
                # Filter out None candidates if any
                candidates = [s for s in item.sql_candidates if s is not None]
                final_sql = get_best_sql_via_sc(item, candidates)
                
                # 3. Fallback: Use first candidate
                if final_sql is None and candidates:
                    logger.warning(f"Item {item.question_id}: SC failed, using first candidate")
                    final_sql = candidates[0]
            
        # 4. Final Fallback: Error
        if final_sql is None:
            logger.warning(f"Item {item.question_id}: No valid SQL found, using 'Error'")
            final_sql = "Error"
            
        data[str(item.question_id)] = final_sql.strip()

    with open(SAVE_PATH.replace(".pkl", ".json"), "w") as f:
        json.dump(data, f, indent=4)
    logger.info(f"Dataset converted to sql file successfully")
    logger.info(f"Dataset sql file saved to {SAVE_PATH.replace('.pkl', '.json')}")
    

convert_to_sql_file()
