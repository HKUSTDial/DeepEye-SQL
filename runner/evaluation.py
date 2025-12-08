import sys
sys.path.append(".")
from app.config import config
from app.db_utils import execute_sql, SQLExecutionResult
from app.dataset import load_dataset
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional, List, Tuple
from tqdm import tqdm
from collections import Counter
import numpy as np


def _eval_ex_after_selection(pred_sql: str, gold_sql: str, db_path: str) -> Optional[int]:
    pred_result = execute_sql(db_path, pred_sql)
    gold_result = execute_sql(db_path, gold_sql)
    if gold_result.result_rows is None:
        print(gold_result)
        return None
    if pred_result.result_rows is None:
        return 0
    return 1 if set(pred_result.result_rows) == set(gold_result.result_rows) else 0


def run_evaluation():
    dataset = load_dataset(config.sql_selection_config.save_path)
    executor = ProcessPoolExecutor(max_workers=1)
    all_futures = [ executor.submit(_eval_ex_after_selection, data_item.final_selected_sql, data_item.gold_sql, data_item.database_path) for data_item in dataset ]
    
    selected_results = []
    for future in tqdm(as_completed(all_futures), total=len(all_futures), desc="Evaluating SQL"):
        selected_result = future.result()
        if selected_result is not None:
            selected_results.append(selected_result)
        else:
            print(f"Gold SQL execution failed")
        print(f"[Tracking] Evaluated SQL selection {np.mean(selected_results) * 100:.2f}%")
        
    return np.mean(selected_results)

if __name__ == "__main__":
    selected_result = run_evaluation()
    print(f"Overall Execution Accuracy: {selected_result * 100:.2f}%")