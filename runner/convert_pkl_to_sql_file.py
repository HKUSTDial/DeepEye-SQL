import sys
import os
import argparse
import json
from typing import List, Optional
from collections import Counter

sys.path.append(".")

from app.dataset import load_dataset
from app.logger import logger
from app.db_utils import execute_sql_for_data_item, measure_execution_time_for_data_item


def convert_to_sql_file(save_path: str):
    dataset = load_dataset(save_path)
    data = {}
    for item in dataset:
        final_sql = None
        
        # 1. Use existing final_selected_sql if available
        if item.final_selected_sql is not None:
            final_sql = item.final_selected_sql
        
        # 2. Final Fallback: Error
        if final_sql is None:
            logger.warning(f"Item {item.question_id}: No valid SQL found, using 'Error'")
            final_sql = "Error"
            
        data[str(item.question_id)] = final_sql.strip()

    output_json_path = save_path.replace(".pkl", ".json")
    with open(output_json_path, "w") as f:
        json.dump(data, f, indent=4)
    logger.info(f"Dataset converted to sql file successfully")
    logger.info(f"Dataset sql file saved to {output_json_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pkl_path", type=str, default=None)
    args = parser.parse_args()

    from app.config import config
    
    save_path = args.pkl_path if args.pkl_path else config.sql_selection_config.save_path
    convert_to_sql_file(save_path)
