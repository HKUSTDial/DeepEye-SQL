"""
Convert Spider2 dataset pkl results to SQL files for official evaluation.
Output format: One SQL file per instance_id (e.g., bq011.sql, sf_bq001.sql)
"""

import sys
sys.path.append(".")

import os
import argparse
from pathlib import Path
from app.dataset import load_dataset, Spider2DataItem
from app.logger import logger
from app.db_utils import execute_sql_for_data_item
from collections import Counter
from typing import List, Optional


def convert_to_spider2_sql_files(
    pkl_path: str,
    output_dir: str
):
    """
    Convert pkl dataset to individual SQL files for Spider2 evaluation.
    
    Args:
        pkl_path: Path to the pkl file with results.
        output_dir: Directory to save SQL files.
    """
    dataset = load_dataset(pkl_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    error_count = 0
    
    for item in dataset:
        # Get instance_id (Spider2 specific)
        instance_id = getattr(item, "instance_id", None)
        if instance_id is None:
            # Fallback to question_id for non-Spider2 datasets
            instance_id = str(item.question_id)
        
        final_sql = None
        
        # 1. Use existing final_selected_sql if available
        if item.final_selected_sql is not None:
            final_sql = item.final_selected_sql
        
        # 2. Final Fallback: Error placeholder
        if final_sql is None:
            logger.warning(f"Item {instance_id}: No valid SQL found, using 'SELECT 1'")
            final_sql = "SELECT 1"  # Placeholder for missing SQL
            error_count += 1
        else:
            success_count += 1
        
        # Write SQL file
        sql_file_path = output_path / f"{instance_id}.sql"
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(final_sql.strip())
    
    logger.info(f"Conversion completed:")
    logger.info(f"  - Success: {success_count}")
    logger.info(f"  - Errors: {error_count}")
    logger.info(f"  - Output directory: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Convert Spider2 pkl results to SQL files")
    parser.add_argument(
        "--pkl_path",
        type=str,
        default=None,
        help="Path to the pkl file (default: use config)"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=None,
        help="Output directory for SQL files (default: same as pkl with _sql suffix)"
    )
    args = parser.parse_args()
    
    from app.config import config
    
    # Determine pkl path
    pkl_path = args.pkl_path
    if pkl_path is None:
        pkl_path = config.sql_selection_config.save_path
    
    # Determine output directory
    output_dir = args.output_dir
    if output_dir is None:
        output_dir = str(Path(pkl_path).parent / "sql_output")
    
    logger.info(f"Converting {pkl_path} to SQL files in {output_dir}")
    
    convert_to_spider2_sql_files(
        pkl_path=pkl_path,
        output_dir=output_dir
    )


if __name__ == "__main__":
    main()
