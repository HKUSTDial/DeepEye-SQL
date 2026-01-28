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
from app.config import config
from app.logger import logger
from app.db_utils import execute_sql_for_data_item
from collections import Counter
from typing import List, Optional


def get_best_sql_via_sc(data_item, candidates: List[str]) -> Optional[str]:
    """
    Select the best SQL candidate via self-consistency (SC).
    
    For SQLite (BIRD/Spider): Uses execution result grouping based on set equality.
    For Cloud DBs (Spider2): Returns first successfully executed SQL (no grouping,
    since Spider2 evaluation uses more complex comparison logic).
    """
    if not candidates:
        return None
    
    # Check if it's a Spider2 cloud database
    db_type = getattr(data_item, "db_type", None)
    is_cloud_db = db_type is not None and db_type in ("bigquery", "snowflake")
    
    if is_cloud_db:
        return _get_best_sql_for_spider2(data_item, candidates)
    else:
        return _get_best_sql_for_bird(data_item, candidates)


def _get_best_sql_for_bird(data_item, candidates: List[str]) -> Optional[str]:
    """
    Select best SQL for BIRD/Spider datasets via self-consistency.
    Groups by execution result set equality.
    """
    valid_sql_candidates = []
    
    # First pass: try to find candidates with non-empty results
    for sql_candidate in candidates:
        if sql_candidate is None:
            continue
        try:
            execution_result = execute_sql_for_data_item(data_item, sql_candidate)
            if execution_result.result_rows is not None and len(execution_result.result_rows) > 0:
                valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
        except Exception as e:
            logger.warning(f"Error executing SQL: {e}")
            continue
            
    # Second pass: if no successful candidates, fallback to candidates with any valid result
    if len(valid_sql_candidates) == 0:
        for sql_candidate in candidates:
            if sql_candidate is None:
                continue
            try:
                execution_result = execute_sql_for_data_item(data_item, sql_candidate)
                if execution_result.result_rows is not None:
                    valid_sql_candidates.append((sql_candidate, frozenset(execution_result.result_rows)))
            except Exception:
                continue
                
    if len(valid_sql_candidates) == 0:
        return None
        
    # Select by consistency (most common result)
    counter = Counter(execution_result for _, execution_result in valid_sql_candidates)
    return max(valid_sql_candidates, key=lambda x: counter[x[1]])[0]


def _get_best_sql_for_spider2(data_item, candidates: List[str]) -> Optional[str]:
    """
    Select best SQL for Spider2 datasets (BigQuery/Snowflake).
    
    Spider2 evaluation uses complex comparison logic (numeric tolerance, NULL handling,
    optional column/row ordering), so we cannot reliably group by result set.
    Instead, return the first successfully executed SQL.
    """
    # First pass: find first candidate with non-empty results
    for sql_candidate in candidates:
        if sql_candidate is None:
            continue
        try:
            execution_result = execute_sql_for_data_item(data_item, sql_candidate)
            if execution_result.result_rows is not None and len(execution_result.result_rows) > 0:
                return sql_candidate
        except Exception as e:
            logger.warning(f"Error executing SQL: {e}")
            continue
    
    # Second pass: find first candidate with any valid result (including empty)
    for sql_candidate in candidates:
        if sql_candidate is None:
            continue
        try:
            execution_result = execute_sql_for_data_item(data_item, sql_candidate)
            if execution_result.result_rows is not None:
                return sql_candidate
        except Exception:
            continue
    
    return None


def convert_to_spider2_sql_files(
    pkl_path: str,
    output_dir: str,
    use_sc_fallback: bool = True
):
    """
    Convert pkl dataset to individual SQL files for Spider2 evaluation.
    
    Args:
        pkl_path: Path to the pkl file with results.
        output_dir: Directory to save SQL files.
        use_sc_fallback: Whether to use self-consistency as fallback.
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
        
        # 2. Fallback: Try SC on revised candidates
        elif use_sc_fallback and item.sql_candidates_after_revision:
            logger.info(f"Item {instance_id}: Using SC on revised candidates")
            candidates = [s for s in item.sql_candidates_after_revision if s is not None]
            final_sql = get_best_sql_via_sc(item, candidates)
        
        # 3. Fallback: Try SC on original candidates
        elif use_sc_fallback and item.sql_candidates:
            logger.info(f"Item {instance_id}: Using SC on original candidates")
            candidates = [s for s in item.sql_candidates if s is not None]
            final_sql = get_best_sql_via_sc(item, candidates)
            
            # 4. Use first candidate
            if final_sql is None and candidates:
                final_sql = candidates[0]
        
        # 5. Final Fallback: Error placeholder
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
    parser.add_argument(
        "--no_sc_fallback",
        action="store_true",
        help="Disable self-consistency fallback"
    )
    args = parser.parse_args()
    
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
        output_dir=output_dir,
        use_sc_fallback=not args.no_sc_fallback
    )


if __name__ == "__main__":
    main()
