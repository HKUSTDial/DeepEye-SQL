"""
Unified evaluation script for all datasets (Spider, BIRD, Spider2).
Automatically detects dataset type and uses appropriate evaluation method.
"""

import sys
sys.path.append(".")

import os
import argparse
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from tqdm import tqdm
import numpy as np

from app.logger import logger
from app.db_utils import execute_sql


def _eval_ex_after_selection(pred_sql: str, gold_sql: str, db_path: str) -> Optional[int]:
    """
    Evaluate execution accuracy by comparing query results.
    Used for Spider and BIRD datasets.
    """
    pred_result = execute_sql(db_path, pred_sql)
    gold_result = execute_sql(db_path, gold_sql)
    
    if gold_result.result_rows is None:
        logger.warning(f"Gold SQL execution failed for database: {db_path}")
        return None
    
    if pred_result.result_rows is None:
        return 0
    
    return 1 if set(pred_result.result_rows) == set(gold_result.result_rows) else 0


def evaluate_spider_bird(pkl_path: str = None, max_workers: int = 32) -> float:
    """
    Evaluate Spider or BIRD dataset using direct SQL execution comparison.
    
    Args:
        pkl_path: Path to the pkl file with results.
        max_workers: Number of parallel workers.
        
    Returns:
        Execution accuracy as a float (0.0 to 1.0).
    """
    from app.config import config
    from app.dataset import load_dataset
    
    if pkl_path is None:
        pkl_path = config.sql_selection_config.save_path
    
    logger.info(f"Loading dataset from: {pkl_path}")
    dataset = load_dataset(pkl_path)
    
    logger.info(f"Evaluating {len(dataset)} queries with {max_workers} workers...")
    executor = ProcessPoolExecutor(max_workers=max_workers)
    all_futures = [
        executor.submit(
            _eval_ex_after_selection,
            data_item.final_selected_sql,
            data_item.gold_sql,
            data_item.database_path
        )
        for data_item in dataset
    ]
    
    selected_results = []
    for future in tqdm(as_completed(all_futures), total=len(all_futures), desc="Evaluating SQL"):
        selected_result = future.result()
        if selected_result is not None:
            selected_results.append(selected_result)
        else:
            logger.warning("Gold SQL execution failed for one query")
        
        # Show progress
        if len(selected_results) > 0 and len(selected_results) % 10 == 0:
            current_acc = np.mean(selected_results) * 100
            logger.info(f"[Progress] {len(selected_results)}/{len(dataset)} queries - Current EX: {current_acc:.2f}%")
    
    executor.shutdown()
    
    if len(selected_results) == 0:
        logger.error("No valid results to evaluate!")
        return 0.0
    
    return np.mean(selected_results)


def evaluate_spider2(
    pkl_path: str = None,
    dataset_split: str = "lite",
    sql_output_dir: str = None,
    max_workers: int = 8,
    timeout: int = None,
    skip_conversion: bool = False
) -> Optional[int]:
    """
    Evaluate Spider2 dataset using official evaluation script.
    
    Args:
        pkl_path: Path to the pkl file with results.
        dataset_split: "lite" or "snow".
        sql_output_dir: Directory for SQL output files.
        max_workers: Number of parallel workers for evaluation.
        timeout: SQL execution timeout in seconds.
        skip_conversion: Skip pkl to SQL conversion (use existing SQL files).
        
    Returns:
        Return code from evaluation script (0 = success, None = error).
    """
    from app.config import config
    
    # Determine paths
    if timeout is None:
        timeout = config.dataset_config.sql_execution_timeout
    if pkl_path is None:
        pkl_path = config.sql_selection_config.save_path
    
    pkl_path = Path(pkl_path)
    
    if sql_output_dir is None:
        sql_output_dir = pkl_path.parent / "sql_output"
    else:
        sql_output_dir = Path(sql_output_dir)
    
    # Ensure absolute path for sql_output_dir as we'll change CWD
    sql_output_dir = sql_output_dir.resolve()
    
    # Step 1: Convert pkl to SQL files (if not skipping)
    if not skip_conversion:
        logger.info(f"Step 1: Converting {pkl_path} to SQL files...")
        from runner.convert_pkl_to_sql import convert_to_sql_files
        convert_to_sql_files(
            pkl_path=str(pkl_path),
            output_dir=str(sql_output_dir)
        )
    else:
        logger.info("Step 1: Skipping pkl conversion (using existing SQL files)")
    
    # Step 2: Run official evaluation script
    logger.info(f"Step 2: Running official Spider2-{dataset_split} evaluation...")
    
    # Determine paths based on dataset split
    project_root = Path(__file__).resolve().parent.parent
    
    # Support both old path format and new unified format
    if dataset_split == "lite":
        eval_script = project_root / "data" / "spider2-lite" / "evaluation_suite" / "evaluate.py"
        gold_dir = project_root / "data" / "spider2-lite" / "evaluation_suite" / "gold"
    elif dataset_split == "snow":
        eval_script = project_root / "data" / "spider2-snow" / "evaluation_suite" / "evaluate.py"
        gold_dir = project_root / "data" / "spider2-snow" / "evaluation_suite" / "gold"
    else:
        raise ValueError(f"Unknown dataset split: {dataset_split}. Expected 'lite' or 'snow'")
    
    if not eval_script.exists():
        logger.error(f"Evaluation script not found: {eval_script}")
        logger.error("Please ensure the Spider2 evaluation suite is properly set up.")
        return None
    
    if not gold_dir.exists():
        logger.error(f"Gold directory not found: {gold_dir}")
        logger.error("Please ensure the Spider2 gold data is properly set up.")
        return None
    
    # Build command
    cmd = [
        sys.executable,
        str(eval_script),
        "--mode", "sql",
        "--result_dir", str(sql_output_dir),
        "--gold_dir", str(gold_dir),
        "--max_workers", str(max_workers),
        "--timeout", str(timeout),
    ]
    
    logger.info(f"Running command: {' '.join(cmd)}")
    
    # Change to the evaluation suite directory (required for relative paths in eval script)
    eval_cwd = eval_script.parent
    
    try:
        result = subprocess.run(
            cmd,
            cwd=str(eval_cwd),
            capture_output=False,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Evaluation failed with return code: {result.returncode}")
        else:
            logger.info("Evaluation completed successfully!")
            
        return result.returncode
        
    except Exception as e:
        logger.error(f"Error running evaluation: {e}")
        return None


def run_evaluation(
    pkl_path: str = None,
    dataset_type: str = None,
    dataset_split: str = None,
    max_workers: int = None,
    timeout: int = None,
    sql_output_dir: str = None,
    skip_conversion: bool = False
):
    """
    Unified evaluation entry point. Auto-detects dataset type and uses appropriate method.
    
    Args:
        pkl_path: Path to the pkl file with results.
        dataset_type: Override dataset type (auto-detected from config if None).
        dataset_split: Dataset split (required for Spider2).
        max_workers: Number of parallel workers.
        timeout: SQL execution timeout (Spider2 only).
        sql_output_dir: SQL output directory (Spider2 only).
        skip_conversion: Skip SQL conversion (Spider2 only).
    """
    from app.config import config
    
    # Auto-detect dataset type from config if not provided
    if dataset_type is None:
        dataset_type = config.dataset_config.type
        logger.info(f"Auto-detected dataset type: {dataset_type}")
    
    # Auto-detect dataset split for Spider2
    if dataset_split is None and dataset_type == "spider2":
        dataset_split = config.dataset_config.split
        logger.info(f"Auto-detected dataset split: {dataset_split}")
    
    # Set default max_workers
    if max_workers is None:
        max_workers = 32 if dataset_type in ["spider", "bird"] else 8
    
    # Route to appropriate evaluation method
    if dataset_type in ["spider", "bird"]:
        logger.info(f"=== Evaluating {dataset_type.upper()} Dataset ===")
        accuracy = evaluate_spider_bird(pkl_path=pkl_path, max_workers=max_workers)
        logger.info(f"\n{'='*60}")
        logger.info(f"  Overall Execution Accuracy: {accuracy * 100:.2f}%")
        logger.info(f"{'='*60}\n")
        return accuracy
        
    elif dataset_type == "spider2":
        logger.info(f"=== Evaluating Spider2-{dataset_split.upper()} Dataset ===")
        result = evaluate_spider2(
            pkl_path=pkl_path,
            dataset_split=dataset_split,
            sql_output_dir=sql_output_dir,
            max_workers=max_workers,
            timeout=timeout,
            skip_conversion=skip_conversion
        )
        if result == 0:
            logger.info("\n" + "="*60)
            logger.info("  Spider2 Evaluation Completed Successfully!")
            logger.info("="*60 + "\n")
        return result
        
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}. Expected 'spider', 'bird', or 'spider2'")


def main():
    parser = argparse.ArgumentParser(
        description="Unified evaluation script for Spider, BIRD, and Spider2 datasets"
    )
    parser.add_argument(
        "--pkl_path",
        type=str,
        default=None,
        help="Path to the pkl file with results (default: use config)"
    )
    parser.add_argument(
        "--dataset_type",
        type=str,
        choices=["spider", "bird", "spider2"],
        default=None,
        help="Dataset type (default: auto-detect from config)"
    )
    parser.add_argument(
        "--dataset_split",
        type=str,
        choices=["dev", "test", "lite", "snow"],
        default=None,
        help="Dataset split (auto-detected from config if not specified)"
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: 32 for Spider/BIRD, 8 for Spider2)"
    )
    
    # Spider2-specific arguments
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="SQL execution timeout in seconds (Spider2 only, default: use config)"
    )
    parser.add_argument(
        "--sql_output_dir",
        type=str,
        default=None,
        help="Directory for SQL output files (Spider2 only)"
    )
    parser.add_argument(
        "--skip_conversion",
        action="store_true",
        help="Skip pkl to SQL conversion for Spider2 (use existing SQL files)"
    )
    
    args = parser.parse_args()
    
    run_evaluation(
        pkl_path=args.pkl_path,
        dataset_type=args.dataset_type,
        dataset_split=args.dataset_split,
        max_workers=args.max_workers,
        timeout=args.timeout,
        sql_output_dir=args.sql_output_dir,
        skip_conversion=args.skip_conversion
    )


if __name__ == "__main__":
    main()
