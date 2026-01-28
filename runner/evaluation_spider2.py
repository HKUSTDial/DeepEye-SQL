"""
Spider2 evaluation runner.
Converts pkl results to SQL files and calls the official evaluation script.
"""

import sys
sys.path.append(".")

import os
import argparse
import subprocess
from pathlib import Path
from app.logger import logger
from app.config import config


def run_spider2_evaluation(
    pkl_path: str = None,
    dataset_type: str = "spider2-lite",
    sql_output_dir: str = None,
    max_workers: int = 8,
    timeout: int = 60,
    skip_conversion: bool = False
):
    """
    Run Spider2 evaluation pipeline.
    
    Args:
        pkl_path: Path to the pkl file with results.
        dataset_type: "spider2-lite" or "spider2-snow".
        sql_output_dir: Directory for SQL output files.
        max_workers: Number of parallel workers for evaluation.
        timeout: SQL execution timeout in seconds.
        skip_conversion: Skip pkl to SQL conversion (use existing SQL files).
    """
    # Determine paths
    if pkl_path is None:
        pkl_path = config.sql_selection_config.save_path
    
    pkl_path = Path(pkl_path)
    
    if sql_output_dir is None:
        sql_output_dir = pkl_path.parent / "sql_output"
    else:
        sql_output_dir = Path(sql_output_dir)
    
    # Step 1: Convert pkl to SQL files (if not skipping)
    if not skip_conversion:
        logger.info(f"Step 1: Converting {pkl_path} to SQL files...")
        from runner.convert_pkl_to_spider2_sql import convert_to_spider2_sql_files
        convert_to_spider2_sql_files(
            pkl_path=str(pkl_path),
            output_dir=str(sql_output_dir),
            use_sc_fallback=True
        )
    else:
        logger.info("Step 1: Skipping pkl conversion (using existing SQL files)")
    
    # Step 2: Run official evaluation script
    logger.info(f"Step 2: Running official {dataset_type} evaluation...")
    
    # Determine paths based on dataset type
    project_root = Path(__file__).parent.parent
    
    if dataset_type == "spider2-lite":
        eval_script = project_root / "data" / "spider2-lite" / "evaluation_suite" / "evaluate.py"
        gold_dir = project_root / "data" / "spider2-lite" / "evaluation_suite" / "gold"
    elif dataset_type == "spider2-snow":
        eval_script = project_root / "data" / "spider2-snow" / "evaluation_suite" / "evaluate.py"
        gold_dir = project_root / "data" / "spider2-snow" / "evaluation_suite" / "gold"
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")
    
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


def main():
    parser = argparse.ArgumentParser(description="Run Spider2 evaluation")
    parser.add_argument(
        "--pkl_path",
        type=str,
        default=None,
        help="Path to the pkl file with results (default: use config)"
    )
    parser.add_argument(
        "--dataset_type",
        type=str,
        choices=["spider2-lite", "spider2-snow"],
        default="spider2-lite",
        help="Dataset type to evaluate"
    )
    parser.add_argument(
        "--sql_output_dir",
        type=str,
        default=None,
        help="Directory for SQL output files"
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=8,
        help="Number of parallel workers"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="SQL execution timeout in seconds"
    )
    parser.add_argument(
        "--skip_conversion",
        action="store_true",
        help="Skip pkl to SQL conversion (use existing SQL files)"
    )
    
    args = parser.parse_args()
    
    run_spider2_evaluation(
        pkl_path=args.pkl_path,
        dataset_type=args.dataset_type,
        sql_output_dir=args.sql_output_dir,
        max_workers=args.max_workers,
        timeout=args.timeout,
        skip_conversion=args.skip_conversion
    )


if __name__ == "__main__":
    main()
