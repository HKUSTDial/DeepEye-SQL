"""
Convert dataset pkl results to SQL files for evaluation.
Automatically detects dataset type and chooses appropriate output format:
- Spider/Bird: Single JSON file with {question_id: sql}
- Spider2: Individual SQL files per instance_id
"""

import sys
sys.path.append(".")

import os
import json
import argparse
from pathlib import Path
from typing import Optional

from app.dataset import load_dataset
from app.logger import logger


def convert_to_json_file(pkl_path: str, output_path: Optional[str] = None):
    """
    Convert pkl dataset to a single JSON file (for Spider/Bird datasets).
    Format: {question_id: sql_string}
    
    Args:
        pkl_path: Path to the pkl file with results.
        output_path: Optional output path. If None, replaces .pkl with .json
    """
    dataset = load_dataset(pkl_path)
    data = {}
    
    for item in dataset:
        final_sql = None
        
        # Use existing final_selected_sql if available
        if item.final_selected_sql is not None:
            final_sql = item.final_selected_sql
        
        # Fallback: Error placeholder
        if final_sql is None:
            logger.warning(f"Item {item.question_id}: No valid SQL found, using 'Error'")
            final_sql = "Error"
            
        data[str(item.question_id)] = final_sql.strip()
    
    # Determine output path
    if output_path is None:
        output_path = pkl_path.replace(".pkl", ".json")
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    
    logger.info(f"Dataset converted to JSON file successfully")
    logger.info(f"Output: {output_path}")
    logger.info(f"Total items: {len(dataset)}")


def convert_to_sql_files(pkl_path: str, output_dir: Optional[str] = None):
    """
    Convert pkl dataset to individual SQL files (for Spider2 datasets).
    Format: One SQL file per instance_id (e.g., bq011.sql, sf_bq001.sql)
    
    Args:
        pkl_path: Path to the pkl file with results.
        output_dir: Directory to save SQL files. If None, creates sql_output dir.
    """
    dataset = load_dataset(pkl_path)
    
    # Determine output directory
    if output_dir is None:
        output_dir = str(Path(pkl_path).parent / "sql_output")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for item in dataset:
        # Get instance_id (Spider2 specific)
        instance_id = getattr(item, "instance_id", None)
        if instance_id is None:
            # Fallback to question_id for compatibility
            instance_id = str(item.question_id)
        
        final_sql = None
        
        # Use existing final_selected_sql if available
        if item.final_selected_sql is not None:
            final_sql = item.final_selected_sql

        if final_sql is None:
            logger.warning(f"Item {instance_id}: No valid SQL found, using 'Error'")
            final_sql = "Error"
        
        # Write SQL file
        sql_file_path = output_path / f"{instance_id}.sql"
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(final_sql.strip())
    
    logger.info(f"Dataset converted to SQL files successfully")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Total items: {len(dataset)}")


def auto_convert(
    pkl_path: str,
    output_path: Optional[str] = None,
    force_format: Optional[str] = None
):
    """
    Automatically detect dataset type and convert to appropriate format.
    
    Args:
        pkl_path: Path to the pkl file with results.
        output_path: Optional output path (for json) or directory (for sql files).
        force_format: Force a specific format ('json' or 'sql_files'). 
                     If None, auto-detects from config.
    """
    from app.config import config
    
    # Determine output format
    if force_format is not None:
        output_format = force_format
        logger.info(f"Using forced format: {output_format}")
    else:
        # Auto-detect from dataset type
        dataset_type = config.dataset_config.type
        
        if dataset_type == "spider2":
            output_format = "sql_files"
        else:  # spider, bird
            output_format = "json"
        
        logger.info(f"Auto-detected format '{output_format}' for dataset type '{dataset_type}'")
    
    # Convert based on format
    if output_format == "json":
        convert_to_json_file(pkl_path, output_path)
    elif output_format == "sql_files":
        convert_to_sql_files(pkl_path, output_path)
    else:
        raise ValueError(f"Invalid output format: {output_format}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert pkl results to SQL files (auto-detects format based on dataset type)"
    )
    parser.add_argument(
        "--pkl_path",
        type=str,
        default=None,
        help="Path to the pkl file (default: use config sql_selection save_path)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path (JSON file) or directory (SQL files). Default: auto-determined"
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["json", "sql_files"],
        default=None,
        help="Force output format (json or sql_files). If not specified, auto-detects from config"
    )
    args = parser.parse_args()
    
    from app.config import config
    
    # Determine pkl path
    pkl_path = args.pkl_path
    if pkl_path is None:
        pkl_path = config.sql_selection_config.save_path
    
    logger.info(f"Converting {pkl_path}")
    
    auto_convert(
        pkl_path=pkl_path,
        output_path=args.output,
        force_format=args.format
    )


if __name__ == "__main__":
    main()
