"""
Convert dataset snapshots to SQL files for evaluation.
Automatically detects dataset type and chooses appropriate output format:
- Spider/Bird: Single JSON file with {question_id: sql}
- Spider2: Individual SQL files per instance_id
"""

import sys
sys.path.append(".")

import json
import argparse
from pathlib import Path
from typing import Optional

from app.dataset import load_dataset
from app.logger import logger


def _default_json_output_path(snapshot_path: str) -> str:
    snapshot = Path(snapshot_path)
    if snapshot.suffix:
        return str(snapshot.with_suffix(".json"))
    return f"{snapshot_path}.json"


def convert_to_json_file(snapshot_path: str, output_path: Optional[str] = None):
    """
    Convert a dataset snapshot to a single JSON file (for Spider/Bird datasets).
    Format: {question_id: sql_string}
    """
    dataset = load_dataset(snapshot_path)
    data = {}

    for item in dataset:
        final_sql = item.final_selected_sql
        if final_sql is None:
            logger.warning(f"Item {item.question_id}: No valid SQL found, using 'Error'")
            final_sql = "Error"
        data[str(item.question_id)] = final_sql.strip()

    if output_path is None:
        output_path = _default_json_output_path(snapshot_path)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    logger.info("Dataset converted to JSON file successfully")
    logger.info(f"Output: {output_path}")
    logger.info(f"Total items: {len(dataset)}")


def convert_to_sql_files(snapshot_path: str, output_dir: Optional[str] = None):
    """
    Convert a dataset snapshot to individual SQL files (for Spider2 datasets).
    Format: One SQL file per instance_id.
    """
    dataset = load_dataset(snapshot_path)

    if output_dir is None:
        output_dir = str(Path(snapshot_path).parent / "sql_output")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for item in dataset:
        instance_id = getattr(item, "instance_id", None) or str(item.question_id)
        final_sql = item.final_selected_sql
        if final_sql is None:
            logger.warning(f"Item {instance_id}: No valid SQL found, using 'Error'")
            final_sql = "Error"

        sql_file_path = output_path / f"{instance_id}.sql"
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(final_sql.strip())

    logger.info("Dataset converted to SQL files successfully")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Total items: {len(dataset)}")


def auto_convert(
    snapshot_path: str,
    output_path: Optional[str] = None,
    force_format: Optional[str] = None,
):
    from app.config import config

    if force_format is not None:
        output_format = force_format
        logger.info(f"Using forced format: {output_format}")
    else:
        dataset_type = config.dataset_config.type
        output_format = "sql_files" if dataset_type == "spider2" else "json"
        logger.info(f"Auto-detected format '{output_format}' for dataset type '{dataset_type}'")

    if output_format == "json":
        convert_to_json_file(snapshot_path, output_path)
    elif output_format == "sql_files":
        convert_to_sql_files(snapshot_path, output_path)
    else:
        raise ValueError(f"Invalid output format: {output_format}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert dataset snapshots to SQL files (auto-detects format based on dataset type)"
    )
    parser.add_argument(
        "--snapshot_path",
        "--pkl_path",
        dest="snapshot_path",
        type=str,
        default=None,
        help="Path to the dataset snapshot (legacy alias: --pkl_path). Default: use config sql_selection save_path",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path (JSON file) or directory (SQL files). Default: auto-determined",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["json", "sql_files"],
        default=None,
        help="Force output format (json or sql_files). If not specified, auto-detects from config",
    )
    args = parser.parse_args()

    from app.config import config

    snapshot_path = args.snapshot_path or config.sql_selection_config.save_path
    logger.info(f"Converting dataset snapshot {snapshot_path}")
    auto_convert(
        snapshot_path=snapshot_path,
        output_path=args.output,
        force_format=args.format,
    )


if __name__ == "__main__":
    main()
