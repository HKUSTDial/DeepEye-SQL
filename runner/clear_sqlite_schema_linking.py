"""
Script to clear pipeline results for SQLite database samples in Spider2-Lite dataset.
This allows re-running pipeline steps for SQLite samples only.
"""

import sys
sys.path.append(".")

from app.dataset import load_dataset, save_dataset, Spider2DataItem
from app.config import config
from app.logger import logger
from pathlib import Path
from tqdm import tqdm
from typing import Optional


def clear_sqlite_results_for_step(pkl_path: str, step: str, backup: bool = True):
    """
    Clear results for a specific pipeline step for SQLite database samples.
    
    Args:
        pkl_path: Path to the pkl file
        step: Pipeline step name ('schema_linking', 'sql_generation', 'sql_revision', 'sql_selection')
        backup: Whether to create a backup before modifying
    """
    pkl_path = Path(pkl_path)
    
    if not pkl_path.exists():
        raise FileNotFoundError(f"Pkl file not found: {pkl_path}")
    
    # Create backup if requested
    if backup:
        backup_path = pkl_path.with_suffix('.pkl.backup')
        logger.info(f"Creating backup at {backup_path}")
        import shutil
        shutil.copy2(pkl_path, backup_path)
    
    # Load dataset
    logger.info(f"Loading dataset from {pkl_path}")
    dataset = load_dataset(str(pkl_path))
    
    # Count SQLite samples
    sqlite_count = 0
    total_count = len(dataset)
    
    # Define fields to clear for each step
    step_fields = {
        'schema_linking': [
            'direct_linked_tables_and_columns',
            'reversed_linked_tables_and_columns',
            'value_linked_tables_and_columns',
            'final_linked_tables_and_columns',
            'database_schema_after_schema_linking',
            'direct_linking_recall',
            'reversed_linking_recall',
            'value_linking_recall',
            'final_linking_recall',
            'schema_linking_time',
            'schema_linking_llm_cost',
        ],
        'sql_generation': [
            'sql_candidates',
            'sql_generation_time',
            'sql_generation_llm_cost',
        ],
        'sql_revision': [
            'sql_candidates_after_revision',
            'sql_revision_time',
            'sql_revision_llm_cost',
        ],
        'sql_selection': [
            'final_selected_sql',
            'sql_selection_time',
            'sql_selection_llm_cost',
        ],
    }
    
    if step not in step_fields:
        raise ValueError(f"Unknown step: {step}. Available steps: {list(step_fields.keys())}")
    
    fields_to_clear = step_fields[step]
    time_field = f"{step}_time"
    cost_field = f"{step}_llm_cost"
    
    logger.info(f"Clearing {step} results for SQLite samples...")
    for data_item in tqdm(dataset, desc="Processing samples"):
        # Check if this is a Spider2DataItem with db_type field
        if isinstance(data_item, Spider2DataItem) and hasattr(data_item, 'db_type'):
            if data_item.db_type == 'sqlite':
                sqlite_count += 1
                
                # First, subtract time from total_time if it exists
                if hasattr(data_item, time_field) and getattr(data_item, time_field) is not None:
                    step_time = getattr(data_item, time_field)
                    if hasattr(data_item, 'total_time') and data_item.total_time is not None:
                        data_item.total_time -= step_time
                        # Ensure non-negative
                        data_item.total_time = max(0.0, data_item.total_time)
                
                # Subtract llm_cost from total_llm_cost if it exists
                if hasattr(data_item, cost_field) and getattr(data_item, cost_field) is not None:
                    step_cost = getattr(data_item, cost_field)
                    if hasattr(data_item, 'total_llm_cost') and data_item.total_llm_cost is not None:
                        if isinstance(data_item.total_llm_cost, dict) and isinstance(step_cost, dict):
                            for key in ['prompt_tokens', 'completion_tokens', 'total_tokens']:
                                if key in data_item.total_llm_cost and key in step_cost:
                                    data_item.total_llm_cost[key] -= step_cost[key]
                                    # Ensure non-negative
                                    data_item.total_llm_cost[key] = max(0, data_item.total_llm_cost[key])
                
                # Then clear all step fields
                for field in fields_to_clear:
                    if hasattr(data_item, field):
                        setattr(data_item, field, None)
    
    logger.info(f"Cleared {step} results for {sqlite_count} SQLite samples out of {total_count} total samples")
    
    # Save the modified dataset
    logger.info(f"Saving modified dataset to {pkl_path}")
    save_dataset(dataset, str(pkl_path))
    logger.info("Done!")


def clear_sqlite_results_for_all_steps(
    schema_linking_path: Optional[str] = None,
    sql_generation_path: Optional[str] = None,
    sql_revision_path: Optional[str] = None,
    sql_selection_path: Optional[str] = None,
    backup: bool = True
):
    """
    Clear results for all pipeline steps for SQLite database samples.
    
    Args:
        schema_linking_path: Path to schema linking pkl file (uses config if None)
        sql_generation_path: Path to sql generation pkl file (uses config if None)
        sql_revision_path: Path to sql revision pkl file (uses config if None)
        sql_selection_path: Path to sql selection pkl file (uses config if None)
        backup: Whether to create backups before modifying
    """
    steps_config = [
        ('schema_linking', schema_linking_path, config.schema_linking_config.save_path),
        ('sql_generation', sql_generation_path, config.sql_generation_config.save_path),
        ('sql_revision', sql_revision_path, config.sql_revision_config.save_path),
        ('sql_selection', sql_selection_path, config.sql_selection_config.save_path),
    ]
    
    for step, provided_path, config_path in steps_config:
        pkl_path = provided_path if provided_path is not None else config_path
        pkl_path = Path(pkl_path)
        
        if pkl_path.exists():
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {step} step...")
            logger.info(f"{'='*60}")
            clear_sqlite_results_for_step(str(pkl_path), step, backup=backup)
        else:
            logger.warning(f"Skipping {step} step: pkl file not found at {pkl_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Clear pipeline results for SQLite database samples in Spider2-Lite dataset"
    )
    parser.add_argument(
        "--step",
        type=str,
        choices=['schema_linking', 'sql_generation', 'sql_revision', 'sql_selection', 'all'],
        default='all',
        help="Pipeline step to clear. Use 'all' to clear all steps. Default: 'all'"
    )
    parser.add_argument(
        "--schema-linking-path",
        type=str,
        default=None,
        help="Path to schema linking pkl file (uses config if not provided)"
    )
    parser.add_argument(
        "--sql-generation-path",
        type=str,
        default=None,
        help="Path to sql generation pkl file (uses config if not provided)"
    )
    parser.add_argument(
        "--sql-revision-path",
        type=str,
        default=None,
        help="Path to sql revision pkl file (uses config if not provided)"
    )
    parser.add_argument(
        "--sql-selection-path",
        type=str,
        default=None,
        help="Path to sql selection pkl file (uses config if not provided)"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Don't create backups before modifying pkl files"
    )
    
    args = parser.parse_args()
    
    if args.step == 'all':
        # Clear all steps
        clear_sqlite_results_for_all_steps(
            schema_linking_path=args.schema_linking_path,
            sql_generation_path=args.sql_generation_path,
            sql_revision_path=args.sql_revision_path,
            sql_selection_path=args.sql_selection_path,
            backup=not args.no_backup
        )
    else:
        # Clear a specific step
        step_paths = {
            'schema_linking': (args.schema_linking_path, config.schema_linking_config.save_path),
            'sql_generation': (args.sql_generation_path, config.sql_generation_config.save_path),
            'sql_revision': (args.sql_revision_path, config.sql_revision_config.save_path),
            'sql_selection': (args.sql_selection_path, config.sql_selection_config.save_path),
        }
        provided_path, config_path = step_paths[args.step]
        pkl_path = provided_path if provided_path is not None else config_path
        
        clear_sqlite_results_for_step(pkl_path, args.step, backup=not args.no_backup)
