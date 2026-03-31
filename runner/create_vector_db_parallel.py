import sys
sys.path.append(".")
from pathlib import Path
import traceback
import shutil
from argparse import ArgumentParser, SUPPRESS
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm

from app.vector_db.vector_db import make_vector_db, get_embedding_function
from app.dataset import load_dataset
from app.logger import logger

_WORKER_STATE = threading.local()


def _embedding_config_key(vector_database_config) -> tuple:
    return (
        vector_database_config.embedding_model_name_or_path,
        vector_database_config.api_type,
        vector_database_config.use_qwen3_embedding,
        vector_database_config.local_files_only,
        vector_database_config.normalize_embeddings,
        vector_database_config.base_url,
        vector_database_config.api_key,
    )


def _get_worker_embedding_function(vector_database_config):
    embedding_cache = getattr(_WORKER_STATE, "embedding_cache", None)
    if embedding_cache is None:
        embedding_cache = {}
        _WORKER_STATE.embedding_cache = embedding_cache

    cache_key = _embedding_config_key(vector_database_config)
    if cache_key not in embedding_cache:
        logger.info(
            f"Initializing embedding function for worker {threading.current_thread().name} "
            f"with model {vector_database_config.embedding_model_name_or_path}"
        )
        embedding_cache[cache_key] = get_embedding_function(
            model_name_or_path=vector_database_config.embedding_model_name_or_path,
            api_type=vector_database_config.api_type,
            use_qwen3_embedding=vector_database_config.use_qwen3_embedding,
            local_files_only=vector_database_config.local_files_only,
            normalize_embeddings=vector_database_config.normalize_embeddings,
            base_url=vector_database_config.base_url,
            api_key=vector_database_config.api_key,
        )
    return embedding_cache[cache_key]


def _collect_sqlite_db_paths(dataset) -> list[str]:
    db_paths = dataset.get_all_database_paths()
    logger.info(f"Found {len(db_paths)} unique databases in the dataset.")

    sqlite_db_paths = []
    for db_path in db_paths:
        if db_path.endswith(".sqlite") and Path(db_path).exists():
            sqlite_db_paths.append(db_path)

    skipped_count = len(db_paths) - len(sqlite_db_paths)
    if skipped_count > 0:
        logger.info(f"Skipping {skipped_count} non-SQLite/cloud databases (Vector DB not supported)")

    return sqlite_db_paths


def _resolve_db_parallel(vector_database_config, override_db_parallel: int | None = None) -> int:
    resolved = override_db_parallel or vector_database_config.db_parallel or vector_database_config.n_parallel
    if resolved < 1:
        raise ValueError(f"db_parallel must be >= 1, got {resolved}")
    return resolved


def _resolve_column_parallel(vector_database_config, override_column_parallel: int | None = None) -> int:
    resolved = override_column_parallel or vector_database_config.column_parallel or vector_database_config.n_parallel
    if resolved < 1:
        raise ValueError(f"column_parallel must be >= 1, got {resolved}")
    return resolved


def make_vector_db_for_db_path(db_path: str, vector_database_config, column_parallel: int | None = None):
    db_id = Path(db_path).stem
    success_flag_file = Path(vector_database_config.store_root_path) / db_id / "success_flag"

    if success_flag_file.exists():
        logger.info(f"Vector database for {db_id} already exists (success_flag found), skipping.")
        return True

    try:
        embedding_function = _get_worker_embedding_function(vector_database_config)
        
        success = make_vector_db(
            db_path=db_path,
            vector_db_path=Path(vector_database_config.store_root_path) / db_id,
            max_value_length=vector_database_config.max_value_length,
            batch_size=vector_database_config.batch_size,
            column_parallel=_resolve_column_parallel(vector_database_config, column_parallel),
            lower_meta_data=vector_database_config.lower_meta_data,
            embedding_function=embedding_function,
        )
        
        if not success:
            logger.error(f"Failed to make vector database for {db_id}")
            return False
        else:
            success_flag_file.parent.mkdir(parents=True, exist_ok=True)
            success_flag_file.touch()
            logger.info(f"Successfully made vector database for {db_id}")
            return True
            
    except Exception as e:
        logger.error(f"Failed to make vector database for {db_id}: {e}")
        logger.error(traceback.format_exc())
        vector_db_path = Path(vector_database_config.store_root_path) / db_id
        if vector_db_path.exists():
            shutil.rmtree(vector_db_path)
        return False


def run_vector_db_creation(
    dataset_snapshot_path: str,
    dataset_type: str,
    vector_database_config,
    db_parallel: int | None = None,
    column_parallel: int | None = None,
) -> None:
    logger.info(f"Loading dataset from {dataset_snapshot_path}")
    dataset = load_dataset(dataset_snapshot_path)

    if dataset_type.startswith("spider2"):
        logger.info(f"Skipping vector database creation for Spider2 dataset: {dataset_type}")
        return

    db_paths = _collect_sqlite_db_paths(dataset)
    if len(db_paths) == 0:
        logger.info("No SQLite databases found for vector DB creation")
        return
    db_parallel = _resolve_db_parallel(vector_database_config, db_parallel)
    column_parallel = _resolve_column_parallel(vector_database_config, column_parallel)
    logger.info(f"Vector DB concurrency: db_parallel={db_parallel}, column_parallel={column_parallel}")

    with ThreadPoolExecutor(max_workers=db_parallel) as executor:
        futures = {
            executor.submit(make_vector_db_for_db_path, db_path, vector_database_config, column_parallel): db_path
            for db_path in db_paths
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Creating vector databases"):
            db_path = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Unhandled exception for database {db_path}: {e}")

    logger.info("All vector database creation tasks completed.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--db_parallel", type=int, default=None, help="Number of databases to process in parallel")
    parser.add_argument("--column_parallel", type=int, default=None, help="Number of columns to scan in parallel within a single database")
    parser.add_argument("--n_parallel", dest="legacy_n_parallel", type=int, default=None, help=SUPPRESS)
    args = parser.parse_args()
    from app.config import config

    db_parallel = args.db_parallel if args.db_parallel is not None else config.vector_database_config.db_parallel
    if args.legacy_n_parallel is not None:
        logger.warning("`--n_parallel` is deprecated and will be removed in a future release. Use `--db_parallel` instead.")
        if args.db_parallel is not None:
            logger.warning("Ignoring deprecated `--n_parallel` because `--db_parallel` was also provided.")
        else:
            db_parallel = args.legacy_n_parallel
    column_parallel = args.column_parallel if args.column_parallel is not None else config.vector_database_config.column_parallel
    run_vector_db_creation(
        dataset_snapshot_path=config.dataset_config.save_path,
        dataset_type=config.dataset_config.type,
        vector_database_config=config.vector_database_config,
        db_parallel=db_parallel,
        column_parallel=column_parallel,
    )
