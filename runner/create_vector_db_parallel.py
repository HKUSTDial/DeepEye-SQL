import sys
sys.path.append(".")
from pathlib import Path
import shutil
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from app.vector_db.vector_db import make_vector_db, get_embedding_function
from app.dataset import load_dataset
from app.logger import configure_logger, logger
from app.progress import log_progress

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
        vector_database_config.embedding_device,
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
            embedding_device=vector_database_config.embedding_device,
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


def make_vector_db_for_db_path(
    db_path: str,
    vector_database_config,
    *,
    parallelism: int,
    embedding_batch_size: int,
    work_semaphore: threading.BoundedSemaphore,
    progress_log_interval: int,
):
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
            embedding_batch_size=embedding_batch_size,
            parallelism=parallelism,
            lower_meta_data=vector_database_config.lower_meta_data,
            embedding_function=embedding_function,
            build_backend=vector_database_config.build_backend,
            work_semaphore=work_semaphore,
            progress_log_interval=progress_log_interval,
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
        logger.exception(f"Failed to make vector database for {db_id}: {e}")
        vector_db_path = Path(vector_database_config.store_root_path) / db_id
        if vector_db_path.exists():
            shutil.rmtree(vector_db_path)
        return False


def run_vector_db_creation(
    dataset_snapshot_path: str,
    dataset_type: str,
    vector_database_config,
    parallelism: int,
    embedding_batch_size: int,
    progress_log_interval: int,
) -> None:
    if parallelism < 1:
        raise ValueError(f"parallelism must be >= 1, got {parallelism}")
    if embedding_batch_size < 1:
        raise ValueError(f"embedding_batch_size must be >= 1, got {embedding_batch_size}")

    logger.info(f"Loading dataset from {dataset_snapshot_path}")
    dataset = load_dataset(dataset_snapshot_path)

    if dataset_type.startswith("spider2"):
        logger.info(f"Skipping vector database creation for Spider2 dataset: {dataset_type}")
        return

    db_paths = _collect_sqlite_db_paths(dataset)
    if len(db_paths) == 0:
        logger.info("No SQLite databases found for vector DB creation")
        return
    logger.info(
        f"Vector DB concurrency: parallelism={parallelism}, "
        f"embedding_batch_size={embedding_batch_size}"
    )

    work_semaphore = threading.BoundedSemaphore(parallelism)
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(
                make_vector_db_for_db_path,
                db_path,
                vector_database_config,
                parallelism=parallelism,
                embedding_batch_size=embedding_batch_size,
                work_semaphore=work_semaphore,
                progress_log_interval=progress_log_interval,
            ): db_path
            for db_path in db_paths
        }
        completed_databases = 0
        succeeded_databases = 0
        failed_databases = 0
        for future in as_completed(futures):
            db_path = futures[future]
            try:
                if future.result():
                    succeeded_databases += 1
                else:
                    failed_databases += 1
            except Exception as e:
                failed_databases += 1
                logger.exception(f"Unhandled exception for database {db_path}: {e}")
            completed_databases += 1
            log_progress(
                f"Vector DB databases (success={succeeded_databases}, failed={failed_databases})",
                completed_databases,
                len(futures),
                progress_log_interval,
                previous_completed=completed_databases - 1,
            )

    logger.info(
        "All vector database creation tasks completed: "
        f"success={succeeded_databases}, failed={failed_databases}"
    )


if __name__ == "__main__":
    ArgumentParser().parse_args()
    from app.config import get_config

    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)

    run_vector_db_creation(
        dataset_snapshot_path=app_config.dataset_config.save_path,
        dataset_type=app_config.dataset_config.type,
        vector_database_config=app_config.vector_database_config,
        parallelism=app_config.run_config.parallelism,
        embedding_batch_size=app_config.run_config.embedding_batch_size,
        progress_log_interval=app_config.run_config.progress_log_interval,
    )
