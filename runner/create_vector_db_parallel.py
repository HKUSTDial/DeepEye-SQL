import sys
sys.path.append(".")
from pathlib import Path
import traceback
import shutil
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from app.vector_db.vector_db import make_vector_db, get_embedding_function
from app.config import config, VectorDatabaseConfig
from app.dataset import load_dataset
from app.logger import logger


def make_vector_db_for_db_path(db_path: str, vector_database_config: VectorDatabaseConfig):
    db_id = Path(db_path).stem
    success_flag_file = Path(vector_database_config.store_root_path) / db_id / "success_flag"
    
    if success_flag_file.exists():
        logger.info(f"Vector database for {db_id} already exists (success_flag found), skipping.")
        return True

    try:
        embedding_function = get_embedding_function(
            model_name_or_path=vector_database_config.embedding_model_name_or_path,
            api_type=vector_database_config.api_type,
            use_qwen3_embedding=vector_database_config.use_qwen3_embedding,
            local_files_only=vector_database_config.local_files_only,
            normalize_embeddings=vector_database_config.normalize_embeddings,
            base_url=vector_database_config.base_url,
            api_key=vector_database_config.api_key,
        )
        
        success = make_vector_db(
            db_path=db_path,
            vector_db_path=Path(vector_database_config.store_root_path) / db_id,
            max_value_length=vector_database_config.max_value_length,
            batch_size=vector_database_config.batch_size,
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


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--n_parallel", type=int, default=4, help="Number of databases to process in parallel")
    args = parser.parse_args()

    logger.info(f"Loading dataset from {config.dataset_config.save_path}")
    dataset = load_dataset(config.dataset_config.save_path)
    db_paths = dataset.get_all_database_paths()
    logger.info(f"Found {len(db_paths)} unique databases in the dataset.")

    with ThreadPoolExecutor(max_workers=args.n_parallel) as executor:
        futures = {executor.submit(make_vector_db_for_db_path, db_path, config.vector_database_config): db_path for db_path in db_paths}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Creating vector databases"):
            db_path = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Unhandled exception for database {db_path}: {e}")

    logger.info("All vector database creation tasks completed.")
