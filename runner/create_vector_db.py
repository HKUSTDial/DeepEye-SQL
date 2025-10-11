import sys
sys.path.append(".")
from pathlib import Path
import traceback
import shutil
from argparse import ArgumentParser
from app.vector_db.vector_db import make_vector_db, get_embedding_function
from app.config import config, VectorDatabaseConfig
from app.dataset import load_dataset
from app.logger import logger


def make_vector_db_for_db_id(db_path: str, vector_database_config: VectorDatabaseConfig):
    try:
        embedding_function = get_embedding_function(
            model_name_or_path=vector_database_config.embedding_model_name_or_path,
            use_qwen3_embedding=vector_database_config.use_qwen3_embedding,
            local_files_only=vector_database_config.local_files_only,
            normalize_embeddings=vector_database_config.normalize_embeddings,
            base_url=vector_database_config.base_url,
            api_key=vector_database_config.api_key,
        )
        db_id = Path(db_path).stem
        success = make_vector_db(
            db_path=db_path,
            vector_db_path=Path(vector_database_config.store_root_path) / db_id,
            max_value_length=vector_database_config.max_value_length,
            lower_meta_data=vector_database_config.lower_meta_data,
            embedding_function=embedding_function,
        )
        if not success:
            logger.error(f"Failed to make vector database for {db_id}")
        else:
            success_flag_file = Path(vector_database_config.store_root_path) / db_id / "success_flag"
            success_flag_file.touch()
            logger.info(f"Successfully made vector database for {db_id}")
    except Exception as e:
        logger.error(f"Failed to make vector database for {db_id}")
        logger.error(traceback.format_exc())
        vector_db_path = str(Path(vector_database_config.store_root_path) / db_id)
        if Path(vector_db_path).exists():
            shutil.rmtree(vector_db_path)


if __name__ == "__main__":
    """
    We will use bash script to make vector database in parallel,
    so we need to skip the databases that have already been made or in progress.
    We only process one db_id at a time.
    """
    parser = ArgumentParser()
    parser.add_argument("--db_path", type=str, required=True)
    args = parser.parse_args()
    # dataset = load_dataset(config.dataset_config.save_path)
    # for db_path in dataset.get_all_database_paths():
    logger.info(f"Making vector database for {args.db_path}")
    make_vector_db_for_db_id(args.db_path, config.vector_database_config)
    logger.info(f"Successfully made vector database for {args.db_path}")