from chromadb import PersistentClient
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction, OpenAIEmbeddingFunction
from pathlib import Path
import shutil
from tqdm import tqdm
from typing import List
from .qwen_embedding_function import QwenEmbeddingFunction
from app.db_utils import load_table_names, load_column_names_and_types, execute_sql
from app.logger import logger
import re
import pandas as pd


UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

NUMBER_PATTERN = re.compile(
    r'^[0-9]+\.?[0-9]*$'
)

def _is_uuid_column(column_values: List[str]) -> bool:
    return all(UUID_PATTERN.match(value) for value in column_values)


def _is_number_column(column_values: List[str]) -> bool:
    return all(NUMBER_PATTERN.match(value) for value in column_values)


def get_embedding_function(model_name_or_path: str, use_qwen3_embedding: bool = False, local_files_only: bool = False, normalize_embeddings: bool = False, base_url: str = None, api_key: str = None):
    if api_key is None:
        if use_qwen3_embedding:
            logger.info(f"Using Qwen3 embedding function for {model_name_or_path}")
            return QwenEmbeddingFunction(
                model_name=model_name_or_path,
                device="cuda",
                trust_remote_code=True,
                local_files_only=local_files_only,
                normalize_embeddings=normalize_embeddings
            )
        else:
            logger.info(f"Using SentenceTransformer embedding function for {model_name_or_path}")
            return SentenceTransformerEmbeddingFunction(
                model_name=model_name_or_path,
                device="cuda",
                trust_remote_code=True,
                local_files_only=local_files_only,
                normalize_embeddings=normalize_embeddings
            )
    else:
        logger.info(f"Using OpenAI embedding function for {model_name_or_path}")
        return OpenAIEmbeddingFunction(model_name=model_name_or_path, api_base=base_url, api_key=api_key)


def make_vector_db(db_path: str, vector_db_path: str, max_value_length: int = 100, lower_meta_data=True, embedding_function=None):
    """
    Make a vector database from a database path.
    """
    if Path(vector_db_path).exists():
        # shutil.rmtree(vector_db_path)
        logger.info(f"Vector database already exists for {db_path}, skipping it...")
        return True
    logger.info(f"Making vector database for {db_path}, vector database path: {vector_db_path}")
    db_id = Path(db_path).stem
    client = PersistentClient(path=vector_db_path)
    collection = client.create_collection(
        name=db_id,
        embedding_function=embedding_function,
        metadata={"hnsw:space": "cosine"}
    )
    id_counter = 0
    for table_name in load_table_names(db_path):
        column_names_and_types = load_column_names_and_types(db_path, table_name)
        for column_name, column_type in tqdm(column_names_and_types, desc=f"Making vector database for {db_id}.{table_name}"):
            if column_type.upper() != "TEXT" and not column_type.upper().startswith("VARCHAR") and not column_type.upper().startswith("CHAR"):
                logger.info(f"Skipping {db_id}.{table_name}.{column_name} ({column_type}) because it is not a text column")
                continue
            else:
                logger.info(f"Processing {db_id}.{table_name}.{column_name} ({column_type})...")
            query_sql = f"""
            SELECT DISTINCT `{column_name}` FROM `{table_name}` 
            WHERE `{column_name}` IS NOT NULL 
            AND LENGTH(CAST(`{column_name}` AS TEXT)) <= {max_value_length};
            """
            result = execute_sql(db_path, query_sql, timeout=300)
            if result.result_type in ["success", "empty_result"]:
                value_examples = [str(row[0]) for row in result.result_rows]
                
                if len(value_examples) == 0:
                    logger.info(f"Skipping {db_id}.{table_name}.{column_name} because it has no value examples")
                    continue
                
                if _is_uuid_column(value_examples):
                    logger.info(f"Skipping {db_id}.{table_name}.{column_name} because it is a uuid column")
                    continue
                if _is_number_column(value_examples):
                    logger.info(f"Skipping {db_id}.{table_name}.{column_name} because it is a number column")
                    continue
                
                # Process in batches to stay under ChromaDB's batch size limit
                batch_size = 1024
                for i in range(0, len(value_examples), batch_size):
                    batch_examples = value_examples[i:i + batch_size]
                    collection.add(
                        ids=[str(j) for j in range(id_counter, id_counter + len(batch_examples))],
                        documents=batch_examples,
                        metadatas=[
                            {"db_id": db_id.lower(), "table_name": table_name.lower(), "column_name": column_name.lower()} 
                            if lower_meta_data else {"db_id": db_id, "table_name": table_name, "column_name": column_name}
                            for _ in range(len(batch_examples))
                        ],
                    )
                    id_counter += len(batch_examples)
            else:
                logger.error(f"Error executing SQL for {db_id}.{table_name}.{column_name}: {result.error_message}")
                # This database is failed to be processed, deleting the vector database
                shutil.rmtree(vector_db_path)
                return False
    return True


def query_vector_db(vector_db_path: str, table_name: str, column_name: str, queries: List[str], top_k: int = 5, lower_meta_data=True, embedding_function=None):
    """
    Query the vector database for the most relevant results.
    """
    db_id = Path(vector_db_path).stem
    client = PersistentClient(path=vector_db_path)
    collection = client.get_collection(name=db_id, embedding_function=embedding_function)
    result = collection.query(
        query_texts=queries,
        where={"$and": [{"table_name": {"$eq": table_name.lower() if lower_meta_data else table_name}}, {"column_name": {"$eq": column_name.lower() if lower_meta_data else column_name}}]},
        n_results=top_k,
    )
    return result