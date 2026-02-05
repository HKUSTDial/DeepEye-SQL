from chromadb import PersistentClient
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction, OpenAIEmbeddingFunction
from pathlib import Path
import shutil
from tqdm import tqdm
from typing import List, Dict, Any
from .qwen_embedding_function import QwenEmbeddingFunction
from app.db_utils import load_table_names, load_column_names_and_types, execute_sql
from app.logger import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import pandas as pd
import uuid


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


def get_collection_name(db_id: str) -> str:
    """
    Get a valid ChromaDB collection name from a database ID.
    ChromaDB requires 3-512 characters from [a-zA-Z0-9._-], 
    starting and ending with a character in [a-zA-Z0-9].
    """
    # Replace any invalid characters with underscore
    name = re.sub(r'[^a-zA-Z0-9._-]', '_', db_id)
    
    # Ensure it starts and ends with alphanumeric
    if not re.match(r'^[a-zA-Z0-9]', name):
        name = "db_" + name
    if not re.match(r'.*[a-zA-Z0-9]$', name):
        name = name + "_db"
        
    # Ensure length is at least 3
    while len(name) < 3:
        name = "db_" + name
        
    return name


def get_embedding_function(
    model_name_or_path: str, 
    api_type: str = "local",
    use_qwen3_embedding: bool = False, 
    local_files_only: bool = False, 
    normalize_embeddings: bool = False, 
    base_url: str = None, 
    api_key: str = None
):
    if api_type == "local":
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
    elif api_type == "openai":
        logger.info(f"Using OpenAI embedding function for {model_name_or_path}")
        return OpenAIEmbeddingFunction(
            model_name=model_name_or_path, 
            api_base=base_url, 
            api_key=api_key
        )
    else:
        raise ValueError(f"Unsupported embedding api_type: {api_type}")


def _process_one_column(
    db_path: str, 
    table_name: str, 
    column_name: str, 
    column_type: str, 
    max_value_length: int, 
    batch_size: int, 
    lower_meta_data: bool, 
    collection: Any, 
    db_id: str
):
    if column_type.upper() != "TEXT" and not column_type.upper().startswith("VARCHAR") and not column_type.upper().startswith("CHAR"):
        return
    
    query_sql = f"""
    SELECT DISTINCT `{column_name}` FROM `{table_name}` 
    WHERE `{column_name}` IS NOT NULL 
    AND LENGTH(CAST(`{column_name}` AS TEXT)) <= {max_value_length};
    """
    result = execute_sql(db_path, query_sql, timeout=100000)
    if result.result_type in ["success", "empty_result"]:
        value_examples = [str(row[0]) for row in result.result_rows]
        
        if len(value_examples) == 0:
            return
        
        if _is_uuid_column(value_examples) or _is_number_column(value_examples):
            return
        
        # Process in batches to stay under ChromaDB's batch size limit
        for i in tqdm(range(0, len(value_examples), batch_size), desc=f"Adding batches for {column_name}", leave=False):
            batch_examples = value_examples[i:i + batch_size]
            collection.add(
                ids=[str(uuid.uuid4()) for _ in range(len(batch_examples))],
                documents=batch_examples,
                metadatas=[
                    {"db_id": db_id.lower(), "table_name": table_name.lower(), "column_name": column_name.lower()} 
                    if lower_meta_data else {"db_id": db_id, "table_name": table_name, "column_name": column_name}
                    for _ in range(len(batch_examples))
                ],
            )
    else:
        raise RuntimeError(f"Error executing SQL for {db_id}.{table_name}.{column_name}: {result.error_message}")


def make_vector_db(db_path: str, vector_db_path: str, max_value_length: int = 100, batch_size: int = 1024, n_parallel: int = 1, lower_meta_data=True, embedding_function=None):
    """
    Make a vector database from a database path.
    """
    if Path(vector_db_path).exists():
        shutil.rmtree(vector_db_path)
        logger.info(f"Vector database already exists for {db_path}, cleaning it and making a new one...")
    
    logger.info(f"Making vector database for {db_path}, vector database path: {vector_db_path}")
    db_id = Path(db_path).stem
    client = PersistentClient(path=vector_db_path)
    collection = client.create_collection(
        name=get_collection_name(db_id),
        embedding_function=embedding_function,
        metadata={"hnsw:space": "cosine"}
    )
    
    all_column_tasks = []
    for table_name in load_table_names(db_path):
        column_names_and_types = load_column_names_and_types(db_path, table_name)
        for column_name, column_type in column_names_and_types:
            all_column_tasks.append((table_name, column_name, column_type))

    failed = False
    with ThreadPoolExecutor(max_workers=n_parallel) as executor:
        futures = []
        for table_name, column_name, column_type in all_column_tasks:
            futures.append(executor.submit(
                _process_one_column,
                db_path, table_name, column_name, column_type, 
                max_value_length, batch_size, lower_meta_data, collection, db_id
            ))
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Making vector database for {db_id}"):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Failed to process column: {e}")
                # Cancel all other pending tasks
                for f in futures:
                    f.cancel()
                failed = True
                break
                
    if failed:
        if Path(vector_db_path).exists():
            shutil.rmtree(vector_db_path)
        return False
        
    return True
