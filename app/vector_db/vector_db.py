from chromadb import PersistentClient
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction, OpenAIEmbeddingFunction
from pathlib import Path
import shutil
from typing import List, Dict, Any
from .qwen_embedding_function import QwenEmbeddingFunction
from .local_index import get_local_index_path, write_local_index_column, write_local_index_manifest
from app.db_utils import load_table_names, load_column_names_and_types, execute_sql_without_cache
from app.logger import logger
from app.progress import log_progress
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


def _is_text_column_type(column_type: str) -> bool:
    normalized_type = column_type.upper()
    return normalized_type == "TEXT" or normalized_type.startswith("VARCHAR") or normalized_type.startswith("CHAR")


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
    api_key: str = None,
    embedding_device: str = "auto",
):
    if api_type == "local":
        resolved_device = _resolve_local_embedding_device(embedding_device)
        if use_qwen3_embedding:
            logger.info(f"Using Qwen3 embedding function for {model_name_or_path} on {resolved_device}")
            return QwenEmbeddingFunction(
                model_name=model_name_or_path,
                device=resolved_device,
                trust_remote_code=True,
                local_files_only=local_files_only,
                normalize_embeddings=normalize_embeddings
            )
        else:
            logger.info(f"Using SentenceTransformer embedding function for {model_name_or_path} on {resolved_device}")
            return SentenceTransformerEmbeddingFunction(
                model_name=model_name_or_path,
                device=resolved_device,
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


def _resolve_local_embedding_device(embedding_device: str) -> str:
    normalized_device = (embedding_device or "auto").lower()
    if normalized_device == "auto":
        try:
            import torch

            return "cuda" if torch.cuda.is_available() else "cpu"
        except Exception as exc:
            logger.warning(f"Could not inspect CUDA availability ({exc}); falling back to CPU")
            return "cpu"

    if normalized_device.startswith("cuda"):
        try:
            import torch

            if not torch.cuda.is_available():
                logger.warning(f"Requested embedding_device={embedding_device}, but CUDA is unavailable; falling back to CPU")
                return "cpu"
        except Exception as exc:
            logger.warning(f"Could not inspect CUDA availability ({exc}); falling back to CPU")
            return "cpu"

    return embedding_device


def _process_one_column(
    db_path: str, 
    table_name: str, 
    column_name: str, 
    column_type: str, 
    max_value_length: int, 
    embedding_batch_size: int,
    lower_meta_data: bool, 
    collection: Any | None, 
    db_id: str,
    embedding_function: Any,
    local_index_path: str | Path | None,
):
    if not _is_text_column_type(column_type):
        return None
    
    query_sql = f"""
    SELECT DISTINCT `{column_name}` FROM `{table_name}` 
    WHERE `{column_name}` IS NOT NULL 
    AND LENGTH(CAST(`{column_name}` AS TEXT)) <= {max_value_length};
    """
    # Keep vector DB scans bounded so large/slow SQLite tables do not stall the pipeline indefinitely.
    # These full-column scans are one-shot ingestion work; bypass the shared SQL cache
    # so large result sets do not evict more valuable execution entries.
    result = execute_sql_without_cache(db_path, query_sql, timeout=600)
    if result.result_type in ["success", "empty_result"]:
        value_examples = [str(row[0]) for row in result.result_rows]
        
        if len(value_examples) == 0:
            return None
        
        if _is_uuid_column(value_examples) or _is_number_column(value_examples):
            return None

        stored_documents = []
        stored_embeddings = []
        stored_table_name = table_name.lower() if lower_meta_data else table_name
        stored_column_name = column_name.lower() if lower_meta_data else column_name
        
        # Process in batches to stay under embedding endpoint/model and ChromaDB limits.
        for i in range(0, len(value_examples), embedding_batch_size):
            batch_examples = value_examples[i:i + embedding_batch_size]
            batch_embeddings = embedding_function(batch_examples)
            if collection is not None:
                collection.add(
                    ids=[str(uuid.uuid4()) for _ in range(len(batch_examples))],
                    documents=batch_examples,
                    embeddings=batch_embeddings,
                    metadatas=[
                        {"db_id": db_id.lower(), "table_name": table_name.lower(), "column_name": column_name.lower()} 
                        if lower_meta_data else {"db_id": db_id, "table_name": table_name, "column_name": column_name}
                        for _ in range(len(batch_examples))
                    ],
                )
            if local_index_path is not None:
                stored_documents.extend(batch_examples)
                stored_embeddings.extend(batch_embeddings)

        if local_index_path is not None:
            return write_local_index_column(
                local_index_path=local_index_path,
                table_name=stored_table_name,
                column_name=stored_column_name,
                documents=stored_documents,
                embeddings=stored_embeddings,
            )
        return None
    else:
        raise RuntimeError(f"Error executing SQL for {db_id}.{table_name}.{column_name}: {result.error_message}")


def _process_one_column_with_limit(
    work_semaphore: Any | None,
    *args,
):
    if work_semaphore is None:
        return _process_one_column(*args)
    with work_semaphore:
        return _process_one_column(*args)


def make_vector_db(
    db_path: str,
    vector_db_path: str,
    max_value_length: int = 100,
    embedding_batch_size: int = 128,
    parallelism: int = 1,
    lower_meta_data=True,
    embedding_function=None,
    build_backend: str = "both",
    work_semaphore: Any | None = None,
    progress_log_interval: int = 50,
):
    """
    Make a vector database from a database path.
    """
    if Path(vector_db_path).exists():
        shutil.rmtree(vector_db_path)
        logger.info(f"Vector database already exists for {db_path}, cleaning it and making a new one...")
    
    logger.info(f"Making vector database for {db_path}, vector database path: {vector_db_path}")
    db_id = Path(db_path).stem
    vector_db_path = Path(vector_db_path)
    vector_db_path.mkdir(parents=True, exist_ok=True)

    build_chroma = build_backend in {"chroma", "both"}
    build_local_index = build_backend in {"local_index", "both"}
    if not build_chroma and not build_local_index:
        raise ValueError(f"Unsupported build_backend: {build_backend}")

    collection = None
    if build_chroma:
        client = PersistentClient(path=vector_db_path)
        collection = client.create_collection(
            name=get_collection_name(db_id),
            embedding_function=embedding_function,
            metadata={"hnsw:space": "cosine"}
        )
    local_index_path = get_local_index_path(vector_db_path) if build_local_index else None
    
    all_column_tasks = []
    for table_name in load_table_names(db_path):
        column_names_and_types = load_column_names_and_types(db_path, table_name)
        for column_name, column_type in column_names_and_types:
            if _is_text_column_type(column_type):
                all_column_tasks.append((table_name, column_name, column_type))

    if len(all_column_tasks) == 0:
        logger.info(f"No text columns found for {db_id}, leaving empty vector database")
        if build_local_index:
            write_local_index_manifest(get_local_index_path(vector_db_path), [])
        return True

    max_workers = min(len(all_column_tasks), max(1, parallelism))
    logger.info(f"Processing {len(all_column_tasks)} text columns for {db_id} with {max_workers} worker(s)")

    failed = False
    local_index_entries = []
    completed_columns = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_column = {}
        for table_name, column_name, column_type in all_column_tasks:
            future = executor.submit(
                _process_one_column_with_limit,
                work_semaphore,
                db_path, table_name, column_name, column_type,
                max_value_length, embedding_batch_size, lower_meta_data, collection, db_id,
                embedding_function, local_index_path,
            )
            future_to_column[future] = (table_name, column_name)
        
        for future in as_completed(future_to_column):
            table_name, column_name = future_to_column[future]
            try:
                local_index_entry = future.result()
                if local_index_entry is not None:
                    local_index_entries.append(local_index_entry)
                previous_completed_columns = completed_columns
                completed_columns += 1
                log_progress(
                    f"Vector DB {db_id} text columns",
                    completed_columns,
                    len(all_column_tasks),
                    progress_log_interval,
                    previous_completed=previous_completed_columns,
                )
            except Exception as e:
                logger.exception(f"Failed to process column {db_id}.{table_name}.{column_name}: {e}")
                # Cancel all other pending tasks
                for f in future_to_column:
                    f.cancel()
                failed = True
                break
                
    if failed:
        if Path(vector_db_path).exists():
            shutil.rmtree(vector_db_path)
        return False

    if build_local_index:
        write_local_index_manifest(local_index_path, local_index_entries)
        
    return True
