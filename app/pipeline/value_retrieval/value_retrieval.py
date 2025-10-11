from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import extract_keywords, retrieve_values_for_one_column
from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.config import config, ValueRetrievalConfig, LLMConfig
from app.llm import LLM
from app.vector_db import get_embedding_function
from app.db_utils import map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from chromadb.api import ClientAPI
from chromadb import PersistentClient
from chromadb.types import Collection
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
import time
from collections import defaultdict
from app.logger import logger


class ValueRetrievalRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _vector_db_client_dict: Dict[str, ClientAPI] = {}
    _vector_db_collection_dict: Dict[str, Collection] = {}
    _thread_pool_executor: ThreadPoolExecutor = None
    
    def __init__(self):
        self._llm = LLM(config.value_retrieval_config.llm)
        self._dataset = load_dataset(config.dataset_config.save_path)
        self._initialize_vector_db_client_and_collection()
    
    def _initialize_vector_db_client_and_collection(self):
        for db_id in self._dataset.get_all_database_ids():
            vector_db_path = Path(config.vector_database_config.store_root_path) / db_id
            self._vector_db_client_dict[db_id] = PersistentClient(path=vector_db_path)
            self._vector_db_collection_dict[db_id] = self._vector_db_client_dict[db_id].get_collection(
                name=db_id,
                embedding_function=get_embedding_function(
                    model_name_or_path=config.vector_database_config.embedding_model_name_or_path,
                    use_qwen3_embedding=config.vector_database_config.use_qwen3_embedding,
                    local_files_only=config.vector_database_config.local_files_only,
                    normalize_embeddings=config.vector_database_config.normalize_embeddings,
                    base_url=config.vector_database_config.base_url,
                    api_key=config.vector_database_config.api_key,
                )
            )
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=config.value_retrieval_config.n_parallel)
    
    def _extract_keywords(self, data_item: DataItem) -> tuple[List[str], Dict[str, int]]:
        return extract_keywords(data_item.question, data_item.evidence, self._llm)
    
    def _retrieve_values(self, data_item: DataItem):
        keywords, token_usage = self._extract_keywords(data_item)
        data_item.question_keywords = keywords
        data_item.value_retrieval_llm_cost = token_usage
        args_list = []
        table_names = data_item.database_schema["tables"].keys()
        for table_name in table_names:
            columns = data_item.database_schema["tables"][table_name]["columns"].items()
            for column_name, column_dict in columns:
                column_type = column_dict["column_type"]
                if column_type.upper() == "TEXT" or column_type.upper().startswith("VARCHAR") or column_type.upper().startswith("CHAR"):
                    args_list.append((
                        keywords,
                        self._vector_db_collection_dict[data_item.database_id],
                        table_name,
                        column_name,
                        config.value_retrieval_config.n_results,
                        config.vector_database_config.lower_meta_data
                    ))

        futures = []
        for args in args_list:
            future = self._thread_pool_executor.submit(retrieve_values_for_one_column, *args)
            futures.append(future)
        
        data_item.retrieved_values = defaultdict(dict)
        for future in as_completed(futures):
            result = future.result()
            original_table_name = map_lower_table_name_to_original_table_name(result["table_name"], data_item.database_schema)
            original_column_name = map_lower_column_name_to_original_column_name(result["table_name"], result["column_name"], data_item.database_schema)
            data_item.retrieved_values[original_table_name][original_column_name] = result["values"]
        data_item.retrieved_values = dict(data_item.retrieved_values)
        
        self._update_database_schema(data_item)
    
    def _update_database_schema(self, data_item: DataItem):
        database_schema_after_value_retrieval = copy.deepcopy(data_item.database_schema)
        for table_name, column_dict in data_item.retrieved_values.items():
            for column_name, values in column_dict.items():
                original_values = data_item.database_schema["tables"][table_name]["columns"][column_name]["value_examples"]
                new_values = [value["value"] for value in values] + original_values
                new_values = new_values[:config.value_retrieval_config.n_results]
                database_schema_after_value_retrieval["tables"][table_name]["columns"][column_name]["value_examples"] = new_values
        data_item.database_schema_after_value_retrieval = database_schema_after_value_retrieval
    
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._vector_db_client_dict = {}
        self._vector_db_collection_dict = {}
    
    def save_result(self):
        save_dataset(self._dataset, config.value_retrieval_config.save_path)
    
    def run(self):
        for idx, data_item in tqdm(enumerate(self._dataset, start=1), desc="Value Retrieval", total=len(self._dataset)):
            data_item: DataItem
            start_time = time.time()
            self._retrieve_values(data_item)
            end_time = time.time()
            data_item.value_retrieval_time = end_time - start_time
            data_item.total_time = data_item.value_retrieval_time
            data_item.total_llm_cost = data_item.value_retrieval_llm_cost
            
            if idx % 20 == 0:
                logger.info(f"Value Retrieval {idx} / {len(self._dataset)} completed")
                self.save_result()
            
        self.save_result()
        self._clean_up()