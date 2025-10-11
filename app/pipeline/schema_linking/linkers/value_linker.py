from .base import BaseSchemaLinker
from app.dataset import DataItem
from app.llm import LLM
from app.config import config
from app.db_utils import map_lower_table_name_to_original_table_name, map_lower_column_name_to_original_column_name
from typing import Dict, List
from collections import defaultdict


class ValueLinker(BaseSchemaLinker):
    
    _value_distance_threshold: float = config.schema_linking_config.value_distance_threshold
    
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        linked_tables_and_columns = defaultdict(list)
        for table_name, columns in data_item.retrieved_values.items():
            table_name = map_lower_table_name_to_original_table_name(table_name, data_item.database_schema)
            if table_name is None:
                continue
            for column_name, values in columns.items():
                if any(value["distance"] < self._value_distance_threshold for value in values):
                    column_name = map_lower_column_name_to_original_column_name(table_name, column_name, data_item.database_schema)
                    if column_name is None:
                        continue
                    linked_tables_and_columns[table_name].append(column_name)
        return dict(linked_tables_and_columns), {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}