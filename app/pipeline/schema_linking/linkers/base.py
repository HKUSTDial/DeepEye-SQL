from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from typing import Dict, List, Any
from abc import ABC, abstractmethod
from app.db_utils import get_identical_schema_table_groups

class BaseSchemaLinker(ABC):

    @abstractmethod
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        pass

    def _expand_identical_schema_tables(self, result: Dict[str, List[str]], database_schema: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Expand the selection to include all tables with identical schema.
        If one table in a partitioned group is selected, all siblings should be included.
        """
        table_groups = get_identical_schema_table_groups(database_schema)
        
        if not table_groups:
            return result
        
        expanded_result = dict(result)
        initial_count = len(result)
        
        for table_name, columns in list(result.items()):
            if table_name in table_groups:
                for group_table in table_groups[table_name]:
                    if group_table not in expanded_result:
                        expanded_result[group_table] = list(columns)
                        logger.debug(f"Auto-expanded identical schema table: {group_table}")
        
        if len(expanded_result) > initial_count:
            logger.info(f"Expanded {initial_count} tables to {len(expanded_result)} tables (via identical schema groups)")
        
        return expanded_result
