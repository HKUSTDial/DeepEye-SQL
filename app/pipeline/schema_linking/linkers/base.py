from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from typing import Dict, List
from abc import ABC, abstractmethod

class BaseSchemaLinker(ABC):

    @abstractmethod
    def link(self, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> tuple[Dict[str, List[str]], Dict[str, int]]:
        pass
