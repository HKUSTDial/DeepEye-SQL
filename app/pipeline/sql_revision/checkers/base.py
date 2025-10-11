from abc import ABC, abstractmethod
from app.dataset import DataItem
from app.llm import LLM
from app.logger import logger
from typing import Dict, List, Any, Tuple, Optional
import re


class BaseChecker(ABC):
    
    @abstractmethod
    def check_and_revise(self, sql: str, data_item: DataItem, llm: LLM, sampling_budget: int = 1) -> Tuple[str, Dict[str, int]]:
        pass
    
    def _parse_llm_response(self, response: str) -> Optional[str]:
        # restore the stop token: </result>
        response += "</result>"
        
        # fix some common format errors
        if "</reasoning>\n[result>" in response:
            response = response.replace("</reasoning>\n[result>", "</reasoning>\n<result>")
        
        try:
            answer_match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
            if not answer_match:
                logger.warning("No <result> tag found in LLM response")
                logger.warning(f"Response content: {response}")
                return None
            answer_content = answer_match.group(1).strip()
            # strip ```sql```
            if answer_content.startswith("```sql") and answer_content.endswith("```"):
                answer_content = answer_content[len("```sql"):-len("```")].strip()
            # logger.info(f"Parsed LLM response: {answer_content}")
            return answer_content
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            logger.debug(f"Response content: {response}")
            return None