from typing import List, Optional
from app.llm import LLM
from chromadb.types import Collection
from typing import Dict, Any
from app.prompt import PromptFactory
from app.llm_extractor import LLMExtractor
import re
import json
from app.logger import logger
from app.config import config
import time
import threading
from tenacity import(
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential
)
from openai import RateLimitError, APITimeoutError
from tqdm import tqdm


def _parse_keywords_response(response: str) -> Optional[List[str]]:
    """Parse keywords from LLM response."""
    try:
        raw_list = re.search(r"<result>(.*?)</result>", response, re.DOTALL).group(1)
        keywords_list = json.loads(raw_list)
        if isinstance(keywords_list, list):
            return keywords_list
        return None
    except Exception as e:
        logger.debug(f"Error parsing keywords: {e}")
        return None


def extract_keywords(question: str, evidence: str, llm: LLM, max_retry: int = 10) -> tuple[List[str], Dict[str, int]]:
    prompt = PromptFactory.format_keywords_extraction_prompt(question, evidence)
    
    extractor = LLMExtractor()
    results, total_token_usage = extractor.extract_with_retry(
        llm=llm,
        messages=[{"role": "user", "content": prompt}],
        rule_parser=_parse_keywords_response,
        fix_end_token=config.value_retrieval_config.llm.fix_end_token,
        end_token="</result>",
        n=1
    )
    
    if results:
        keywords_list = results[0]
    else:
        logger.warning("Failed to extract keywords from LLM response, using default keywords splitting strategy")
        keywords_list = question.split(" ") + evidence.split(" ")
        
    # post process the keywords_list
    processed_keywords = set()
    for keyword in keywords_list:
        keyword: str = keyword.strip()
        processed_keywords.add(keyword)
        processed_keywords.update(keyword.split(" "))
    keywords_list = list(processed_keywords)
    
    return keywords_list, total_token_usage


@retry(
    wait=wait_random_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((RateLimitError, APITimeoutError))
)
def embed_keywords(keywords: List[str], embedding_function: Any) -> List[List[float]]:
    """
    Independently embed keywords with batching and retry logic.
    """
    if not keywords:
        return []
    
    batch_size = config.vector_database_config.batch_size
    all_embeddings = []
    
    # Manual batching to respect API limits (e.g., max 10 per request)
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i : i + batch_size]
        batch_embeddings = embedding_function(batch)
        all_embeddings.extend(batch_embeddings)
        
    return all_embeddings


def retrieve_values_for_one_column(
    query_embeddings: List[List[float]], # Changed from keywords: List[str]
    collection: Collection,
    table_name: str,
    column_name: str,
    n_results: int,
    lower_meta_data: bool
) -> Dict[str, Any]:
    table_name = table_name.lower() if lower_meta_data else table_name
    column_name = column_name.lower() if lower_meta_data else column_name
    
    # We no longer need batching here because we already have the embeddings
    query_results = collection.query(
        query_embeddings=query_embeddings, # Pass pre-computed embeddings
        where={"$and": [{"table_name": {"$eq": table_name}}, {"column_name": {"$eq": column_name}}]},
        n_results=n_results,
    )
    
    values = []
    for documents, distances in zip(query_results["documents"], query_results["distances"]):
        for doc, dist in zip(documents, distances):
            values.append((doc, dist))
    seen_values = set()
    top_k_values = []
    for value, distance in sorted(values, key=lambda x: x[1]):
        if value not in seen_values:
            seen_values.add(value)
            top_k_values.append({"value": value, "distance": distance})
            if len(top_k_values) >= n_results:
                break
    
    return {
        "table_name": table_name,
        "column_name": column_name,
        "values": top_k_values,
    }