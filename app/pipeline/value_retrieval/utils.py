from typing import List
from app.llm import LLM
from chromadb.types import Collection
from typing import Dict, Any
from app.prompt import PromptFactory
import re
import json
from app.logger import logger


def extract_keywords(question: str, evidence: str, llm: LLM, max_retry: int = 5) -> tuple[List[str], Dict[str, int]]:
    prompt = PromptFactory.format_keywords_extraction_prompt(question, evidence)
    retry = 0
    keywords_list = None
    total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    
    while retry < max_retry:
        try:
            response, token_usage = llm.ask([{"role": "user", "content": prompt}], n=1, stop=["</result>"])
            total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
            total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
            total_token_usage["total_tokens"] += token_usage["total_tokens"]
            
            # restore the stop token: </result>
            content = response[0].content + "</result>"
            
            raw_list = re.search(r"<result>(.*?)</result>", content, re.DOTALL).group(1)
            keywords_list = json.loads(raw_list)
            if isinstance(keywords_list, list):
                break
        except Exception as e:
            retry += 1
            logger.error(f"Error extracting keywords: {e}")
    
    # post process the keywords_list
    processed_keywords = set()
    for keyword in keywords_list:
        keyword: str = keyword.strip()
        processed_keywords.add(keyword)
        processed_keywords.update(keyword.split(" "))
    keywords_list = list(processed_keywords)
    
    return keywords_list, total_token_usage


def retrieve_values_for_one_column(
    keywords: List[str],
    collection: Collection,
    table_name: str,
    column_name: str,
    n_results: int,
    lower_meta_data: bool
) -> Dict[str, Any]:
    table_name = table_name.lower() if lower_meta_data else table_name
    column_name = column_name.lower() if lower_meta_data else column_name
    query_results = collection.query(
        query_texts=keywords,
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