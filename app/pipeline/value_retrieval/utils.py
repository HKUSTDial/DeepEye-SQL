from typing import Any, Dict, List, Optional
from app.llm import LLM
from chromadb.types import Collection
from app.prompt import PromptFactory
from app.llm_extractor import LLMExtractor
import ast
import re
import json
from app.logger import logger
from tenacity import(
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential
)
from openai import RateLimitError, APITimeoutError


MAX_KEYWORDS_PER_ITEM = 8
KEYWORD_EXTRACTION_MAX_TOKENS = 256
KEYWORD_EXTRACTION_TEMPERATURE = 0.0
LOW_VALUE_SINGLE_TOKEN_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "between", "by", "did", "do", "does",
    "each", "for", "from", "how", "in", "into", "is", "it", "many", "most", "name",
    "number", "of", "on", "or", "per", "show", "than", "that", "the", "their", "to",
    "was", "what", "which", "who", "with", "year",
}


def _normalize_keyword(keyword: str) -> str:
    return re.sub(r"\s+", " ", keyword).strip().strip("\"'")


def _is_informative_keyword(keyword: str) -> bool:
    if not keyword:
        return False

    if len(keyword) == 1 and not keyword.isdigit():
        return False

    if " " in keyword:
        return True

    lowered = keyword.lower()
    if lowered in LOW_VALUE_SINGLE_TOKEN_STOPWORDS:
        return False

    if any(char.isdigit() for char in keyword):
        return True

    return len(keyword) >= 3


def _post_process_keywords(keywords_list: List[str]) -> List[str]:
    deduped_keywords: List[str] = []
    seen_keywords = set()

    for keyword in keywords_list:
        normalized_keyword = _normalize_keyword(str(keyword))
        if not _is_informative_keyword(normalized_keyword):
            continue

        normalized_key = normalized_keyword.casefold()
        if normalized_key in seen_keywords:
            continue

        seen_keywords.add(normalized_key)
        deduped_keywords.append(normalized_keyword)

        if len(deduped_keywords) >= MAX_KEYWORDS_PER_ITEM * 2:
            break

    phrase_token_sets = []
    for keyword in deduped_keywords:
        if " " in keyword:
            phrase_token_sets.append({token.casefold() for token in keyword.split()})

    processed_keywords: List[str] = []
    for keyword in deduped_keywords:
        if " " not in keyword:
            lowered_keyword = keyword.casefold()
            if any(lowered_keyword in token_set for token_set in phrase_token_sets):
                continue
        processed_keywords.append(keyword)
        if len(processed_keywords) >= MAX_KEYWORDS_PER_ITEM:
            break

    return processed_keywords


def _fallback_extract_keywords(question: str, evidence: str) -> List[str]:
    fallback_keywords: List[str] = []
    seen_keywords = set()
    text = f"{question}\n{evidence}".strip()

    def add_keyword(candidate: str) -> None:
        normalized_keyword = _normalize_keyword(candidate)
        if not _is_informative_keyword(normalized_keyword):
            return
        normalized_key = normalized_keyword.casefold()
        if normalized_key in seen_keywords:
            return
        seen_keywords.add(normalized_key)
        fallback_keywords.append(normalized_keyword)

    for match in re.finditer(r"'([^']+)'|\"([^\"]+)\"", text):
        add_keyword(next(group for group in match.groups() if group))

    for match in re.finditer(r"\b\d{4}\b|\b\d+(?:\.\d+)?%?\b", text):
        add_keyword(match.group(0))

    for match in re.finditer(r"\b[A-Z][A-Za-z0-9&'.-]*(?:\s+[A-Z][A-Za-z0-9&'.-]*)*\b", text):
        add_keyword(match.group(0))

    for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9&'.-]*", text):
        add_keyword(token)
        if len(fallback_keywords) >= MAX_KEYWORDS_PER_ITEM:
            break

    return fallback_keywords[:MAX_KEYWORDS_PER_ITEM]

def _parse_keywords_response(response: str) -> Optional[List[str]]:
    """Parse keywords from LLM response."""
    try:
        match = re.search(r"<result>(.*?)</result>", response, re.DOTALL)
        if match is None:
            return None

        raw_list = match.group(1).strip()
        try:
            keywords_list = json.loads(raw_list)
        except json.JSONDecodeError:
            keywords_list = ast.literal_eval(raw_list)

        if isinstance(keywords_list, list):
            return [str(keyword) for keyword in keywords_list]
        return None
    except Exception as e:
        logger.debug(f"Error parsing keywords: {e}")
        return None


def extract_keywords(
    question: str,
    evidence: str,
    llm: LLM,
    fix_end_token: bool = False,
    extractor_max_retry: Optional[int] = None,
    extractor: Optional[LLMExtractor] = None,
) -> tuple[List[str], Dict[str, int]]:
    prompt = PromptFactory.format_keywords_extraction_prompt(question, evidence)
    
    if extractor is None:
        extractor = LLMExtractor() if extractor_max_retry is None else LLMExtractor(max_retry=extractor_max_retry)
    results, total_token_usage = extractor.extract_with_retry(
        llm=llm,
        messages=[{"role": "user", "content": prompt}],
        rule_parser=_parse_keywords_response,
        fix_end_token=fix_end_token,
        end_token="</result>",
        n=1,
        max_tokens=KEYWORD_EXTRACTION_MAX_TOKENS,
        temperature=KEYWORD_EXTRACTION_TEMPERATURE,
    )
    
    if results:
        keywords_list = _post_process_keywords(results[0])
    else:
        logger.warning("Failed to extract keywords from LLM response, using compact fallback extraction")
        keywords_list = _post_process_keywords(_fallback_extract_keywords(question, evidence))
    
    return keywords_list, total_token_usage


@retry(
    wait=wait_random_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((RateLimitError, APITimeoutError))
)
def embed_keywords(keywords: List[str], embedding_function: Any, batch_size: int) -> List[List[float]]:
    """
    Independently embed keywords with batching and retry logic.
    """
    if not keywords:
        return []

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

    if not query_embeddings:
        return {
            "table_name": table_name,
            "column_name": column_name,
            "values": [],
        }
    
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
