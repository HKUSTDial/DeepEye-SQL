"""
LLM Extractor Module

This module provides a robust extraction mechanism for LLM responses.
It uses rule-based parsing with configurable retry logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, TypeVar
from app.logger import logger

if TYPE_CHECKING:
    from app.llm import LLM

T = TypeVar('T')
DEFAULT_LLM_EXTRACTOR_MAX_RETRY = 3


class LLMExtractor:
    """
    A reusable LLM extractor that provides extraction for LLM responses.
    
    Usage:
        extractor = LLMExtractor()
        
        # Extract with rule parser and retry logic
        results, token_usage = extractor.extract_with_retry(
            llm=llm,
            messages=[...],
            rule_parser=my_parser_function,
            parser_kwargs={"database_schema": schema},
            n=5
        )
    """
    
    def __init__(self, max_retry: int = DEFAULT_LLM_EXTRACTOR_MAX_RETRY):
        self._max_retry = max_retry
    
    @property
    def max_retry(self) -> int:
        """Get the maximum retry attempts."""
        return self._max_retry
    
    def extract_with_retry(
        self,
        llm: LLM,
        messages: list,
        rule_parser: Callable[..., Optional[T]],
        parser_kwargs: Optional[Dict[str, Any]] = None,
        fix_end_token: bool = False,
        end_token: str = "</result>",
        n: int = 1,
        max_retry: Optional[int] = None,
        **llm_kwargs
    ) -> Tuple[list, Dict[str, int]]:
        """
        Call LLM and parse responses with retry logic.
        
        This method will:
        1. Call the LLM to get responses
        2. For each response, try rule-based parsing
        3. Retry up to max_retry times if not enough valid results
        
        Args:
            llm: The LLM to call for generating responses
            messages: The messages to send to the LLM
            rule_parser: A callable that attempts to parse the response using rules
            parser_kwargs: Additional keyword arguments to pass to the rule_parser
            fix_end_token: Whether to fix missing end tokens
            end_token: The end token to append if missing
            n: Target number of successfully parsed results
            **llm_kwargs: Additional keyword arguments to pass to the LLM
        
        Returns:
            Tuple of (list of parsed results, total token_usage_dict)
        """
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        all_results = []
        parser_kwargs = parser_kwargs or {}
        retry_count = 0
        retry_limit = self.max_retry if max_retry is None else max_retry
        
        while len(all_results) < n and retry_count < retry_limit:
            remaining = n - len(all_results)
            
            try:
                responses, token_usage = llm.ask(messages, n=remaining, **llm_kwargs)
                total_token_usage["prompt_tokens"] += token_usage["prompt_tokens"]
                total_token_usage["completion_tokens"] += token_usage["completion_tokens"]
                total_token_usage["total_tokens"] += token_usage["total_tokens"]
                
                for response in responses:
                    content = response.content.strip()
                    
                    # Fix end token if needed
                    if fix_end_token and not content.endswith(end_token):
                        content += end_token
                    
                    # Try rule-based parsing
                    try:
                        if not content or not content.strip():
                            logger.warning("LLM returned empty content for response")
                            continue

                        result = rule_parser(content, **parser_kwargs)
                        # Check if result is truly valid (not None and not empty if it's a collection)
                        is_valid = result is not None
                        if is_valid and isinstance(result, (dict, list, str)):
                            if not result: # Handle empty dict/list/string
                                is_valid = False
                                
                        if is_valid:
                            all_results.append(result)
                            logger.debug(f"Rule parsing succeeded")
                        else:
                            logger.warning(f"Rule parsing returned invalid or empty result")
                            logger.debug(f"Response content: {content}")
                    except Exception as e:
                        logger.warning(f"Rule parsing failed with exception: {e}")
                        logger.debug(f"Response content: {content}")
                        
            except Exception as e:
                logger.warning(f"Error during LLM call (retry {retry_count + 1}/{retry_limit}): {e}")
            
            retry_count += 1
        
        if len(all_results) < n:
            logger.warning(f"Only got {len(all_results)}/{n} valid results after {retry_limit} retries")
        
        return all_results, total_token_usage


def get_extractor(max_retry: int = DEFAULT_LLM_EXTRACTOR_MAX_RETRY) -> LLMExtractor:
    """Create an extractor with the requested retry budget."""
    return LLMExtractor(max_retry=max_retry)
