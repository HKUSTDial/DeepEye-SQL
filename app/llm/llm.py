from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Dict, List, Optional
from tenacity import(
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential
)
from openai import (
    OpenAI,
    AzureOpenAI,
    OpenAIError,
    AuthenticationError,
    RateLimitError,
    BadRequestError,
    APITimeoutError,
    APIConnectionError,
    InternalServerError
)
from openai.types.chat import ChatCompletionMessage
from app.logger import logger

if TYPE_CHECKING:
    from app.config.config import LLMConfig


class EmptyResponseError(Exception):
    """Custom exception for empty LLM responses, used to trigger retry."""
    pass


class LLM:
    """LLM wrapper class. Each instance lazily creates its own OpenAI client."""
    
    def __init__(self, llm_config: LLMConfig):
        self._config = llm_config
        self._client = None
        self._client_lock = threading.Lock()
        logger.debug(
            f"Initialized LLM wrapper: model={llm_config.model}, "
            f"temperature={llm_config.temperature}, reasoning_effort={llm_config.reasoning_effort}"
        )
    
    @property
    def llm_config(self) -> LLMConfig:
        """Get the LLM configuration."""
        return self._config
    
    def _create_client(self):
        if self._config.api_type == "openai":
            return OpenAI(api_key=self._config.api_key, base_url=self._config.base_url)
        elif self._config.api_type == "azure":
            return AzureOpenAI(api_key=self._config.api_key, base_url=self._config.base_url, api_version=self._config.api_version)
        else:
            raise ValueError(f"Unsupported api type: {self._config.api_type}")

    def _get_client(self):
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = self._create_client()
                    logger.debug(f"Created LLM client for model={self._config.model}")
        return self._client
        
    @retry(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(15),
        # Retry on recoverable errors (including BadRequestError for provider-specific issues like "user location not supported")
        # NOT on AuthenticationError (wrong API key won't fix itself)
        retry=retry_if_exception_type((RateLimitError, APITimeoutError, APIConnectionError, InternalServerError, BadRequestError, EmptyResponseError))
    )
    def ask(self, messages: List[Dict[str, str]],
                  system_message: Optional[Dict[str, str]] = None,
                  timeout: int = 300,
                  **kwargs) -> tuple[List[ChatCompletionMessage], Dict[str, int]]:
        if system_message:
            messages = [system_message] + messages
            
        target_n = kwargs.pop("n", 1)
        max_request_n = self._config.max_request_n or target_n
        
        all_choices = []
        total_token_usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
        
        current_max_tokens = kwargs.pop("max_tokens", self._config.max_tokens)
        
        while len(all_choices) < target_n:
            current_n = min(target_n - len(all_choices), max_request_n)
            try:
                request_params = {
                    "model": self._config.model,
                    "messages": messages,
                    "max_tokens": current_max_tokens,
                    "temperature": self._config.temperature,
                    "timeout": timeout,
                    "n": current_n,
                }
                if self._config.reasoning_effort is not None:
                    request_params["reasoning_effort"] = self._config.reasoning_effort
                request_params.update(kwargs)
                    
                response = self._get_client().chat.completions.create(**request_params)
                if not response.choices:
                    raise EmptyResponseError(f"No response from the model: {response}")
                
                # Check if any choice has None or empty content
                for choice in response.choices:
                    if choice.message.content is None or choice.message.content.strip() == "":
                        raise EmptyResponseError(f"Model returned empty content (possibly filtered): {response}")
                
                all_choices.extend([choice.message for choice in response.choices])
                
                # Calculate token usage
                if response.usage:
                    total_token_usage["prompt_tokens"] += response.usage.prompt_tokens
                    total_token_usage["completion_tokens"] += response.usage.completion_tokens
                    total_token_usage["total_tokens"] += response.usage.total_tokens
                
            except OpenAIError as e:
                if isinstance(e, RateLimitError):
                    logger.error(f"OpenAI error: {e}")
                    logger.error("Rate limit exceeded, please try again later.")
                elif isinstance(e, AuthenticationError):
                    logger.error(f"OpenAI error: {e}")
                    logger.error("Authentication error, please check your api key.")
                elif isinstance(e, BadRequestError):
                    error_msg = str(e).lower()
                    if "context" in error_msg or "tokens" in error_msg or "length" in error_msg:
                        new_max_tokens = int(current_max_tokens * 0.9) if current_max_tokens else 0
                        if new_max_tokens > 0:
                            logger.warning(f"Context length exceeded. Reducing max_tokens from {current_max_tokens} to {new_max_tokens} and retrying.")
                            current_max_tokens = new_max_tokens
                            continue
                        else:
                            logger.error(f"Context length exceeded, but max_tokens cannot be reduced further (current: {current_max_tokens}).")
                    
                    logger.error(f"OpenAI error: {e}")
                    logger.error("Bad request, please check your request parameters.")
                raise e
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise e
                
        return all_choices, total_token_usage
