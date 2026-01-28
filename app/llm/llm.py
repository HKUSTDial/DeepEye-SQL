from typing import Optional, List, Dict
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
from app.config import config, LLMConfig
from app.logger import logger


class EmptyResponseError(Exception):
    """Custom exception for empty LLM responses, used to trigger retry."""
    pass


class LLM:
    """LLM wrapper class. Each instance creates its own OpenAI client."""
    
    def __init__(self, llm_config: LLMConfig):
        self._config = llm_config
        self._client = self._create_client()
        logger.debug(f"Created LLM instance: model={llm_config.model}, temperature={llm_config.temperature}, reasoning_effort={llm_config.reasoning_effort}")
    
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
        try:
            if system_message:
                messages = [system_message] + messages
            request_params = {
                "model": self._config.model,
                "messages": messages,
                "max_tokens": self._config.max_tokens,
                "temperature": self._config.temperature,
                "timeout": timeout,
            }
            if self._config.reasoning_effort is not None:
                request_params["reasoning_effort"] = self._config.reasoning_effort
            request_params.update(kwargs)
                
            response = self._client.chat.completions.create(**request_params)
            if not response.choices:
                raise EmptyResponseError(f"No response from the model: {response}")
            
            # Check if any choice has None or empty content
            for choice in response.choices:
                if choice.message.content is None or choice.message.content.strip() == "":
                    raise EmptyResponseError(f"Model returned empty content (possibly filtered): {response}")
            
            # Calculate token usage for this specific request
            current_token_usage = {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens
            }
            
            return [choice.message for choice in response.choices], current_token_usage
        except OpenAIError as e:
            if isinstance(e, RateLimitError):
                logger.error(f"OpenAI error: {e}")
                logger.error("Rate limit exceeded, please try again later.")
            elif isinstance(e, AuthenticationError):
                logger.error(f"OpenAI error: {e}")
                logger.error("Authentication error, please check your api key.")
            elif isinstance(e, BadRequestError):
                logger.error(f"OpenAI error: {e}")
                logger.error("Bad request, please check your request parameters.")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise e