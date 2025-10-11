import re
from typing import Optional, List, Dict
from tenacity import(
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential
)
import threading
from openai import (
    OpenAI,
    AzureOpenAI,
    OpenAIError,
    AuthenticationError,
    RateLimitError,
    BadRequestError
)
from openai.types.chat import ChatCompletionMessage
from app.config import config, LLMConfig
from app.logger import logger


class LLM:
    _client: OpenAI | AzureOpenAI = None
    _config: LLMConfig = None
    _instances: Dict[str, "LLM"] = {}
    _lock = threading.Lock()
    
    def __new__(cls, llm_config: LLMConfig):
        if llm_config.model not in cls._instances:
            with cls._lock:
                if llm_config.model not in cls._instances:
                    instance = super().__new__(cls)
                    instance.__init__(llm_config)
                    cls._instances[llm_config.model] = instance
        return cls._instances[llm_config.model]
    
    def __init__(self, llm_config: LLMConfig):
        self._config = llm_config
        self._client = self._create_client()
    
    def _create_client(self):
        if self._config.api_type == "openai":
            return OpenAI(api_key=self._config.api_key, base_url=self._config.base_url)
        elif self._config.api_type == "azure":
            return AzureOpenAI(api_key=self._config.api_key, base_url=self._config.base_url, api_version=self._config.api_version)
        else:
            raise ValueError(f"Unsupported api type: {self._config.api_type}")
    
    @staticmethod
    def get_instances():
        return LLM._instances
    
        
    @retry(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(6),
        retry=retry_if_exception_type(RateLimitError)
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
            request_params.update(kwargs)
                
            response = self._client.chat.completions.create(**request_params)
            if not response.choices:
                raise OpenAIError(f"No response from the model: {response}")
            
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