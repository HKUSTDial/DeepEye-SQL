__all__ = ["LLMExtractor"]


def __getattr__(name):
    if name == "LLMExtractor":
        from .extractor import LLMExtractor
        return LLMExtractor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
