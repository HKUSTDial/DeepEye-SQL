__all__ = ["LLM"]


def __getattr__(name):
    if name == "LLM":
        from .llm import LLM
        return LLM
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
