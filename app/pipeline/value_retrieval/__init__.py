__all__ = ["ValueRetrievalRunner"]


def __getattr__(name):
    if name == "ValueRetrievalRunner":
        from .value_retrieval import ValueRetrievalRunner
        return ValueRetrievalRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
