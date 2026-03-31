__all__ = ["SQLGenerationRunner"]


def __getattr__(name):
    if name == "SQLGenerationRunner":
        from .sql_generation import SQLGenerationRunner
        return SQLGenerationRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
