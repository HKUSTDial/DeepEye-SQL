__all__ = ["SQLSelectionRunner"]


def __getattr__(name):
    if name == "SQLSelectionRunner":
        from .sql_selection import SQLSelectionRunner
        return SQLSelectionRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
