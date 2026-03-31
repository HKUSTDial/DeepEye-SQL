__all__ = ["SQLRevisionRunner"]


def __getattr__(name):
    if name == "SQLRevisionRunner":
        from .sql_revision import SQLRevisionRunner
        return SQLRevisionRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
