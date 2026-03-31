__all__ = ["SchemaLinkingRunner"]


def __getattr__(name):
    if name == "SchemaLinkingRunner":
        from .schema_linking import SchemaLinkingRunner
        return SchemaLinkingRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
