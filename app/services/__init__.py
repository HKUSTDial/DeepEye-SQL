__all__ = [
    "ArtifactStore",
    "STAGE_ARTIFACT_FIELDS",
    "load_stage_dataset",
    "ExecutionService",
    "configure_execution_service",
    "get_execution_service",
    "reset_execution_service",
    "SchemaService",
    "configure_schema_service",
    "get_schema_service",
    "reset_schema_service",
]


def __getattr__(name):
    if name in {"ArtifactStore", "STAGE_ARTIFACT_FIELDS", "load_stage_dataset"}:
        from .artifact_store import ArtifactStore, STAGE_ARTIFACT_FIELDS, load_stage_dataset

        return {
            "ArtifactStore": ArtifactStore,
            "STAGE_ARTIFACT_FIELDS": STAGE_ARTIFACT_FIELDS,
            "load_stage_dataset": load_stage_dataset,
        }[name]

    if name in {"ExecutionService", "configure_execution_service", "get_execution_service", "reset_execution_service"}:
        from .execution_service import ExecutionService, configure_execution_service, get_execution_service, reset_execution_service

        return {
            "ExecutionService": ExecutionService,
            "configure_execution_service": configure_execution_service,
            "get_execution_service": get_execution_service,
            "reset_execution_service": reset_execution_service,
        }[name]

    if name in {"SchemaService", "configure_schema_service", "get_schema_service", "reset_schema_service"}:
        from .schema_service import SchemaService, configure_schema_service, get_schema_service, reset_schema_service

        return {
            "SchemaService": SchemaService,
            "configure_schema_service": configure_schema_service,
            "get_schema_service": get_schema_service,
            "reset_schema_service": reset_schema_service,
        }[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
