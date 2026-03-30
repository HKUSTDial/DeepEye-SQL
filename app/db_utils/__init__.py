__all__ = [
    # SQLite schema utilities
    "load_table_names",
    "load_column_names_and_types",
    "load_primary_keys",
    "load_foreign_keys",
    "load_database_schema_dict",
    "get_database_schema_profile",
    "map_lower_table_name_to_original_table_name",
    "map_lower_column_name_to_original_column_name",
    "filter_used_database_schema",
    "get_identical_schema_table_groups",
    # SQLite execution utilities
    "execute_sql",
    "execute_sql_without_cache",
    "execute_sql_for_data_item",
    "measure_execution_time",
    "measure_execution_time_for_data_item",
    "SQLExecutionResult",
    # Cloud schema utilities (Spider2)
    "load_cloud_database_schema_dict",
    "load_external_knowledge",
    "load_snowflake_database_schema",
    "load_bigquery_database_schema",
    # Cloud execution utilities (Spider2)
    "execute_cloud_sql",
    "execute_bigquery_sql",
    "execute_snowflake_sql",
]


def __getattr__(name):
    if name in {
        "load_table_names",
        "load_column_names_and_types",
        "load_primary_keys",
        "load_foreign_keys",
        "load_database_schema_dict",
        "get_database_schema_profile",
        "map_lower_table_name_to_original_table_name",
        "map_lower_column_name_to_original_column_name",
        "filter_used_database_schema",
        "get_identical_schema_table_groups",
    }:
        from . import schema as schema_module

        return getattr(schema_module, name)

    if name in {
        "execute_sql",
        "execute_sql_without_cache",
        "execute_sql_for_data_item",
        "measure_execution_time",
        "measure_execution_time_for_data_item",
        "SQLExecutionResult",
    }:
        from . import execution as execution_module

        return getattr(execution_module, name)

    if name in {
        "load_cloud_database_schema_dict",
        "load_external_knowledge",
        "load_snowflake_database_schema",
        "load_bigquery_database_schema",
    }:
        from . import cloud_schema as cloud_schema_module

        return getattr(cloud_schema_module, name)

    if name in {
        "execute_cloud_sql",
        "execute_bigquery_sql",
        "execute_snowflake_sql",
    }:
        from . import cloud_execution as cloud_execution_module

        return getattr(cloud_execution_module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
