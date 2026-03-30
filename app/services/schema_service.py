import threading
from pathlib import Path
from typing import Any, Dict

from app.db_utils.defaults import DEFAULT_MAX_VALUE_EXAMPLE_LENGTH
from app.db_utils.schema import load_database_schema_dict, load_value_examples, load_value_statistics


class SchemaService:
    def __init__(self, max_value_example_length: int = DEFAULT_MAX_VALUE_EXAMPLE_LENGTH):
        self._sqlite_schema_cache: dict[str, Dict[str, Any]] = {}
        self._value_examples_cache: dict[tuple[str, str, str], list[str]] = {}
        self._value_statistics_cache: dict[tuple[str, str, str], Dict[str, Any]] = {}
        self._schema_locks: dict[str, threading.RLock] = {}
        self._lock = threading.RLock()
        self._max_value_example_length = max_value_example_length

    def load_sqlite_schema(self, db_path: str) -> Dict[str, Any]:
        cache_key = str(Path(db_path).resolve())
        with self._lock:
            if cache_key not in self._sqlite_schema_cache:
                self._sqlite_schema_cache[cache_key] = load_database_schema_dict(cache_key)
            return self._sqlite_schema_cache[cache_key]

    def ensure_schema_features(
        self,
        database_schema_dict: Dict[str, Any],
        *,
        include_value_examples: bool = False,
        include_value_statistics: bool = False,
    ) -> Dict[str, Any]:
        if not include_value_examples and not include_value_statistics:
            return database_schema_dict

        if database_schema_dict.get("db_type", "sqlite") != "sqlite":
            return database_schema_dict

        db_path = database_schema_dict.get("db_path")
        if not db_path:
            return database_schema_dict

        with self._get_schema_lock(db_path):
            for table_name, table_schema_dict in database_schema_dict.get("tables", {}).items():
                for column_name in table_schema_dict.get("columns", {}):
                    self._ensure_column_features_locked(
                        database_schema_dict,
                        table_name,
                        column_name,
                        include_value_examples=include_value_examples,
                        include_value_statistics=include_value_statistics,
                    )
        return database_schema_dict

    def ensure_column_features(
        self,
        database_schema_dict: Dict[str, Any],
        table_name: str,
        column_name: str,
        *,
        include_value_examples: bool = False,
        include_value_statistics: bool = False,
    ) -> Dict[str, Any]:
        if not include_value_examples and not include_value_statistics:
            return database_schema_dict

        if database_schema_dict.get("db_type", "sqlite") != "sqlite":
            return database_schema_dict

        db_path = database_schema_dict.get("db_path")
        if not db_path:
            return database_schema_dict

        with self._get_schema_lock(db_path):
            self._ensure_column_features_locked(
                database_schema_dict,
                table_name,
                column_name,
                include_value_examples=include_value_examples,
                include_value_statistics=include_value_statistics,
            )
        return database_schema_dict

    def reset(self) -> None:
        with self._lock:
            self._sqlite_schema_cache.clear()
            self._value_examples_cache.clear()
            self._value_statistics_cache.clear()
            self._schema_locks.clear()

    def _ensure_column_features_locked(
        self,
        database_schema_dict: Dict[str, Any],
        table_name: str,
        column_name: str,
        *,
        include_value_examples: bool,
        include_value_statistics: bool,
    ) -> None:
        column_schema_dict = database_schema_dict["tables"][table_name]["columns"][column_name]
        db_path = str(Path(database_schema_dict["db_path"]).resolve())
        column_cache_key = (db_path, table_name, column_name)
        column_type = str(column_schema_dict.get("column_type", "")).upper()

        if include_value_examples and column_schema_dict.get("value_examples") is None:
            if column_type == "BLOB":
                column_schema_dict["value_examples"] = []
            else:
                if column_cache_key not in self._value_examples_cache:
                    self._value_examples_cache[column_cache_key] = load_value_examples(
                        db_path,
                        table_name,
                        column_name,
                        max_example_length=self._max_value_example_length,
                    )
                column_schema_dict["value_examples"] = self._value_examples_cache[column_cache_key]

        if include_value_statistics and column_schema_dict.get("value_statistics") is None:
            if column_cache_key not in self._value_statistics_cache:
                self._value_statistics_cache[column_cache_key] = load_value_statistics(db_path, table_name, column_name)
            column_schema_dict["value_statistics"] = self._value_statistics_cache[column_cache_key]

    def _get_schema_lock(self, db_path: str) -> threading.RLock:
        cache_key = str(Path(db_path).resolve())
        with self._lock:
            if cache_key not in self._schema_locks:
                self._schema_locks[cache_key] = threading.RLock()
            return self._schema_locks[cache_key]


_schema_service: SchemaService | None = None


def get_schema_service() -> SchemaService:
    global _schema_service
    if _schema_service is None:
        max_value_example_length = DEFAULT_MAX_VALUE_EXAMPLE_LENGTH
        try:
            from app.config import config
        except FileNotFoundError:
            config = None
        if config is not None:
            max_value_example_length = config.dataset_config.max_value_example_length
        _schema_service = SchemaService(max_value_example_length=max_value_example_length)
    return _schema_service


def reset_schema_service() -> None:
    global _schema_service
    if _schema_service is not None:
        _schema_service.reset()
        _schema_service = None
