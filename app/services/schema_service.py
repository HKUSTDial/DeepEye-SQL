import threading
from pathlib import Path
from typing import Any, Callable, Dict

from app.db_utils.defaults import DEFAULT_MAX_VALUE_EXAMPLE_LENGTH
from app.db_utils.schema import get_database_schema_profile, load_database_schema_dict, load_value_examples, load_value_statistics
from app.logger import logger


PROFILE_STRIPPING_LEVELS = [
    {"include_description": True, "include_value_statistics": True, "include_value_examples": True, "include_nested_columns": True},
    {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": True},
    {"include_description": True, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
    {"include_description": False, "include_value_statistics": False, "include_value_examples": False, "include_nested_columns": False},
]


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

    def build_schema_profile(
        self,
        database_schema_dict: Dict[str, Any],
        *,
        compress_identical_schemas: bool = True,
        include_description: bool = True,
        include_value_statistics: bool = True,
        include_value_examples: bool = True,
        include_nested_columns: bool = True,
    ) -> str:
        self.ensure_schema_features(
            database_schema_dict,
            include_value_statistics=include_value_statistics,
            include_value_examples=include_value_examples,
        )
        return get_database_schema_profile(
            database_schema_dict,
            compress_identical_schemas=compress_identical_schemas,
            include_description=include_description,
            include_value_statistics=include_value_statistics,
            include_value_examples=include_value_examples,
            include_nested_columns=include_nested_columns,
        )

    def build_prompt_with_progressive_schema_stripping(
        self,
        database_schema_dict: Dict[str, Any],
        *,
        encoding_model_name: str,
        max_prompt_len: int,
        prompt_format_func: Callable[[str], str],
        item_id: Any,
        log_prefix: str,
    ) -> tuple[str | None, int]:
        import tiktoken

        try:
            encoding = tiktoken.encoding_for_model(encoding_model_name)
        except Exception:
            try:
                encoding = tiktoken.get_encoding("cl100k_base")
            except Exception as exc:
                logger.warning(
                    f"Falling back to approximate prompt length for {log_prefix} item {item_id}: {exc}"
                )
                encoding = None

        for level_idx, levels in enumerate(PROFILE_STRIPPING_LEVELS):
            database_schema_profile = self.build_schema_profile(
                database_schema_dict,
                **levels,
            )
            prompt = prompt_format_func(database_schema_profile).strip()
            token_count = len(encoding.encode(prompt)) if encoding is not None else len(prompt) // 4
            if token_count <= max_prompt_len:
                return prompt, level_idx
            logger.info(f"Level {level_idx} {log_prefix} prompt for item {item_id} too large ({token_count} tokens). Trying next level...")
        return None, len(PROFILE_STRIPPING_LEVELS)

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


def configure_schema_service(max_value_example_length: int = DEFAULT_MAX_VALUE_EXAMPLE_LENGTH) -> SchemaService:
    global _schema_service
    _schema_service = SchemaService(max_value_example_length=max_value_example_length)
    return _schema_service


def get_schema_service() -> SchemaService:
    global _schema_service
    if _schema_service is None:
        _schema_service = SchemaService()
    return _schema_service


def reset_schema_service() -> None:
    global _schema_service
    if _schema_service is not None:
        _schema_service.reset()
        _schema_service = None
