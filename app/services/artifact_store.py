import json
import shutil
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Tuple

from app.dataset.artifacts import STAGE_ARTIFACT_FIELDS
from app.logger import logger


class ArtifactStore:
    def __init__(self, save_path: str, stage_name: str, stage_fields: Iterable[str]):
        self._save_path = Path(save_path)
        self._stage_name = stage_name
        self._stage_fields = list(dict.fromkeys(stage_fields))
        self._root = self._save_path.with_name(f"{self._save_path.stem}.artifacts")
        self._meta_path = self._root / "meta.json"
        self._records_path = self._root / f"{stage_name}.jsonl"
        self._buffer: list[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def has_checkpoint(self) -> bool:
        return self._records_path.exists() and self._records_path.stat().st_size > 0

    def record_item(self, data_item: Any, extra_fields: Iterable[str] | None = None) -> None:
        entry = {
            "item_id": self._get_item_id(data_item),
            "stage_artifact": self._to_jsonable(data_item.get_stage_artifact(self._stage_name).model_dump()),
            "metrics": self._to_jsonable(data_item.get_metrics_record().model_dump()),
        }
        if extra_fields is not None:
            entry["fields"] = {
                field: self._to_jsonable(getattr(data_item, field))
                for field in dict.fromkeys(extra_fields)
                if hasattr(data_item, field)
            }
        with self._lock:
            self._buffer.append(entry)

    def flush(self) -> int:
        with self._lock:
            if not self._buffer:
                return 0
            entries = self._buffer
            self._buffer = []

        self._root.mkdir(parents=True, exist_ok=True)
        if not self._meta_path.exists():
            self._meta_path.write_text(
                json.dumps(
                    {
                        "format_version": 2,
                        "stage_name": self._stage_name,
                        "save_path": str(self._save_path),
                        "stage_fields": self._stage_fields,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        with open(self._records_path, "a", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=False))
                f.write("\n")

        logger.info(f"[{self._stage_name}] Flushed {len(entries)} checkpoint records to {self._records_path}")
        return len(entries)

    def apply_to_dataset(self, dataset: Any) -> int:
        if not self.has_checkpoint():
            return 0

        latest_entries: Dict[str, Dict[str, Any]] = {}
        with open(self._records_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                entry = json.loads(line)
                latest_entries[entry["item_id"]] = entry

        item_map = {self._get_item_id(item): item for item in dataset}
        applied = 0
        for item_id, entry in latest_entries.items():
            item = item_map.get(item_id)
            if item is None:
                logger.warning(f"[{self._stage_name}] Checkpoint item {item_id} not found in base dataset")
                continue
            if "stage_artifact" in entry:
                item.apply_stage_artifact(self._stage_name, entry["stage_artifact"])
            if "metrics" in entry:
                item.apply_metrics_record(entry["metrics"])
            fields = entry.get("fields", {})
            for field, value in fields.items():
                setattr(item, field, value)
            applied += 1

        logger.info(f"[{self._stage_name}] Restored {applied} items from incremental checkpoint")
        return applied

    def cleanup(self) -> None:
        if self._root.exists():
            shutil.rmtree(self._root)

    @staticmethod
    def _get_item_id(data_item: Any) -> str:
        if hasattr(data_item, "get_item_id") and callable(data_item.get_item_id):
            return str(data_item.get_item_id())
        if hasattr(data_item, "instance_id") and getattr(data_item, "instance_id"):
            return str(getattr(data_item, "instance_id"))
        return str(getattr(data_item, "question_id"))

    @classmethod
    def _to_jsonable(cls, value: Any) -> Any:
        if hasattr(value, "model_dump") and callable(value.model_dump):
            value = value.model_dump()

        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(k): cls._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._to_jsonable(v) for v in value]
        if isinstance(value, set):
            return [cls._to_jsonable(v) for v in sorted(value, key=str)]
        return value


def load_stage_dataset(
    *,
    load_dataset_fn: Callable[[str], Any],
    current_save_path: str,
    fallback_load_path: str,
    artifact_store: ArtifactStore,
    stage_name: str,
) -> Tuple[Any, str]:
    current_path = Path(current_save_path)
    if current_path.exists():
        logger.info(f"Loading {stage_name} snapshot from {current_path}")
        dataset = load_dataset_fn(str(current_path))
        checkpoint_source = "snapshot"
    else:
        logger.info(f"Loading {stage_name} base dataset from {fallback_load_path}")
        dataset = load_dataset_fn(str(fallback_load_path))
        checkpoint_source = "base"

    if artifact_store.has_checkpoint():
        artifact_store.apply_to_dataset(dataset)
        checkpoint_source = f"{checkpoint_source}+artifact_checkpoint"

    return dataset, checkpoint_source
