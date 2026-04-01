import json
import queue
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
        self._write_queue: queue.Queue[str | None] = queue.Queue()
        self._pending_lock = threading.Condition()
        self._pending_batches = 0
        self._writer_error: Exception | None = None
        self._closed = False
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name=f"artifact-writer-{stage_name}",
            daemon=True,
        )
        self._writer_thread.start()

    def has_checkpoint(self) -> bool:
        self.sync()
        return self._records_path.exists() and self._records_path.stat().st_size > 0

    def record_item(self, data_item: Any, extra_fields: Iterable[str] | None = None) -> None:
        self._raise_if_closed()
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
        self._raise_if_closed()
        self._raise_writer_error()
        with self._lock:
            if not self._buffer:
                return 0
            entries = self._buffer
            self._buffer = []

        self._ensure_meta()

        payload = "".join(f"{json.dumps(entry, ensure_ascii=False)}\n" for entry in entries)
        with self._pending_lock:
            self._pending_batches += 1
        self._write_queue.put(payload)
        logger.info(f"[{self._stage_name}] Queued {len(entries)} checkpoint records for {self._records_path}")
        return len(entries)

    def apply_to_dataset(self, dataset: Any) -> int:
        self.sync()
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
        self.close()
        if self._root.exists():
            shutil.rmtree(self._root)

    def sync(self) -> None:
        self._raise_writer_error()
        with self._pending_lock:
            while self._pending_batches > 0 and self._writer_error is None:
                self._pending_lock.wait()
        self._raise_writer_error()

    def close(self) -> None:
        if self._closed:
            return
        self.sync()
        self._write_queue.put(None)
        self._writer_thread.join()
        self._closed = True
        self._raise_writer_error()

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

    def _ensure_meta(self) -> None:
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

    def _writer_loop(self) -> None:
        while True:
            payload = self._write_queue.get()
            if payload is None:
                self._write_queue.task_done()
                break
            try:
                if self._writer_error is None:
                    with open(self._records_path, "a", encoding="utf-8", buffering=1024 * 1024) as f:
                        f.write(payload)
            except Exception as exc:
                self._writer_error = exc
            finally:
                with self._pending_lock:
                    self._pending_batches = max(0, self._pending_batches - 1)
                    if self._pending_batches == 0 or self._writer_error is not None:
                        self._pending_lock.notify_all()
                self._write_queue.task_done()

    def _raise_writer_error(self) -> None:
        if self._writer_error is not None:
            raise RuntimeError(
                f"ArtifactStore writer failed for stage {self._stage_name}: {self._writer_error}"
            ) from self._writer_error

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise RuntimeError(f"ArtifactStore for stage {self._stage_name} is already closed")


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
        fallback_path = Path(fallback_load_path)
        if not fallback_path.exists():
            raise FileNotFoundError(
                f"{stage_name} requires an input snapshot at {fallback_path}. "
                "Run the previous pipeline stage or preprocess the dataset first."
            )
        logger.info(f"Loading {stage_name} base dataset from {fallback_path}")
        dataset = load_dataset_fn(str(fallback_path))
        checkpoint_source = "base"

    if artifact_store.has_checkpoint():
        artifact_store.apply_to_dataset(dataset)
        checkpoint_source = f"{checkpoint_source}+artifact_checkpoint"

    return dataset, checkpoint_source
