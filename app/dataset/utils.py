import importlib
import json
import pickle
from pathlib import Path
from typing import Any

from app.config import DatasetConfig
from app.logger import logger

from .dataset import BaseDataset, DataItem


STRUCTURED_SNAPSHOT_FORMAT = "structured_dataset_snapshot"
STRUCTURED_SNAPSHOT_VERSION = 1


def save_dataset(dataset: BaseDataset, save_path: str) -> None:
    save_path = Path(save_path)
    snapshot_root = _get_snapshot_root(save_path)
    items_path = snapshot_root / "items.jsonl"
    save_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_root.mkdir(parents=True, exist_ok=True)

    item_class = DataItem
    if len(dataset) > 0:
        item_class = dataset[0].__class__

    with open(items_path, "w", encoding="utf-8") as f:
        for data_item in dataset:
            record = {
                "input": data_item.get_input_record().model_dump(),
                "pipeline_artifacts": data_item.get_pipeline_artifacts().model_dump(),
            }
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")

    manifest = {
        "format": STRUCTURED_SNAPSHOT_FORMAT,
        "version": STRUCTURED_SNAPSHOT_VERSION,
        "snapshot_root": snapshot_root.name,
        "dataset_class_module": dataset.__class__.__module__,
        "dataset_class_name": dataset.__class__.__name__,
        "item_class_module": item_class.__module__,
        "item_class_name": item_class.__name__,
        "dataset_config": dataset._config.model_dump(),
        "num_items": len(dataset),
    }
    save_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"Dataset saved to structured snapshot {save_path}")


def load_dataset(load_path: str) -> BaseDataset:
    load_path = Path(load_path)
    if not load_path.exists():
        raise FileNotFoundError(f"Dataset file not found at {load_path}")

    manifest = _try_load_structured_manifest(load_path)
    if manifest is not None:
        dataset = _load_structured_dataset(load_path, manifest)
        logger.info(f"Dataset loaded from structured snapshot {load_path}")
        return dataset

    with open(load_path, "rb") as f:
        dataset = pickle.load(f)
    logger.info(f"Dataset loaded from legacy pickle {load_path}")
    return dataset


def _try_load_structured_manifest(load_path: Path) -> dict[str, Any] | None:
    try:
        manifest = json.loads(load_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    if manifest.get("format") != STRUCTURED_SNAPSHOT_FORMAT:
        return None
    return manifest


def _load_structured_dataset(load_path: Path, manifest: dict[str, Any]) -> BaseDataset:
    snapshot_root = load_path.parent / manifest["snapshot_root"]
    items_path = snapshot_root / "items.jsonl"
    if not items_path.exists():
        raise FileNotFoundError(f"Structured dataset items file not found at {items_path}")

    dataset_cls = _load_class(manifest["dataset_class_module"], manifest["dataset_class_name"])
    item_cls = _load_class(manifest["item_class_module"], manifest["item_class_name"])
    dataset_config = DatasetConfig(**manifest["dataset_config"])

    dataset = object.__new__(dataset_cls)
    dataset._config = dataset_config
    dataset._database_schema_cache = {}
    dataset._data = []

    with open(items_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            item = item_cls(**record["input"])
            item.apply_pipeline_artifacts(record["pipeline_artifacts"])
            dataset._data.append(item)
            _populate_schema_cache(dataset, item)

    return dataset


def _populate_schema_cache(dataset: BaseDataset, data_item: DataItem) -> None:
    if not getattr(data_item, "database_schema", None):
        return

    cache_keys = {data_item.database_path, data_item.database_id}
    db_type = getattr(data_item, "db_type", None)
    if db_type:
        cache_keys.add(f"{db_type}:{data_item.database_id}")

    try:
        if data_item.database_path.endswith(".sqlite"):
            cache_keys.add(str(Path(data_item.database_path).resolve()))
    except OSError:
        pass

    for cache_key in cache_keys:
        dataset._database_schema_cache[cache_key] = data_item.database_schema


def _load_class(module_name: str, class_name: str):
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _get_snapshot_root(save_path: Path) -> Path:
    return save_path.with_name(f"{save_path.name}.snapshot")
