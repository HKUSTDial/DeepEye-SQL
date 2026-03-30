import pickle
from pathlib import Path

from app.logger import logger

from .dataset import BaseDataset


def load_legacy_pickle_dataset(load_path: str | Path) -> BaseDataset:
    load_path = Path(load_path)
    if not load_path.exists():
        raise FileNotFoundError(f"Legacy dataset file not found at {load_path}")

    logger.warning(
        f"Loading deprecated legacy pickle dataset {load_path}. "
        "Please migrate it with `runner/migrate_legacy_snapshot.py`."
    )
    with open(load_path, "rb") as f:
        dataset = pickle.load(f)
    logger.info(f"Dataset loaded from legacy pickle {load_path}")
    return dataset


def migrate_legacy_pickle_dataset(legacy_path: str | Path, snapshot_path: str | Path | None = None) -> Path:
    from .utils import save_dataset

    legacy_path = Path(legacy_path)
    target_path = Path(snapshot_path) if snapshot_path is not None else legacy_path
    dataset = load_legacy_pickle_dataset(legacy_path)
    save_dataset(dataset, str(target_path))
    logger.info(f"Migrated legacy pickle {legacy_path} to structured snapshot {target_path}")
    return target_path
