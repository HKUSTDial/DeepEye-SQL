import pickle
from .dataset import BaseDataset
from pathlib import Path
from app.logger import logger


def save_dataset(dataset: BaseDataset, save_path: str) -> None:
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, "wb") as f:
        pickle.dump(dataset, f)
    logger.info(f"Dataset saved to {save_path}")


def load_dataset(load_path: str) -> BaseDataset:
    load_path = Path(load_path)
    if not load_path.exists():
        raise FileNotFoundError(f"Dataset file not found at {load_path}")
    with open(load_path, "rb") as f:
        dataset = pickle.load(f)
    logger.info(f"Dataset loaded from {load_path}")
    return dataset