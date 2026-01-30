import sys
from pathlib import Path
sys.path.append(".")
from app.dataset import DatasetFactory, save_dataset, load_dataset
from app.config import DatasetConfig, config
from app.logger import logger


def preprocess_dataset(dataset_config: DatasetConfig):
    if Path(dataset_config.save_path).exists():
        logger.info(f"Dataset already preprocessed at {dataset_config.save_path}, skipping...")
        return
    logger.info(f"Preprocessing dataset: {dataset_config.type} {dataset_config.split}")
    dataset = DatasetFactory.get_dataset(dataset_config)
    logger.info(f"Dataset loaded: {len(dataset)} items")
    save_dataset(dataset, dataset_config.save_path)
    logger.info(f"Dataset saved: {dataset_config.save_path}")


if __name__ == "__main__":
    preprocess_dataset(config.dataset_config)