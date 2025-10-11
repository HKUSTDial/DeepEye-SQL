import sys
sys.path.append(".")

from app.dataset import load_dataset, BaseDataset
from app.config import config
import json
from app.logger import logger

SAVE_PATH = config.sql_selection_config.save_path

def convert_to_sql_file():
    dataset = load_dataset(SAVE_PATH)
    data = {}
    for item in dataset:
        data[str(item.question_id)] = item.final_selected_sql.strip()
    with open(SAVE_PATH.replace(".pkl", ".json"), "w") as f:
        json.dump(data, f, indent=4)
    logger.info(f"Dataset converted to sql file successfully")
    logger.info(f"Dataset sql file saved to {SAVE_PATH.replace('.pkl', '.json')}")
    

convert_to_sql_file()