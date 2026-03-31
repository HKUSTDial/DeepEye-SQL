import sys
sys.path.append(".")

from app.config import get_config
from app.pipeline.sql_selection import SQLSelectionRunner


if __name__ == "__main__":
    sql_selection_runner = SQLSelectionRunner.from_config(get_config())
    sql_selection_runner.run()
