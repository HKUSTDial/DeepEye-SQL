import sys
sys.path.append(".")

from app.pipeline.sql_selection import SQLSelectionRunner


if __name__ == "__main__":
    sql_selection_runner = SQLSelectionRunner()
    sql_selection_runner.run()