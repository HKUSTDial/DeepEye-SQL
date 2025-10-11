import sys
sys.path.append(".")

from app.pipeline.sql_selection import BRSelectionRunner


if __name__ == "__main__":
    sql_selection_runner = BRSelectionRunner()
    sql_selection_runner.run()