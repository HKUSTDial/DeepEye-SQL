import sys
sys.path.append(".")

from app.pipeline.sql_revision import SQLRevisionRunner


if __name__ == "__main__":
    sql_reviser = SQLRevisionRunner()
    sql_reviser.run()