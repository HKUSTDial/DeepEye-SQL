import sys
sys.path.append(".")

from app.config import get_config
from app.pipeline.sql_revision import SQLRevisionRunner


if __name__ == "__main__":
    sql_reviser = SQLRevisionRunner.from_config(get_config())
    sql_reviser.run()
