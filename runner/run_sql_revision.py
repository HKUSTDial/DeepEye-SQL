import sys
sys.path.append(".")

from app.config import get_config
from app.logger import configure_logger
from app.pipeline.sql_revision import SQLRevisionRunner


if __name__ == "__main__":
    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)
    sql_reviser = SQLRevisionRunner.from_config(app_config)
    sql_reviser.run()
