import sys
sys.path.append(".")
from app.config import get_config
from app.pipeline import ValueRetrievalRunner

if __name__ == "__main__":
    runner = ValueRetrievalRunner.from_config(get_config())
    runner.run()
