import sys
sys.path.append(".")
from app.config import get_config
from app.pipeline import SQLGenerationRunner

if __name__ == "__main__":
    runner = SQLGenerationRunner.from_config(get_config())
    runner.run()
