import sys
sys.path.append(".")
from app.pipeline import SQLGenerationRunner

if __name__ == "__main__":
    runner = SQLGenerationRunner()
    runner.run()