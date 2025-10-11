import sys
sys.path.append(".")
from app.pipeline import ValueRetrievalRunner

if __name__ == "__main__":
    runner = ValueRetrievalRunner()
    runner.run()