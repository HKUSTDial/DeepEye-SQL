import sys
sys.path.append(".")
from app.config import get_config
from app.pipeline import SchemaLinkingRunner

if __name__ == "__main__":
    runner = SchemaLinkingRunner.from_config(get_config())
    runner.run()
