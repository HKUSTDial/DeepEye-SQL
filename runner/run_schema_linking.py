import sys
sys.path.append(".")
from app.pipeline import SchemaLinkingRunner

if __name__ == "__main__":
    runner = SchemaLinkingRunner()
    runner.run()