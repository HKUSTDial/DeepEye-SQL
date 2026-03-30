"""
Deprecated wrapper for the old converter entrypoint.
Use `runner/convert_snapshot_to_sql.py` instead.
"""

import sys
sys.path.append(".")

from runner.convert_snapshot_to_sql import auto_convert, convert_to_json_file, convert_to_sql_files, main

__all__ = [
    "auto_convert",
    "convert_to_json_file",
    "convert_to_sql_files",
    "main",
]


if __name__ == "__main__":
    main()
