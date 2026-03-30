"""
Deprecated wrapper for the old converter entrypoint.
Use `runner/convert_snapshot_to_sql.py` instead.
"""

import sys
sys.path.append(".")

from app.logger import logger

__all__ = [
    "auto_convert",
    "convert_to_json_file",
    "convert_to_sql_files",
    "main",
]


def auto_convert(*args, **kwargs):
    from runner.convert_snapshot_to_sql import auto_convert as snapshot_auto_convert

    return snapshot_auto_convert(*args, **kwargs)


def convert_to_json_file(*args, **kwargs):
    from runner.convert_snapshot_to_sql import convert_to_json_file as snapshot_convert_to_json_file

    return snapshot_convert_to_json_file(*args, **kwargs)


def convert_to_sql_files(*args, **kwargs):
    from runner.convert_snapshot_to_sql import convert_to_sql_files as snapshot_convert_to_sql_files

    return snapshot_convert_to_sql_files(*args, **kwargs)


def _rewrite_legacy_args(argv: list[str]) -> list[str]:
    rewritten_args: list[str] = []
    for arg in argv:
        if arg == "--pkl_path":
            rewritten_args.append("--snapshot_path")
        else:
            rewritten_args.append(arg)
    return rewritten_args


def main() -> None:
    logger.warning(
        "`runner/convert_pkl_to_sql.py` is deprecated and will be removed in a future release. "
        "Use `runner/convert_snapshot_to_sql.py --snapshot_path ...` instead."
    )
    sys.argv = [sys.argv[0], *_rewrite_legacy_args(sys.argv[1:])]
    from runner.convert_snapshot_to_sql import main as snapshot_main

    snapshot_main()


if __name__ == "__main__":
    main()
