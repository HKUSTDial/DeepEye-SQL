"""
Migrate a legacy pickle dataset file to the structured dataset snapshot format.
"""

import sys
sys.path.append(".")

import argparse

from app.dataset import migrate_legacy_pickle_dataset
from app.logger import logger


def main():
    parser = argparse.ArgumentParser(
        description="Migrate a legacy pickle dataset file to the structured dataset snapshot format"
    )
    parser.add_argument(
        "--legacy_path",
        type=str,
        required=True,
        help="Path to the legacy pickle dataset file",
    )
    parser.add_argument(
        "--snapshot_path",
        type=str,
        default=None,
        help="Optional output path for the structured snapshot manifest. Defaults to in-place migration.",
    )
    args = parser.parse_args()

    target_path = migrate_legacy_pickle_dataset(
        legacy_path=args.legacy_path,
        snapshot_path=args.snapshot_path,
    )
    logger.info(f"Structured snapshot written to {target_path}")


if __name__ == "__main__":
    main()
