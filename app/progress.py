from __future__ import annotations

from typing import Optional

from app.logger import logger


def should_log_progress(
    completed: int,
    total: int,
    interval: int,
    *,
    previous_completed: Optional[int] = None,
) -> bool:
    if total <= 0:
        return False
    completed = min(max(completed, 0), total)
    if completed == 0:
        return False

    markers = {1, total, max(1, total // 4), max(1, total // 2), max(1, (total * 3) // 4)}
    if completed in markers:
        return True

    interval = max(interval, 1)
    if previous_completed is None:
        return completed % interval == 0

    previous_completed = min(max(previous_completed, 0), total)
    return completed // interval > previous_completed // interval


def log_progress(
    label: str,
    completed: int,
    total: int,
    interval: int,
    *,
    previous_completed: Optional[int] = None,
) -> None:
    if should_log_progress(completed, total, interval, previous_completed=previous_completed):
        percent = min(max(completed, 0), total) / total * 100
        logger.info(f"{label}: {completed}/{total} ({percent:.1f}%)")


def should_checkpoint(completed: int, interval: int) -> bool:
    return completed > 0 and completed % max(interval, 1) == 0
