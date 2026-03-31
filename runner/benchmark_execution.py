import json
import random
import sqlite3
import statistics
import sys
import tempfile
import time
from argparse import ArgumentParser
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

sys.path.append(".")

from app.dataset import load_dataset
from app.logger import configure_logger, logger
from app.pipeline.sql_selection.sql_selection import SQLSelectionRunner
from app.services.execution_service import ExecutionService


def _percentile(samples: list[float], percentile: float) -> float:
    if not samples:
        return 0.0
    if len(samples) == 1:
        return float(samples[0])
    rank = (len(samples) - 1) * percentile
    lower = int(rank)
    upper = min(lower + 1, len(samples) - 1)
    if lower == upper:
        return float(samples[lower])
    weight = rank - lower
    return float(samples[lower] * (1 - weight) + samples[upper] * weight)


def _summarize_ms(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    return {
        "count": len(ordered),
        "mean_ms": round(statistics.mean(ordered), 3),
        "median_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(_percentile(ordered, 0.95), 3),
        "min_ms": round(ordered[0], 3),
        "max_ms": round(ordered[-1], 3),
    }


def _time_call(fn: Callable[[], Any], iterations: int) -> list[float]:
    samples = []
    for _ in range(iterations):
        start_time = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start_time) * 1000)
    return samples


def _create_synthetic_sqlite(db_path: Path, row_count: int) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute(
            """
            CREATE TABLE records (
                id INTEGER PRIMARY KEY,
                group_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX idx_records_group_id ON records(group_id)")
        conn.execute("CREATE INDEX idx_records_amount ON records(amount)")

        rows = []
        for idx in range(row_count):
            rows.append(
                (
                    idx,
                    idx % 97,
                    f"cat_{idx % 11}",
                    round((idx * 1.37) % 1000, 3),
                    f"payload_{idx:06d}",
                )
            )
        conn.executemany(
            "INSERT INTO records(id, group_id, category, amount, payload) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _build_synthetic_item(db_path: str, sql_candidates: list[str] | None = None) -> Any:
    return SimpleNamespace(
        question_id=0,
        question="Synthetic benchmark item",
        evidence="",
        database_path=db_path,
        sql_candidates_after_revision=sql_candidates or [],
    )


class CountingExecutionService:
    def __init__(self):
        self._service = ExecutionService()
        self.execute_calls = 0
        self.measure_calls = 0
        self.hash_calls = 0

    def execute(self, data_item: Any, sql: str, timeout: int | None = None, use_cache: bool = True):
        self.execute_calls += 1
        return self._service.execute(data_item, sql, timeout=timeout, use_cache=use_cache)

    def measure_time(self, data_item: Any, sql: str, timeout: int | None = None, repeat: int = 10, use_cache: bool = True) -> float:
        self.measure_calls += 1
        return self._service.measure_time(data_item, sql, timeout=timeout, repeat=repeat, use_cache=use_cache)

    def hash_result(self, data_item: Any, result_rows: Any) -> Any:
        self.hash_calls += 1
        return self._service.hash_result(data_item, result_rows)

    def reset_counters(self) -> None:
        self.execute_calls = 0
        self.measure_calls = 0
        self.hash_calls = 0


def _benchmark_query(service: ExecutionService, item: Any, sql: str, iterations: int, repeat: int) -> dict[str, Any]:
    uncached_execute = _time_call(lambda: service.execute(item, sql, use_cache=False), iterations)
    service.execute(item, sql)
    cached_execute = _time_call(lambda: service.execute(item, sql), iterations)
    measure_time = _time_call(lambda: service.measure_time(item, sql, repeat=repeat, use_cache=False), iterations)

    baseline_result = service.execute(item, sql, use_cache=False)
    return {
        "result_type": baseline_result.result_type,
        "row_count": len(baseline_result.result_rows or []),
        "uncached_execute_ms": _summarize_ms(uncached_execute),
        "cached_execute_ms": _summarize_ms(cached_execute),
        "measure_time_ms": _summarize_ms(measure_time),
    }


def _benchmark_selection_scan(db_path: str, iterations: int, filter_top_k_sql: int) -> dict[str, Any]:
    sql_candidates = [
        "SELECT payload FROM records WHERE id = 42",
        "SELECT payload FROM records WHERE id=42",
        "SELECT id, payload FROM records WHERE group_id = 7 ORDER BY id LIMIT 10",
        "SELECT id, payload FROM records WHERE group_id = 7 ORDER BY id LIMIT 10",
        "SELECT id, category FROM records WHERE id = 11",
        "SELECT id FROM records WHERE id < 0",
        "SELECT missing_column FROM records",
    ]
    item = _build_synthetic_item(db_path, sql_candidates=sql_candidates)

    cold_samples = []
    cold_execute_calls = []
    cold_measure_calls = []
    top_k_sizes = []
    for _ in range(iterations):
        service = CountingExecutionService()
        runner = SQLSelectionRunner.__new__(SQLSelectionRunner)
        runner._execution_service = service
        runner._stage_config = SimpleNamespace(filter_top_k_sql=filter_top_k_sql)
        start_time = time.perf_counter()
        top_k = runner._get_top_k_sql_candidates(item)
        cold_samples.append((time.perf_counter() - start_time) * 1000)
        cold_execute_calls.append(service.execute_calls)
        cold_measure_calls.append(service.measure_calls)
        top_k_sizes.append(len(top_k))

    warm_service = CountingExecutionService()
    warm_runner = SQLSelectionRunner.__new__(SQLSelectionRunner)
    warm_runner._execution_service = warm_service
    warm_runner._stage_config = SimpleNamespace(filter_top_k_sql=filter_top_k_sql)
    warm_runner._get_top_k_sql_candidates(item)
    warm_samples = []
    warm_execute_calls = []
    warm_measure_calls = []
    for _ in range(iterations):
        warm_service.reset_counters()
        start_time = time.perf_counter()
        warm_runner._get_top_k_sql_candidates(item)
        warm_samples.append((time.perf_counter() - start_time) * 1000)
        warm_execute_calls.append(warm_service.execute_calls)
        warm_measure_calls.append(warm_service.measure_calls)

    return {
        "candidate_count": len(sql_candidates),
        "unique_candidate_count": len(set(sql_candidates)),
        "cold_scan_ms": _summarize_ms(cold_samples),
        "warm_scan_ms": _summarize_ms(warm_samples),
        "cold_execute_invocations_mean": round(statistics.mean(cold_execute_calls), 3),
        "warm_execute_invocations_mean": round(statistics.mean(warm_execute_calls), 3),
        "cold_measure_invocations_mean": round(statistics.mean(cold_measure_calls), 3),
        "warm_measure_invocations_mean": round(statistics.mean(warm_measure_calls), 3),
        "top_k_size_mean": round(statistics.mean(top_k_sizes), 3),
    }


def run_synthetic_benchmark(rows: int, iterations: int, repeat: int, filter_top_k_sql: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="deepeye-sql-bench-") as temp_dir:
        db_path = Path(temp_dir) / "synthetic.sqlite"
        logger.info(f"Creating synthetic SQLite database with {rows} rows at {db_path}")
        _create_synthetic_sqlite(db_path, rows)
        item = _build_synthetic_item(str(db_path))
        service = ExecutionService()
        point_lookup_id = min(42, max(rows - 1, 0))
        range_start = max(rows // 4, 0)
        range_end = max(range_start, min(rows - 1, (rows * 3) // 4))

        queries = {
            "point_lookup": f"SELECT payload FROM records WHERE id = {point_lookup_id}",
            "indexed_group_agg": "SELECT group_id, COUNT(*), AVG(amount) FROM records WHERE group_id = 7 GROUP BY group_id",
            "range_sum": f"SELECT SUM(amount) FROM records WHERE id BETWEEN {range_start} AND {range_end}",
            "sort_limit": "SELECT id, payload FROM records ORDER BY amount DESC LIMIT 20",
            "empty_result": "SELECT id FROM records WHERE id < 0",
        }

        query_results = {}
        for query_name, sql in queries.items():
            logger.info(f"Benchmarking query workload: {query_name}")
            query_results[query_name] = _benchmark_query(service, item, sql, iterations, repeat)

        selection_results = _benchmark_selection_scan(
            str(db_path),
            iterations=iterations,
            filter_top_k_sql=filter_top_k_sql,
        )
        return {
            "mode": "synthetic_sqlite",
            "rows": rows,
            "iterations": iterations,
            "measure_repeat": repeat,
            "queries": query_results,
            "selection_scan": selection_results,
        }


def run_snapshot_selection_benchmark(snapshot_path: str, sample_size: int, filter_top_k_sql: int) -> dict[str, Any]:
    dataset = load_dataset(snapshot_path)
    candidates = [
        item for item in dataset
        if getattr(item, "sql_candidates_after_revision", None)
        and getattr(item, "database_path", None)
        and (getattr(item, "db_type", "sqlite") in (None, "sqlite"))
    ]
    if not candidates:
        raise ValueError("No SQLite items with sql_candidates_after_revision found in the snapshot")

    sample_size = min(sample_size, len(candidates))
    rng = random.Random(0)
    sampled_items = rng.sample(candidates, sample_size) if sample_size < len(candidates) else candidates

    durations = []
    execute_calls = []
    measure_calls = []
    candidate_counts = []
    top_k_sizes = []
    for item in sampled_items:
        service = CountingExecutionService()
        runner = SQLSelectionRunner.__new__(SQLSelectionRunner)
        runner._execution_service = service
        runner._stage_config = SimpleNamespace(filter_top_k_sql=filter_top_k_sql)
        start_time = time.perf_counter()
        top_k = runner._get_top_k_sql_candidates(item)
        durations.append((time.perf_counter() - start_time) * 1000)
        execute_calls.append(service.execute_calls)
        measure_calls.append(service.measure_calls)
        candidate_counts.append(len(item.sql_candidates_after_revision))
        top_k_sizes.append(len(top_k))

    return {
        "mode": "snapshot_selection",
        "snapshot_path": snapshot_path,
        "sample_size": sample_size,
        "selection_scan_ms": _summarize_ms(durations),
        "candidate_count_mean": round(statistics.mean(candidate_counts), 3),
        "execute_invocations_mean": round(statistics.mean(execute_calls), 3),
        "measure_invocations_mean": round(statistics.mean(measure_calls), 3),
        "top_k_size_mean": round(statistics.mean(top_k_sizes), 3),
    }


def main() -> None:
    parser = ArgumentParser(description="Benchmark DeepEye-SQL execution hotspots")
    parser.add_argument("--rows", type=int, default=20000, help="Synthetic SQLite row count")
    parser.add_argument("--iterations", type=int, default=8, help="Number of benchmark iterations per workload")
    parser.add_argument("--measure-repeat", type=int, default=5, help="Repeat count for ExecutionService.measure_time")
    parser.add_argument("--filter-top-k-sql", type=int, default=10, help="Top-k cutoff for SQL selection benchmark")
    parser.add_argument("--snapshot-path", type=str, default=None, help="Optional structured snapshot path for real SQL selection benchmark")
    parser.add_argument("--snapshot-sample-size", type=int, default=20, help="Number of snapshot items to sample when --snapshot-path is provided")
    parser.add_argument("--json-output", type=str, default=None, help="Optional path to write JSON benchmark output")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logger print level")
    args = parser.parse_args()

    configure_logger(args.log_level)

    summary = {
        "synthetic": run_synthetic_benchmark(
            rows=args.rows,
            iterations=args.iterations,
            repeat=args.measure_repeat,
            filter_top_k_sql=args.filter_top_k_sql,
        )
    }

    if args.snapshot_path is not None:
        summary["snapshot"] = run_snapshot_selection_benchmark(
            snapshot_path=args.snapshot_path,
            sample_size=args.snapshot_sample_size,
            filter_top_k_sql=args.filter_top_k_sql,
        )

    formatted = json.dumps(summary, indent=2, ensure_ascii=False)
    if args.json_output is not None:
        output_path = Path(args.json_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(formatted + "\n", encoding="utf-8")
        logger.info(f"Benchmark summary written to {output_path}")

    print(formatted)


if __name__ == "__main__":
    main()
