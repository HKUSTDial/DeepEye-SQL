from __future__ import annotations

import json
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from app.few_shot.masker import MaskCache, MaskResult, mask_training_example
from app.few_shot.train_loader import TrainingExample, load_training_examples
from app.llm import LLM
from app.logger import logger
from app.vector_db.vector_db import get_embedding_function


@dataclass
class FewShotIndexBuildResult:
    save_path: Path
    example_count: int
    manifest_path: Path
    skipped: bool = False


def build_few_shot_index(
    dataset_type: str,
    root_path: str | Path,
    save_path: str | Path,
    embedding_config: Any,
    llm: Optional[LLM] = None,
    mask_cache_path: Optional[str | Path] = None,
    batch_size: int = 128,
    n_parallel: int = 1,
    llm_timeout: int = 300,
    max_samples: Optional[int] = None,
    force_rebuild: bool = False,
    skip_mask_llm: bool = False,
) -> FewShotIndexBuildResult:
    save_path = Path(save_path)
    manifest_path = save_path / "manifest.json"
    if manifest_path.exists() and not force_rebuild:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        logger.info(f"Few-shot index already exists at {save_path}; use force_rebuild to rebuild.")
        return FewShotIndexBuildResult(
            save_path=save_path,
            example_count=int(manifest.get("example_count", 0)),
            manifest_path=manifest_path,
            skipped=True,
        )

    if save_path.exists():
        if not force_rebuild:
            raise FileExistsError(f"Few-shot index path already exists without manifest: {save_path}. Use --force to rebuild.")
        shutil.rmtree(save_path)

    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    if n_parallel < 1:
        raise ValueError(f"n_parallel must be >= 1, got {n_parallel}")
    if llm_timeout < 1:
        raise ValueError(f"llm_timeout must be >= 1, got {llm_timeout}")
    if not skip_mask_llm and llm is None:
        raise ValueError("llm is required for LLM masking. Set skip_mask_llm=True to build a raw-text index.")

    save_path.mkdir(parents=True, exist_ok=True)
    resolved_cache_path = Path(mask_cache_path) if mask_cache_path is not None else save_path / "mask_cache.jsonl"

    examples = load_training_examples(
        dataset_type=dataset_type,
        root_path=root_path,
        max_samples=max_samples,
    )
    if not examples:
        raise ValueError(f"No training examples loaded for dataset={dataset_type}, root_path={root_path}")

    logger.info(f"Loaded {len(examples)} training examples for few-shot index")
    cache = None if skip_mask_llm else MaskCache(resolved_cache_path)
    mask_results = _mask_examples(
        examples=examples,
        llm=llm,
        cache=cache,
        skip_mask_llm=skip_mask_llm,
        n_parallel=n_parallel,
        llm_timeout=llm_timeout,
    )

    examples_path = save_path / "examples.jsonl"
    _write_examples(examples_path=examples_path, examples=examples, mask_results=mask_results)

    embedding_function = get_embedding_function(
        model_name_or_path=embedding_config.embedding_model_name_or_path,
        api_type=embedding_config.api_type,
        use_qwen3_embedding=embedding_config.use_qwen3_embedding,
        local_files_only=embedding_config.local_files_only,
        normalize_embeddings=embedding_config.normalize_embeddings,
        base_url=embedding_config.base_url,
        api_key=embedding_config.api_key,
        embedding_device=embedding_config.embedding_device,
    )

    question_embeddings = _embed_texts(
        texts=[mask_result.masked_question for mask_result in mask_results],
        embedding_function=embedding_function,
        batch_size=batch_size,
        label="masked questions",
    )
    sql_embeddings = _embed_texts(
        texts=[mask_result.masked_sql for mask_result in mask_results],
        embedding_function=embedding_function,
        batch_size=batch_size,
        label="masked SQL",
    )

    question_embeddings_path = save_path / "question_embeddings.npy"
    sql_embeddings_path = save_path / "sql_embeddings.npy"
    np.save(question_embeddings_path, question_embeddings)
    np.save(sql_embeddings_path, sql_embeddings)

    manifest = _build_manifest(
        dataset_type=dataset_type,
        root_path=root_path,
        save_path=save_path,
        example_count=len(examples),
        embedding_config=embedding_config,
        llm_config=llm.llm_config if llm is not None and not skip_mask_llm else None,
        mask_cache_path=resolved_cache_path if not skip_mask_llm else None,
        batch_size=batch_size,
        n_parallel=n_parallel,
        llm_timeout=llm_timeout,
        max_samples=max_samples,
        skip_mask_llm=skip_mask_llm,
        question_embedding_dim=question_embeddings.shape[1],
        sql_embedding_dim=sql_embeddings.shape[1],
    )
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    logger.info(f"Built few-shot index at {save_path} with {len(examples)} examples")
    return FewShotIndexBuildResult(
        save_path=save_path,
        example_count=len(examples),
        manifest_path=manifest_path,
    )


def _mask_examples(
    examples: List[TrainingExample],
    llm: Optional[LLM],
    cache: Optional[MaskCache],
    skip_mask_llm: bool,
    n_parallel: int,
    llm_timeout: int,
) -> List[MaskResult]:
    if n_parallel == 1:
        results = []
        for idx, example in enumerate(examples, start=1):
            results.append(
                mask_training_example(
                    example=example,
                    llm=llm,
                    cache=cache,
                    skip_llm=skip_mask_llm,
                    llm_timeout=llm_timeout,
                )
            )
            _log_progress("Masking few-shot examples", idx, len(examples))
        return results

    results: List[Optional[MaskResult]] = [None] * len(examples)
    completed = 0
    with ThreadPoolExecutor(max_workers=n_parallel) as executor:
        futures = {
            executor.submit(mask_training_example, example, llm, cache, skip_mask_llm, llm_timeout): idx
            for idx, example in enumerate(examples)
        }
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
            completed += 1
            _log_progress("Masking few-shot examples", completed, len(examples))

    if any(result is None for result in results):
        raise RuntimeError("Some few-shot examples did not produce mask results")
    return [result for result in results if result is not None]


def _write_examples(examples_path: Path, examples: List[TrainingExample], mask_results: List[MaskResult]) -> None:
    if len(examples) != len(mask_results):
        raise ValueError(f"Example/mask result count mismatch: {len(examples)} examples, {len(mask_results)} mask results")

    with open(examples_path, "w", encoding="utf-8") as f:
        for example, mask_result in zip(examples, mask_results):
            record = example.to_record(
                masked_question=mask_result.masked_question,
                masked_sql=mask_result.masked_sql,
                mask_source=mask_result.source,
            )
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _embed_texts(texts: List[str], embedding_function: Any, batch_size: int, label: str) -> np.ndarray:
    embeddings: List[List[float]] = []
    total = len(texts)
    for start in range(0, total, batch_size):
        batch = texts[start : start + batch_size]
        batch_embeddings = embedding_function(batch)
        embeddings.extend(batch_embeddings)
        _log_progress(f"Embedding {label}", min(start + len(batch), total), total)

    matrix = np.asarray(embeddings, dtype=np.float32)
    if matrix.ndim != 2 or matrix.shape[0] != total:
        raise ValueError(f"Embedding function returned invalid shape for {label}: {matrix.shape}")
    return _l2_normalize(matrix)


def _l2_normalize(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


def _log_progress(label: str, completed: int, total: int) -> None:
    if total <= 0:
        return
    markers = {1, total, max(1, total // 4), max(1, total // 2), max(1, (total * 3) // 4)}
    if completed in markers:
        logger.info(f"{label}: {completed}/{total}")


def _build_manifest(
    dataset_type: str,
    root_path: str | Path,
    save_path: Path,
    example_count: int,
    embedding_config: Any,
    llm_config: Any,
    mask_cache_path: Optional[Path],
    batch_size: int,
    n_parallel: int,
    llm_timeout: int,
    max_samples: Optional[int],
    skip_mask_llm: bool,
    question_embedding_dim: int,
    sql_embedding_dim: int,
) -> Dict[str, Any]:
    return {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset_type": dataset_type,
        "root_path": str(root_path),
        "save_path": str(save_path),
        "example_count": example_count,
        "files": {
            "examples": "examples.jsonl",
            "question_embeddings": "question_embeddings.npy",
            "sql_embeddings": "sql_embeddings.npy",
        },
        "masking": {
            "skip_mask_llm": skip_mask_llm,
            "cache_path": str(mask_cache_path) if mask_cache_path is not None else None,
            "n_parallel": n_parallel,
            "llm_timeout": llm_timeout,
            "llm": _redact_config(llm_config) if llm_config is not None else None,
        },
        "embedding": {
            "config": _redact_config(embedding_config),
            "batch_size": batch_size,
            "question_embedding_dim": question_embedding_dim,
            "sql_embedding_dim": sql_embedding_dim,
            "normalized": True,
        },
        "max_samples": max_samples,
    }


def _redact_config(config_obj: Any) -> Dict[str, Any]:
    if hasattr(config_obj, "model_dump"):
        config = config_obj.model_dump()
    elif isinstance(config_obj, dict):
        config = dict(config_obj)
    else:
        config = dict(getattr(config_obj, "__dict__", {}))

    if config.get("api_key"):
        config["api_key"] = "<redacted>"
    return config
