from __future__ import annotations

import hashlib
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
from app.progress import log_progress
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
    embedding_batch_size: int = 128,
    parallelism: int = 1,
    llm_timeout: int = 300,
    progress_log_interval: int = 50,
    max_samples: Optional[int] = None,
    max_samples_per_db: Optional[int] = None,
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
        if force_rebuild:
            shutil.rmtree(save_path)
        elif not manifest_path.exists():
            logger.info(f"Resuming incomplete few-shot index build at {save_path}")

    if embedding_batch_size < 1:
        raise ValueError(f"embedding_batch_size must be >= 1, got {embedding_batch_size}")
    if parallelism < 1:
        raise ValueError(f"parallelism must be >= 1, got {parallelism}")
    if llm_timeout < 1:
        raise ValueError(f"llm_timeout must be >= 1, got {llm_timeout}")
    if progress_log_interval < 1:
        raise ValueError(f"progress_log_interval must be >= 1, got {progress_log_interval}")
    if not skip_mask_llm and llm is None:
        raise ValueError("llm is required for LLM masking. Set skip_mask_llm=True to build a raw-text index.")

    save_path.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = save_path / ".build_checkpoint"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    resolved_cache_path = Path(mask_cache_path) if mask_cache_path is not None else save_path / "mask_cache.jsonl"

    examples = load_training_examples(
        dataset_type=dataset_type,
        root_path=root_path,
        max_samples=max_samples,
        max_samples_per_db=max_samples_per_db,
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
        parallelism=parallelism,
        llm_timeout=llm_timeout,
        progress_log_interval=progress_log_interval,
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
    embedding_checkpoint_key = _embedding_checkpoint_key(embedding_config, embedding_batch_size)

    question_embeddings = _embed_texts(
        texts=[mask_result.masked_question for mask_result in mask_results],
        embedding_function=embedding_function,
        embedding_batch_size=embedding_batch_size,
        label="masked questions",
        progress_log_interval=progress_log_interval,
        checkpoint_dir=checkpoint_dir / "question_embeddings",
        checkpoint_key=embedding_checkpoint_key,
    )
    sql_embeddings = _embed_texts(
        texts=[mask_result.masked_sql for mask_result in mask_results],
        embedding_function=embedding_function,
        embedding_batch_size=embedding_batch_size,
        label="masked SQL",
        progress_log_interval=progress_log_interval,
        checkpoint_dir=checkpoint_dir / "sql_embeddings",
        checkpoint_key=embedding_checkpoint_key,
    )

    question_embeddings_path = save_path / "question_embeddings.npy"
    sql_embeddings_path = save_path / "sql_embeddings.npy"
    _save_npy_atomic(question_embeddings_path, question_embeddings)
    _save_npy_atomic(sql_embeddings_path, sql_embeddings)

    manifest = _build_manifest(
        dataset_type=dataset_type,
        root_path=root_path,
        save_path=save_path,
        example_count=len(examples),
        embedding_config=embedding_config,
        llm_config=llm.llm_config if llm is not None and not skip_mask_llm else None,
        mask_cache_path=resolved_cache_path if not skip_mask_llm else None,
        embedding_batch_size=embedding_batch_size,
        parallelism=parallelism,
        llm_timeout=llm_timeout,
        max_samples=max_samples,
        max_samples_per_db=max_samples_per_db,
        skip_mask_llm=skip_mask_llm,
        question_embedding_dim=question_embeddings.shape[1],
        sql_embedding_dim=sql_embeddings.shape[1],
    )
    manifest_tmp_path = manifest_path.with_suffix(".json.tmp")
    with open(manifest_tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    manifest_tmp_path.replace(manifest_path)
    shutil.rmtree(checkpoint_dir, ignore_errors=True)

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
    parallelism: int,
    llm_timeout: int,
    progress_log_interval: int,
) -> List[MaskResult]:
    if parallelism == 1:
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
            log_progress("Masking few-shot examples", idx, len(examples), progress_log_interval, previous_completed=idx - 1)
        return results

    results: List[Optional[MaskResult]] = [None] * len(examples)
    completed = 0
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(mask_training_example, example, llm, cache, skip_mask_llm, llm_timeout): idx
            for idx, example in enumerate(examples)
        }
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
            previous_completed = completed
            completed += 1
            log_progress("Masking few-shot examples", completed, len(examples), progress_log_interval, previous_completed=previous_completed)

    if any(result is None for result in results):
        raise RuntimeError("Some few-shot examples did not produce mask results")
    return [result for result in results if result is not None]


def _write_examples(examples_path: Path, examples: List[TrainingExample], mask_results: List[MaskResult]) -> None:
    if len(examples) != len(mask_results):
        raise ValueError(f"Example/mask result count mismatch: {len(examples)} examples, {len(mask_results)} mask results")

    tmp_path = examples_path.with_suffix(".jsonl.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        for example, mask_result in zip(examples, mask_results):
            record = example.to_record(
                masked_question=mask_result.masked_question,
                masked_sql=mask_result.masked_sql,
                mask_source=mask_result.source,
            )
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    tmp_path.replace(examples_path)


def _embed_texts(
    texts: List[str],
    embedding_function: Any,
    embedding_batch_size: int,
    label: str,
    progress_log_interval: int,
    checkpoint_dir: Path,
    checkpoint_key: str,
) -> np.ndarray:
    embeddings: List[List[float]] = []
    total = len(texts)
    _prepare_embedding_checkpoint(
        checkpoint_dir=checkpoint_dir,
        checkpoint_key=checkpoint_key,
        text_hash=_hash_texts(texts),
        total=total,
        embedding_batch_size=embedding_batch_size,
        label=label,
    )
    for start in range(0, total, embedding_batch_size):
        batch = texts[start : start + embedding_batch_size]
        shard_path = checkpoint_dir / f"{start:08d}_{start + len(batch):08d}.npy"
        batch_embeddings = _load_embedding_shard(shard_path, expected_rows=len(batch))
        if batch_embeddings is None:
            batch_embeddings = np.asarray(embedding_function(batch), dtype=np.float32)
            _save_npy_atomic(shard_path, batch_embeddings)
        embeddings.extend(batch_embeddings)
        log_progress(
            f"Embedding {label}",
            min(start + len(batch), total),
            total,
            progress_log_interval,
            previous_completed=start,
        )

    matrix = np.asarray(embeddings, dtype=np.float32)
    if matrix.ndim != 2 or matrix.shape[0] != total:
        raise ValueError(f"Embedding function returned invalid shape for {label}: {matrix.shape}")
    return _l2_normalize(matrix)


def _prepare_embedding_checkpoint(
    checkpoint_dir: Path,
    checkpoint_key: str,
    text_hash: str,
    total: int,
    embedding_batch_size: int,
    label: str,
) -> None:
    expected_metadata = {
        "version": 1,
        "label": label,
        "checkpoint_key": checkpoint_key,
        "text_hash": text_hash,
        "total": total,
        "embedding_batch_size": embedding_batch_size,
    }
    metadata_path = checkpoint_dir / "metadata.json"
    if metadata_path.exists():
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                existing_metadata = json.load(f)
        except json.JSONDecodeError:
            existing_metadata = None
        if existing_metadata != expected_metadata:
            logger.info(f"Discarding stale embedding checkpoint for {label}")
            shutil.rmtree(checkpoint_dir, ignore_errors=True)

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    if not metadata_path.exists():
        tmp_path = metadata_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(expected_metadata, f, indent=2, ensure_ascii=False)
        tmp_path.replace(metadata_path)


def _load_embedding_shard(shard_path: Path, expected_rows: int) -> Optional[np.ndarray]:
    if not shard_path.exists():
        return None
    try:
        shard = np.load(shard_path)
    except Exception:
        shard_path.unlink(missing_ok=True)
        return None
    if shard.ndim != 2 or shard.shape[0] != expected_rows:
        shard_path.unlink(missing_ok=True)
        return None
    return np.asarray(shard, dtype=np.float32)


def _save_npy_atomic(path: Path, array: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "wb") as f:
        np.save(f, array)
    tmp_path.replace(path)


def _hash_texts(texts: List[str]) -> str:
    hasher = hashlib.sha1()
    for text in texts:
        encoded = text.encode("utf-8")
        hasher.update(len(encoded).to_bytes(8, byteorder="big"))
        hasher.update(encoded)
    return hasher.hexdigest()


def _embedding_checkpoint_key(embedding_config: Any, embedding_batch_size: int) -> str:
    payload = {
        "embedding_config": _redact_config(embedding_config),
        "embedding_batch_size": embedding_batch_size,
    }
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(content.encode("utf-8")).hexdigest()


def _l2_normalize(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


def _build_manifest(
    dataset_type: str,
    root_path: str | Path,
    save_path: Path,
    example_count: int,
    embedding_config: Any,
    llm_config: Any,
    mask_cache_path: Optional[Path],
    embedding_batch_size: int,
    parallelism: int,
    llm_timeout: int,
    max_samples: Optional[int],
    max_samples_per_db: Optional[int],
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
            "parallelism": parallelism,
            "llm_timeout": llm_timeout,
            "llm": _redact_config(llm_config) if llm_config is not None else None,
        },
        "embedding": {
            "config": _redact_config(embedding_config),
            "embedding_batch_size": embedding_batch_size,
            "question_embedding_dim": question_embedding_dim,
            "sql_embedding_dim": sql_embedding_dim,
            "normalized": True,
        },
        "max_samples": max_samples,
        "max_samples_per_db": max_samples_per_db,
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
