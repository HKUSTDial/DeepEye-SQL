from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import numpy as np

from app.logger import logger
from app.vector_db.vector_db import get_embedding_function

try:
    import torch
except ImportError:  # pragma: no cover - torch is expected in normal runtime, but CPU fallback is valid.
    torch = None


FewShotRecord = Dict[str, Any]
FewShotExample = Dict[str, str]


@dataclass(frozen=True)
class FewShotRetrievalResult:
    rank: int
    index: int
    example: FewShotRecord
    score: float
    question_score: float
    sql_score: Optional[float]

    def to_few_shot_example(self) -> FewShotExample:
        few_shot_example = {
            "question": str(self.example["question"]),
            "sql": str(self.example["sql"]),
        }
        evidence = str(self.example.get("evidence", "")).strip()
        if evidence:
            few_shot_example["evidence"] = evidence
        return few_shot_example


class FewShotIndex:
    def __init__(
        self,
        index_path: str | Path,
        records: List[FewShotRecord],
        question_embeddings: np.ndarray,
        sql_embeddings: np.ndarray,
        manifest: Dict[str, Any],
        similarity_device: str = "cpu",
    ) -> None:
        self.index_path = Path(index_path)
        self.records = records
        self.question_embeddings = _normalize_matrix(question_embeddings)
        self.sql_embeddings = _normalize_matrix(sql_embeddings)
        self.manifest = manifest
        self._similarity_device = _resolve_similarity_device(similarity_device)
        self._similarity_tensor_lock = Lock()
        self._question_embeddings_tensor = None
        self._sql_embeddings_tensor = None
        self._validate()
        logger.info(f"Using few-shot similarity_device={self.similarity_device}")

    @property
    def similarity_device(self) -> str:
        return self._similarity_device or "cpu"

    @classmethod
    def load(
        cls,
        index_path: str | Path,
        mmap_mode: Optional[str] = "r",
        similarity_device: str = "cpu",
    ) -> "FewShotIndex":
        index_path = Path(index_path)
        manifest_path = index_path / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Few-shot index manifest not found: {manifest_path}")

        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        files = manifest.get("files", {})
        examples_path = index_path / files.get("examples", "examples.jsonl")
        question_embeddings_path = index_path / files.get("question_embeddings", "question_embeddings.npy")
        sql_embeddings_path = index_path / files.get("sql_embeddings", "sql_embeddings.npy")

        records = _load_records(examples_path)
        question_embeddings = np.load(question_embeddings_path, mmap_mode=mmap_mode)
        sql_embeddings = np.load(sql_embeddings_path, mmap_mode=mmap_mode)
        return cls(
            index_path=index_path,
            records=records,
            question_embeddings=question_embeddings,
            sql_embeddings=sql_embeddings,
            manifest=manifest,
            similarity_device=similarity_device,
        )

    def retrieve_by_embeddings(
        self,
        question_embedding: Sequence[float],
        sql_embedding: Optional[Sequence[float]] = None,
        top_k: int = 5,
        question_weight: float = 0.5,
        sql_weight: float = 0.5,
        exclude_example_ids: Optional[Iterable[str]] = None,
        exclude_db_ids: Optional[Iterable[str]] = None,
    ) -> List[FewShotRetrievalResult]:
        if top_k < 1:
            raise ValueError(f"top_k must be >= 1, got {top_k}")

        question_weight, sql_weight = _resolve_weights(
            question_weight=question_weight,
            sql_weight=sql_weight,
            has_sql_embedding=sql_embedding is not None,
        )
        question_vector = _normalize_vector(np.asarray(question_embedding, dtype=np.float32))
        if question_vector.shape[0] != self.question_embeddings.shape[1]:
            raise ValueError(
                "Question embedding dimension mismatch: "
                f"query={question_vector.shape[0]}, index={self.question_embeddings.shape[1]}"
            )

        sql_vector: Optional[np.ndarray] = None
        if sql_embedding is not None and sql_weight > 0:
            sql_vector = _normalize_vector(np.asarray(sql_embedding, dtype=np.float32))
            if sql_vector.shape[0] != self.sql_embeddings.shape[1]:
                raise ValueError(
                    "SQL embedding dimension mismatch: "
                    f"query={sql_vector.shape[0]}, index={self.sql_embeddings.shape[1]}"
                )

        question_scores, sql_scores = self._score_embeddings(
            question_vector=question_vector,
            sql_vector=sql_vector,
        )
        if sql_scores is not None:
            combined_scores = np.clip(question_weight * question_scores + sql_weight * sql_scores, -1.0, 1.0)
        else:
            combined_scores = question_scores

        allowed_indices = self._allowed_indices(
            exclude_example_ids=exclude_example_ids,
            exclude_db_ids=exclude_db_ids,
        )
        ranked_indices = sorted(
            allowed_indices,
            key=lambda idx: (
                float(combined_scores[idx]),
                float(question_scores[idx]),
                float(sql_scores[idx]) if sql_scores is not None else 0.0,
                -idx,
            ),
            reverse=True,
        )[:top_k]

        results: List[FewShotRetrievalResult] = []
        for rank, idx in enumerate(ranked_indices, start=1):
            results.append(
                FewShotRetrievalResult(
                    rank=rank,
                    index=idx,
                    example=self.records[idx],
                    score=float(combined_scores[idx]),
                    question_score=float(question_scores[idx]),
                    sql_score=float(sql_scores[idx]) if sql_scores is not None else None,
                )
            )
        return results

    def _score_embeddings(
        self,
        question_vector: np.ndarray,
        sql_vector: Optional[np.ndarray],
    ) -> tuple[np.ndarray, Optional[np.ndarray]]:
        if self._similarity_device is not None:
            return self._score_embeddings_torch(question_vector=question_vector, sql_vector=sql_vector)

        question_scores = np.clip(np.asarray(self.question_embeddings @ question_vector, dtype=np.float32), -1.0, 1.0)
        sql_scores = None
        if sql_vector is not None:
            sql_scores = np.clip(np.asarray(self.sql_embeddings @ sql_vector, dtype=np.float32), -1.0, 1.0)
        return question_scores, sql_scores

    def _score_embeddings_torch(
        self,
        question_vector: np.ndarray,
        sql_vector: Optional[np.ndarray],
    ) -> tuple[np.ndarray, Optional[np.ndarray]]:
        if torch is None or self._similarity_device is None:
            return self._score_embeddings(question_vector=question_vector, sql_vector=sql_vector)

        question_embeddings_tensor, sql_embeddings_tensor = self._get_similarity_tensors()
        with torch.no_grad():
            question_tensor = torch.as_tensor(question_vector, dtype=torch.float32, device=self._similarity_device)
            question_scores_tensor = torch.clamp(question_embeddings_tensor @ question_tensor, -1.0, 1.0)
            question_scores = question_scores_tensor.detach().cpu().numpy().astype(np.float32, copy=False)

            sql_scores = None
            if sql_vector is not None:
                sql_tensor = torch.as_tensor(sql_vector, dtype=torch.float32, device=self._similarity_device)
                sql_scores_tensor = torch.clamp(sql_embeddings_tensor @ sql_tensor, -1.0, 1.0)
                sql_scores = sql_scores_tensor.detach().cpu().numpy().astype(np.float32, copy=False)

        return question_scores, sql_scores

    def _get_similarity_tensors(self):
        if torch is None or self._similarity_device is None:
            raise RuntimeError("Torch similarity tensors requested without an active torch device")

        if self._question_embeddings_tensor is not None and self._sql_embeddings_tensor is not None:
            return self._question_embeddings_tensor, self._sql_embeddings_tensor

        with self._similarity_tensor_lock:
            if self._question_embeddings_tensor is None:
                self._question_embeddings_tensor = torch.as_tensor(
                    self.question_embeddings,
                    dtype=torch.float32,
                    device=self._similarity_device,
                )
            if self._sql_embeddings_tensor is None:
                self._sql_embeddings_tensor = torch.as_tensor(
                    self.sql_embeddings,
                    dtype=torch.float32,
                    device=self._similarity_device,
                )
            return self._question_embeddings_tensor, self._sql_embeddings_tensor

    def _allowed_indices(
        self,
        exclude_example_ids: Optional[Iterable[str]],
        exclude_db_ids: Optional[Iterable[str]],
    ) -> List[int]:
        excluded_example_ids = _to_string_set(exclude_example_ids)
        excluded_db_ids = _to_string_set(exclude_db_ids)

        if not excluded_example_ids and not excluded_db_ids:
            return list(range(len(self.records)))

        indices = []
        for idx, record in enumerate(self.records):
            example_id = str(record.get("example_id", ""))
            db_id = str(record.get("db_id", ""))
            if example_id in excluded_example_ids or db_id in excluded_db_ids:
                continue
            indices.append(idx)
        return indices

    def _validate(self) -> None:
        record_count = len(self.records)
        if record_count == 0:
            raise ValueError(f"Few-shot index has no records: {self.index_path}")
        if self.question_embeddings.ndim != 2:
            raise ValueError(f"Question embeddings must be 2D, got shape={self.question_embeddings.shape}")
        if self.sql_embeddings.ndim != 2:
            raise ValueError(f"SQL embeddings must be 2D, got shape={self.sql_embeddings.shape}")
        if self.question_embeddings.shape[0] != record_count:
            raise ValueError(
                f"Question embedding count mismatch: records={record_count}, embeddings={self.question_embeddings.shape[0]}"
            )
        if self.sql_embeddings.shape[0] != record_count:
            raise ValueError(f"SQL embedding count mismatch: records={record_count}, embeddings={self.sql_embeddings.shape[0]}")


class FewShotRetriever:
    def __init__(self, index: FewShotIndex, embedding_config: Any, embedding_batch_size: int = 128) -> None:
        if embedding_batch_size < 1:
            raise ValueError(f"embedding_batch_size must be >= 1, got {embedding_batch_size}")
        self.index = index
        self.embedding_config = embedding_config
        self.embedding_batch_size = embedding_batch_size
        self._embedding_function = None

    @classmethod
    def from_index_path(
        cls,
        index_path: str | Path,
        embedding_config: Any,
        embedding_batch_size: int = 128,
        mmap_mode: Optional[str] = "r",
        similarity_device: str = "cpu",
    ) -> "FewShotRetriever":
        return cls(
            index=FewShotIndex.load(
                index_path=index_path,
                mmap_mode=mmap_mode,
                similarity_device=similarity_device,
            ),
            embedding_config=embedding_config,
            embedding_batch_size=embedding_batch_size,
        )

    def retrieve_by_texts(
        self,
        masked_question: str,
        masked_sql: Optional[str] = None,
        top_k: int = 5,
        question_weight: float = 0.5,
        sql_weight: float = 0.5,
        exclude_example_ids: Optional[Iterable[str]] = None,
        exclude_db_ids: Optional[Iterable[str]] = None,
    ) -> List[FewShotRetrievalResult]:
        texts = [masked_question]
        if masked_sql:
            texts.append(masked_sql)

        embeddings = self._embed_texts(texts)
        question_embedding = embeddings[0]
        sql_embedding = embeddings[1] if masked_sql else None
        return self.index.retrieve_by_embeddings(
            question_embedding=question_embedding,
            sql_embedding=sql_embedding,
            top_k=top_k,
            question_weight=question_weight,
            sql_weight=sql_weight,
            exclude_example_ids=exclude_example_ids,
            exclude_db_ids=exclude_db_ids,
        )

    def _embed_texts(self, texts: List[str]) -> List[np.ndarray]:
        embedding_function = self._get_embedding_function()
        embeddings: List[np.ndarray] = []
        for start in range(0, len(texts), self.embedding_batch_size):
            batch = texts[start : start + self.embedding_batch_size]
            batch_embeddings = embedding_function(batch)
            embeddings.extend(np.asarray(embedding, dtype=np.float32) for embedding in batch_embeddings)
        return embeddings

    def _get_embedding_function(self):
        if self._embedding_function is None:
            self._embedding_function = get_embedding_function(
                model_name_or_path=self.embedding_config.embedding_model_name_or_path,
                api_type=self.embedding_config.api_type,
                use_qwen3_embedding=self.embedding_config.use_qwen3_embedding,
                local_files_only=self.embedding_config.local_files_only,
                normalize_embeddings=self.embedding_config.normalize_embeddings,
                base_url=self.embedding_config.base_url,
                api_key=self.embedding_config.api_key,
                embedding_device=self.embedding_config.embedding_device,
            )
        return self._embedding_function


def retrieval_results_to_examples(results: Iterable[FewShotRetrievalResult]) -> List[FewShotExample]:
    return [result.to_few_shot_example() for result in results]


def _load_records(examples_path: Path) -> List[FewShotRecord]:
    records: List[FewShotRecord] = []
    with open(examples_path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {examples_path}:{line_number}: {exc}") from exc
            _validate_record(record=record, examples_path=examples_path, line_number=line_number)
            records.append(record)
    return records


def _validate_record(record: Any, examples_path: Path, line_number: int) -> None:
    if not isinstance(record, dict):
        raise ValueError(f"Expected object in {examples_path}:{line_number}")
    for field_name in ("example_id", "db_id", "question", "sql", "masked_question", "masked_sql"):
        value = record.get(field_name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing or empty {field_name} in {examples_path}:{line_number}")


def _resolve_similarity_device(device: str) -> Optional[str]:
    requested_device = (device or "cpu").strip().lower()
    if requested_device == "cpu":
        return None
    if torch is None:
        logger.warning(f"Requested few-shot similarity_device={device}, but torch is unavailable; falling back to CPU")
        return None

    if requested_device == "auto":
        requested_device = "cuda" if torch.cuda.is_available() else "cpu"
    if requested_device == "cpu":
        return None

    resolved_device = torch.device(requested_device)
    if resolved_device.type == "cuda":
        if not torch.cuda.is_available():
            logger.warning(f"Requested few-shot similarity_device={device}, but CUDA is unavailable; falling back to CPU")
            return None
        if resolved_device.index is not None and resolved_device.index >= torch.cuda.device_count():
            raise ValueError(
                f"Requested few-shot similarity_device={device}, but only "
                f"{torch.cuda.device_count()} CUDA device(s) are visible"
            )
        return str(resolved_device)

    raise ValueError(f"Unsupported few-shot similarity_device={device}; expected cpu, auto, cuda, or cuda:N")


def _resolve_weights(question_weight: float, sql_weight: float, has_sql_embedding: bool) -> tuple[float, float]:
    if question_weight < 0 or sql_weight < 0:
        raise ValueError(f"Retrieval weights must be non-negative, got question={question_weight}, sql={sql_weight}")
    if not has_sql_embedding:
        if question_weight <= 0:
            raise ValueError("question_weight must be > 0 when sql_embedding is not provided")
        return 1.0, 0.0

    total = question_weight + sql_weight
    if total <= 0:
        raise ValueError("question_weight and sql_weight cannot both be 0")
    return question_weight / total, sql_weight / total


def _normalize_matrix(matrix: np.ndarray) -> np.ndarray:
    matrix = np.asarray(matrix, dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


def _normalize_vector(vector: np.ndarray) -> np.ndarray:
    if vector.ndim != 1:
        raise ValueError(f"Embedding vector must be 1D, got shape={vector.shape}")
    norm = float(np.linalg.norm(vector))
    if norm == 0:
        return vector
    return vector / norm


def _to_string_set(values: Optional[Iterable[str]]) -> Set[str]:
    if values is None:
        return set()
    return {str(value) for value in values}
