from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
import json
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Tuple

import numpy as np
import torch


LOCAL_INDEX_DIRNAME = "local_index"
LOCAL_INDEX_MANIFEST_FILENAME = "manifest.json"
LOCAL_INDEX_COLUMNS_DIRNAME = "columns"


def get_local_index_path(vector_db_path: str | Path) -> Path:
    return Path(vector_db_path) / LOCAL_INDEX_DIRNAME


def _column_key(table_name: str, column_name: str) -> str:
    return f"{table_name}\t{column_name}"


def _normalize_embeddings(embeddings: np.ndarray) -> np.ndarray:
    if embeddings.size == 0:
        return embeddings.astype(np.float32, copy=False)

    embeddings = embeddings.astype(np.float32, copy=False)
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms = np.clip(norms, a_min=1e-12, a_max=None)
    return embeddings / norms


def write_local_index_column(
    local_index_path: str | Path,
    table_name: str,
    column_name: str,
    documents: List[str],
    embeddings: List[List[float]],
) -> Dict[str, Any] | None:
    if not documents:
        return None

    local_index_path = Path(local_index_path)
    columns_dir = local_index_path / LOCAL_INDEX_COLUMNS_DIRNAME
    columns_dir.mkdir(parents=True, exist_ok=True)

    column_key = _column_key(table_name, column_name)
    file_stem = sha1(column_key.encode("utf-8")).hexdigest()
    embeddings_filename = f"{file_stem}.embeddings.npy"
    documents_filename = f"{file_stem}.documents.json"

    normalized_embeddings = _normalize_embeddings(np.asarray(embeddings, dtype=np.float32))

    np.save(columns_dir / embeddings_filename, normalized_embeddings)
    with (columns_dir / documents_filename).open("w", encoding="utf-8") as file_obj:
        json.dump(documents, file_obj, ensure_ascii=True)

    return {
        "table_name": table_name,
        "column_name": column_name,
        "column_key": column_key,
        "embeddings_file": str(Path(LOCAL_INDEX_COLUMNS_DIRNAME) / embeddings_filename),
        "documents_file": str(Path(LOCAL_INDEX_COLUMNS_DIRNAME) / documents_filename),
        "count": len(documents),
        "embedding_dim": int(normalized_embeddings.shape[1]) if normalized_embeddings.ndim == 2 else 0,
    }


def write_local_index_manifest(
    local_index_path: str | Path,
    column_entries: List[Dict[str, Any]],
) -> None:
    local_index_path = Path(local_index_path)
    local_index_path.mkdir(parents=True, exist_ok=True)

    manifest = {
        "version": 1,
        "metric": "cosine",
        "columns": sorted(column_entries, key=lambda entry: entry["column_key"]),
    }
    with (local_index_path / LOCAL_INDEX_MANIFEST_FILENAME).open("w", encoding="utf-8") as file_obj:
        json.dump(manifest, file_obj, ensure_ascii=True, indent=2)


def local_index_exists(vector_db_path: str | Path) -> bool:
    return (get_local_index_path(vector_db_path) / LOCAL_INDEX_MANIFEST_FILENAME).exists()


@dataclass
class _LoadedColumn:
    embeddings: torch.Tensor
    documents: List[str]


class LocalValueIndex:
    def __init__(self, index_path: str | Path, device: str = "auto"):
        self._index_path = Path(index_path)
        self._manifest_path = self._index_path / LOCAL_INDEX_MANIFEST_FILENAME
        if not self._manifest_path.exists():
            raise FileNotFoundError(f"Local index manifest not found: {self._manifest_path}")

        with self._manifest_path.open("r", encoding="utf-8") as file_obj:
            manifest = json.load(file_obj)

        self._columns = {
            entry["column_key"]: entry
            for entry in manifest.get("columns", [])
        }
        self._device = self._resolve_device(device)
        self._column_cache: Dict[str, _LoadedColumn] = {}
        self._cache_lock = Lock()

    @staticmethod
    def _resolve_device(device: str) -> torch.device:
        normalized_device = device.lower()
        if normalized_device == "auto":
            normalized_device = "cuda" if torch.cuda.is_available() else "cpu"

        if normalized_device.startswith("cuda") and not torch.cuda.is_available():
            normalized_device = "cpu"

        return torch.device(normalized_device)

    @property
    def device(self) -> str:
        return self._device.type

    def _load_column(self, table_name: str, column_name: str) -> _LoadedColumn:
        column_key = _column_key(table_name, column_name)
        cached_column = self._column_cache.get(column_key)
        if cached_column is not None:
            return cached_column

        with self._cache_lock:
            cached_column = self._column_cache.get(column_key)
            if cached_column is not None:
                return cached_column

            entry = self._columns.get(column_key)
            if entry is None:
                raise KeyError(f"Local index column not found: {table_name}.{column_name}")

            embeddings = np.load(self._index_path / entry["embeddings_file"])
            with (self._index_path / entry["documents_file"]).open("r", encoding="utf-8") as file_obj:
                documents = json.load(file_obj)

            tensor = torch.from_numpy(embeddings)
            if self._device.type == "cuda":
                tensor = tensor.to(self._device, non_blocking=True)
            cached_column = _LoadedColumn(embeddings=tensor, documents=documents)
            self._column_cache[column_key] = cached_column
            return cached_column

    def retrieve_values_for_column(
        self,
        query_embeddings: List[List[float]],
        table_name: str,
        column_name: str,
        n_results: int,
        lower_meta_data: bool,
    ) -> Dict[str, Any]:
        lookup_table_name = table_name.lower() if lower_meta_data else table_name
        lookup_column_name = column_name.lower() if lower_meta_data else column_name

        if not query_embeddings:
            return {
                "table_name": lookup_table_name,
                "column_name": lookup_column_name,
                "values": [],
            }

        try:
            loaded_column = self._load_column(lookup_table_name, lookup_column_name)
        except KeyError:
            return {
                "table_name": lookup_table_name,
                "column_name": lookup_column_name,
                "values": [],
            }
        column_embeddings = loaded_column.embeddings
        if column_embeddings.shape[0] == 0:
            return {
                "table_name": lookup_table_name,
                "column_name": lookup_column_name,
                "values": [],
            }

        query_array = np.asarray(query_embeddings, dtype=np.float32)
        query_tensor = torch.as_tensor(query_array, dtype=torch.float32, device=self._device)
        if query_tensor.ndim == 1:
            query_tensor = query_tensor.unsqueeze(0)
        query_tensor = torch.nn.functional.normalize(query_tensor, p=2, dim=1, eps=1e-12)

        top_k = min(n_results, column_embeddings.shape[0])
        similarities = query_tensor @ column_embeddings.T
        top_similarities, top_indices = torch.topk(similarities, k=top_k, dim=1)

        similarity_rows = top_similarities.detach().cpu().tolist()
        index_rows = top_indices.detach().cpu().tolist()

        values: List[Tuple[str, float]] = []
        for similarity_row, index_row in zip(similarity_rows, index_rows):
            for similarity, index in zip(similarity_row, index_row):
                values.append((loaded_column.documents[index], float(1.0 - similarity)))

        seen_values = set()
        top_values = []
        for value, distance in sorted(values, key=lambda item: item[1]):
            if value in seen_values:
                continue
            seen_values.add(value)
            top_values.append({"value": value, "distance": distance})
            if len(top_values) >= n_results:
                break

        return {
            "table_name": lookup_table_name,
            "column_name": lookup_column_name,
            "values": top_values,
        }
