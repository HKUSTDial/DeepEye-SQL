#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "${PROJECT_ROOT}"

LOG_ROOT="${LOG_ROOT:-logs/full_runs}"

mkdir -p "${LOG_ROOT}"

config_value() {
  local config_path="$1"
  local key="$2"
  uv run python - "${config_path}" "${key}" <<'PY'
import os
import sys

config_path, key = sys.argv[1], sys.argv[2]
os.environ["CONFIG_PATH"] = config_path

from app.config import get_config

cfg = get_config()
values = {
    "dataset.type": cfg.dataset_config.type,
    "dataset.split": cfg.dataset_config.split,
    "few_shot_index.prepared_save_path": cfg.few_shot_index_config.prepared_save_path,
    "sql_selection.save_path": cfg.sql_selection_config.save_path,
}
try:
    print(values[key])
except KeyError as exc:
    raise SystemExit(f"Unsupported config key: {key}") from exc
PY
}

build_few_shot_index() {
  local config_path="$1"
  shift || true
  uv run python runner/build_few_shot_index.py \
    --config "${config_path}" \
    "$@"
}

run_pipeline_for_config() {
  local config_path="$1"
  bash script/run_pipeline.sh "${config_path}"
}

inspect_few_shot() {
  local config_path="$1"
  local input_path
  local output_dir
  input_path="$(config_value "${config_path}" "few_shot_index.prepared_save_path")"
  output_dir="$(dirname "${input_path}")"
  uv run python runner/inspect_few_shot_preparation.py \
    --config "${config_path}" \
    --input_path "${input_path}" \
    --output_path "${output_dir}/few_shot_preparation_summary.json" \
    --details_output_path "${output_dir}/few_shot_preparation_details.jsonl"
}

eval_sql() {
  local config_path="$1"
  local max_workers="${2:-}"
  local args=()
  if [ -n "${max_workers}" ]; then
    args+=(--max_workers "${max_workers}")
  fi
  CONFIG_PATH="${config_path}" uv run python runner/evaluation.py "${args[@]}"
}

export_sql() {
  local config_path="$1"
  local snapshot_path
  local output_dir
  local dataset_type
  local output_path
  snapshot_path="$(config_value "${config_path}" "sql_selection.save_path")"
  output_dir="$(dirname "${snapshot_path}")"
  dataset_type="$(config_value "${config_path}" "dataset.type")"
  if [ "${dataset_type}" = "spider2" ]; then
    output_path="${output_dir}/sql_output"
  else
    output_path="${output_dir}/predictions.json"
  fi
  CONFIG_PATH="${config_path}" uv run python runner/convert_snapshot_to_sql.py \
    --snapshot_path "${snapshot_path}" \
    --output "${output_path}"
}
