#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

DEV_CONFIG="${DEV_CONFIG:-workspace/run_configs/bird-full-dev.toml}"
TEST_CONFIG="${TEST_CONFIG:-workspace/run_configs/bird-full-test.toml}"
LOG_ROOT="${LOG_ROOT:-logs/full_runs}"
UNPROXY=(env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u all_proxy)

mkdir -p "${LOG_ROOT}"

config_value() {
  local config_path="$1"
  local key="$2"
  "${UNPROXY[@]}" uv run python - "${config_path}" "${key}" <<'PY'
import os
import sys

config_path, key = sys.argv[1], sys.argv[2]
os.environ["CONFIG_PATH"] = config_path

from app.config import get_config

cfg = get_config()
values = {
    "dataset.type": cfg.dataset_config.type,
    "few_shot_index.prepared_save_path": cfg.few_shot_index_config.prepared_save_path,
    "sql_selection.save_path": cfg.sql_selection_config.save_path,
}
try:
    print(values[key])
except KeyError as exc:
    raise SystemExit(f"Unsupported config key: {key}") from exc
PY
}

inspect_few_shot() {
  local config_path="$1"
  local input_path
  local output_dir
  input_path="$(config_value "${config_path}" "few_shot_index.prepared_save_path")"
  output_dir="$(dirname "${input_path}")"
  "${UNPROXY[@]}" uv run python runner/inspect_few_shot_preparation.py \
    --config "${config_path}" \
    --input_path "${input_path}" \
    --output_path "${output_dir}/few_shot_preparation_summary.json" \
    --details_output_path "${output_dir}/few_shot_preparation_details.jsonl"
}

export_sql() {
  local config_path="$1"
  local snapshot_path
  local output_dir
  snapshot_path="$(config_value "${config_path}" "sql_selection.save_path")"
  output_dir="$(dirname "${snapshot_path}")"
  "${UNPROXY[@]}" uv run python runner/convert_snapshot_to_sql.py \
    --snapshot_path "${snapshot_path}" \
    --output "${output_dir}/predictions.json"
}

case "${1:-help}" in
  build-index)
    "${UNPROXY[@]}" uv run python runner/build_few_shot_index.py \
      --config "${DEV_CONFIG}"
    ;;

  rebuild-index)
    "${UNPROXY[@]}" uv run python runner/build_few_shot_index.py \
      --config "${DEV_CONFIG}" \
      --force
    ;;

  dev)
    "${UNPROXY[@]}" bash script/run_pipeline.sh "${DEV_CONFIG}"
    ;;

  inspect-dev)
    inspect_few_shot "${DEV_CONFIG}"
    ;;

  eval-dev)
    DEV_SQL_SELECTION_PATH="$(config_value "${DEV_CONFIG}" "sql_selection.save_path")"
    "${UNPROXY[@]}" uv run python runner/evaluation.py \
      --snapshot_path "${DEV_SQL_SELECTION_PATH}" \
      --dataset_type bird \
      --max_workers 32
    ;;

  export-dev)
    export_sql "${DEV_CONFIG}"
    ;;

  test)
    "${UNPROXY[@]}" bash script/run_pipeline.sh "${TEST_CONFIG}"
    ;;

  inspect-test)
    inspect_few_shot "${TEST_CONFIG}"
    ;;

  export-test)
    export_sql "${TEST_CONFIG}"
    ;;

  help|*)
    cat <<EOF
Usage:
  bash script/bird-full-run-commands.sh build-index
  bash script/bird-full-run-commands.sh rebuild-index
  bash script/bird-full-run-commands.sh dev
  bash script/bird-full-run-commands.sh inspect-dev
  bash script/bird-full-run-commands.sh eval-dev
  bash script/bird-full-run-commands.sh export-dev
  bash script/bird-full-run-commands.sh test
  bash script/bird-full-run-commands.sh inspect-test
  bash script/bird-full-run-commands.sh export-test

Config overrides:
  DEV_CONFIG=path/to/bird-dev.toml TEST_CONFIG=path/to/bird-test.toml \
    bash script/bird-full-run-commands.sh dev

Default configs:
  DEV_CONFIG=${DEV_CONFIG}
  TEST_CONFIG=${TEST_CONFIG}

Recommended order for BIRD dev + test:
  1. dev          # automatically builds the few-shot train index if missing
  2. inspect-dev
  3. eval-dev
  4. export-dev
  5. test         # reuses the same few-shot train index
  6. inspect-test
  7. export-test

Use rebuild-index only when the few-shot index directory is incomplete/corrupt or
when you intentionally want to overwrite an existing index.
EOF
    ;;
esac
