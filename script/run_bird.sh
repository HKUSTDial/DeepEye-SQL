#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

source "${PROJECT_ROOT}/script/run-command-utils.sh"

require_config_path() {
  local command_name="$1"
  if [ -z "${CONFIG_PATH:-}" ]; then
    echo "Error: CONFIG_PATH is required for '${command_name}'." >&2
    echo "Example:" >&2
    echo "  CONFIG_PATH=config/local/<model>/config-bird-dev.toml bash script/run_bird.sh ${command_name}" >&2
    exit 2
  fi
}

case "${1:-help}" in
  build-index)
    require_config_path "build-index"
    build_few_shot_index "${CONFIG_PATH}"
    ;;

  rebuild-index)
    require_config_path "rebuild-index"
    build_few_shot_index "${CONFIG_PATH}" --force
    ;;

  run)
    require_config_path "run"
    run_pipeline_for_config "${CONFIG_PATH}"
    ;;

  inspect)
    require_config_path "inspect"
    inspect_few_shot "${CONFIG_PATH}"
    ;;

  eval)
    require_config_path "eval"
    eval_sql "${CONFIG_PATH}" "${MAX_WORKERS:-32}"
    ;;

  export)
    require_config_path "export"
    export_sql "${CONFIG_PATH}"
    ;;

  help|*)
    cat <<EOF
Usage:
  CONFIG_PATH=path/to/config-bird-dev.toml bash script/run_bird.sh build-index
  CONFIG_PATH=path/to/config-bird-dev.toml bash script/run_bird.sh rebuild-index
  CONFIG_PATH=path/to/config-bird-dev.toml bash script/run_bird.sh run
  CONFIG_PATH=path/to/config-bird-dev.toml bash script/run_bird.sh inspect
  CONFIG_PATH=path/to/config-bird-dev.toml bash script/run_bird.sh eval
  CONFIG_PATH=path/to/config-bird-dev.toml bash script/run_bird.sh export

Commands:
  run            Run the full pipeline for CONFIG_PATH.
  inspect        Inspect dynamic few-shot preparation for CONFIG_PATH.
  eval           Evaluate selected SQL for CONFIG_PATH when gold labels are available.
  export         Export selected SQL predictions for CONFIG_PATH.
  build-index    Build the few-shot training index used by CONFIG_PATH.
  rebuild-index  Force rebuild the few-shot training index used by CONFIG_PATH.

Recommended BIRD dev order:
  1. CONFIG_PATH=... bash script/run_bird.sh run
  2. CONFIG_PATH=... bash script/run_bird.sh inspect
  3. CONFIG_PATH=... bash script/run_bird.sh eval
  4. CONFIG_PATH=... bash script/run_bird.sh export

Recommended BIRD test order:
  1. CONFIG_PATH=... bash script/run_bird.sh run
  2. CONFIG_PATH=... bash script/run_bird.sh inspect
  3. CONFIG_PATH=... bash script/run_bird.sh export

BIRD test has no public gold labels, so skip eval for official test runs.
Use rebuild-index only when the few-shot index directory is incomplete/corrupt or
when you intentionally want to overwrite an existing index.
EOF
    ;;
esac
