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
    echo "  CONFIG_PATH=config/local/<model>/config-spider2-lite.toml bash script/run_spider2.sh ${command_name}" >&2
    exit 2
  fi
}

case "${1:-help}" in
  run)
    require_config_path "run"
    run_pipeline_for_config "${CONFIG_PATH}"
    ;;

  eval)
    require_config_path "eval"
    eval_sql "${CONFIG_PATH}" "${MAX_WORKERS:-8}"
    ;;

  export)
    require_config_path "export"
    export_sql "${CONFIG_PATH}"
    ;;

  help|*)
    cat <<EOF
Usage:
  CONFIG_PATH=path/to/config-spider2-lite.toml bash script/run_spider2.sh run
  CONFIG_PATH=path/to/config-spider2-lite.toml bash script/run_spider2.sh eval
  CONFIG_PATH=path/to/config-spider2-lite.toml bash script/run_spider2.sh export

Commands:
  run     Run the full pipeline for CONFIG_PATH.
  eval    Evaluate selected SQL for CONFIG_PATH.
  export  Export selected SQL files for CONFIG_PATH.

Recommended Spider2 order:
  1. CONFIG_PATH=... bash script/run_spider2.sh run
  2. CONFIG_PATH=... bash script/run_spider2.sh eval
  3. CONFIG_PATH=... bash script/run_spider2.sh export

Use a lite config for Spider2-Lite and a snow config for Spider2-Snow.
Spider2 currently skips dynamic few-shot preparation because it has no supported
training index path in this pipeline.
EOF
    ;;
esac
