#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

LITE_CONFIG="${LITE_CONFIG:-config/local/config-spider2-lite.toml}"
SNOW_CONFIG="${SNOW_CONFIG:-config/local/config-spider2-snow.toml}"

source "${PROJECT_ROOT}/script/run-command-utils.sh"

case "${1:-help}" in
  lite)
    run_pipeline_for_config "${LITE_CONFIG}"
    ;;

  eval-lite)
    eval_sql "${LITE_CONFIG}" "${MAX_WORKERS:-8}"
    ;;

  export-lite)
    export_sql "${LITE_CONFIG}"
    ;;

  snow)
    run_pipeline_for_config "${SNOW_CONFIG}"
    ;;

  eval-snow)
    eval_sql "${SNOW_CONFIG}" "${MAX_WORKERS:-8}"
    ;;

  export-snow)
    export_sql "${SNOW_CONFIG}"
    ;;

  help|*)
    cat <<EOF
Usage:
  bash script/spider2-run-commands.sh lite
  bash script/spider2-run-commands.sh eval-lite
  bash script/spider2-run-commands.sh export-lite
  bash script/spider2-run-commands.sh snow
  bash script/spider2-run-commands.sh eval-snow
  bash script/spider2-run-commands.sh export-snow

Config overrides:
  LITE_CONFIG=path/to/config-spider2-lite.toml SNOW_CONFIG=path/to/config-spider2-snow.toml bash script/spider2-run-commands.sh lite

Default configs:
  LITE_CONFIG=${LITE_CONFIG}
  SNOW_CONFIG=${SNOW_CONFIG}

Recommended order for Spider2:
  1. lite or snow
  2. eval-lite or eval-snow
  3. export-lite or export-snow

Spider2 currently skips dynamic few-shot preparation because it has no supported
training index path in this pipeline.
EOF
    ;;
esac
