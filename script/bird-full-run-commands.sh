#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

DEV_CONFIG="${DEV_CONFIG:-config/local/config-bird-dev.toml}"
TEST_CONFIG="${TEST_CONFIG:-config/local/config-bird-test.toml}"

source "${PROJECT_ROOT}/script/run-command-utils.sh"

case "${1:-help}" in
  build-index)
    build_few_shot_index "${DEV_CONFIG}"
    ;;

  rebuild-index)
    build_few_shot_index "${DEV_CONFIG}" --force
    ;;

  dev)
    run_pipeline_for_config "${DEV_CONFIG}"
    ;;

  inspect-dev)
    inspect_few_shot "${DEV_CONFIG}"
    ;;

  eval-dev)
    eval_sql "${DEV_CONFIG}" "${MAX_WORKERS:-32}"
    ;;

  export-dev)
    export_sql "${DEV_CONFIG}"
    ;;

  test)
    run_pipeline_for_config "${TEST_CONFIG}"
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
  DEV_CONFIG=path/to/bird-dev.toml TEST_CONFIG=path/to/bird-test.toml bash script/bird-full-run-commands.sh dev

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
