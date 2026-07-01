#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

TEST_CONFIG="${TEST_CONFIG:-config/local/Qwen3.6-27B/config-spider-test.toml}"

source "${PROJECT_ROOT}/script/run-command-utils.sh"

case "${1:-help}" in
  build-index)
    build_few_shot_index "${TEST_CONFIG}"
    ;;

  rebuild-index)
    build_few_shot_index "${TEST_CONFIG}" --force
    ;;

  test)
    run_pipeline_for_config "${TEST_CONFIG}"
    ;;

  inspect-test)
    inspect_few_shot "${TEST_CONFIG}"
    ;;

  eval-test)
    eval_sql "${TEST_CONFIG}" "${MAX_WORKERS:-32}"
    ;;

  export-test)
    export_sql "${TEST_CONFIG}"
    ;;

  help|*)
    cat <<EOF
Usage:
  bash script/run_spider.sh build-index
  bash script/run_spider.sh rebuild-index
  bash script/run_spider.sh test
  bash script/run_spider.sh inspect-test
  bash script/run_spider.sh eval-test
  bash script/run_spider.sh export-test

Config overrides:
  TEST_CONFIG=path/to/config-spider-test.toml bash script/run_spider.sh test

Default config:
  TEST_CONFIG=${TEST_CONFIG}

Recommended order for Spider test:
  1. test          # automatically builds the few-shot train index if missing
  2. inspect-test
  3. eval-test
  4. export-test

Use rebuild-index only when the few-shot index directory is incomplete/corrupt or
when you intentionally want to overwrite an existing index.
EOF
    ;;
esac
