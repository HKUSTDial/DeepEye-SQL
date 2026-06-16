#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${PROJECT_ROOT}/data"
cd "${PROJECT_ROOT}"

BIRD_ROOT="${DATA_ROOT}/bird"
BIRD_DEV_URL="https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip"
BIRD_TRAIN_URL="https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip"
BIRD_TRAIN_FILTERED_URL="https://huggingface.co/datasets/birdsql/bird23-train-filtered/resolve/main/data/train-00000-of-00001.jsonl"
BIRD_TRAIN_COLUMN_MEANING_URL="https://huggingface.co/datasets/birdsql/bird23-train-filtered/resolve/main/train_column_meaning.json"

SPIDER_ROOT="${DATA_ROOT}/spider"
SPIDER_GDRIVE_ID="1403EGqzIDoHMdQF4c9Bkyl7dZLZ5Wt6J"

download_file() {
    local url="$1"
    local output_path="$2"
    local partial_path="${output_path}.part"

    if [[ -s "${output_path}" ]]; then
        echo "Skip download: ${output_path} already exists."
        return
    fi

    mkdir -p "$(dirname "${output_path}")"
    wget -c -O "${partial_path}" "${url}"
    mv "${partial_path}" "${output_path}"
}

extract_zip() {
    local zip_path="$1"
    local output_dir="$2"

    uv run python - "${zip_path}" "${output_dir}" <<'PY'
import sys
import zipfile
from pathlib import Path

zip_path = Path(sys.argv[1])
output_dir = Path(sys.argv[2])
output_dir.mkdir(parents=True, exist_ok=True)

with zipfile.ZipFile(zip_path) as archive:
    archive.extractall(output_dir)
PY
}

prepare_bird_dev() {
    echo "#################### prepare BIRD dev dataset ##########################"
    mkdir -p "${BIRD_ROOT}"

    download_file "${BIRD_DEV_URL}" "${BIRD_ROOT}/dev.zip"

    if [[ ! -f "${BIRD_ROOT}/dev/dev.json" ]]; then
        extract_zip "${BIRD_ROOT}/dev.zip" "${BIRD_ROOT}"
        if [[ -d "${BIRD_ROOT}/dev_20240627" && ! -d "${BIRD_ROOT}/dev" ]]; then
            mv "${BIRD_ROOT}/dev_20240627" "${BIRD_ROOT}/dev"
        fi
    else
        echo "Skip unzip: ${BIRD_ROOT}/dev/dev.json already exists."
    fi

    if [[ -f "${BIRD_ROOT}/dev/dev_databases.zip" && ! -d "${BIRD_ROOT}/dev/dev_databases" ]]; then
        extract_zip "${BIRD_ROOT}/dev/dev_databases.zip" "${BIRD_ROOT}/dev"
    else
        echo "Skip dev database unzip: ${BIRD_ROOT}/dev/dev_databases already exists."
    fi
}

prepare_bird_train() {
    echo "#################### prepare BIRD train dataset ########################"
    mkdir -p "${BIRD_ROOT}"

    download_file "${BIRD_TRAIN_URL}" "${BIRD_ROOT}/train.zip"

    if [[ ! -d "${BIRD_ROOT}/train/train_databases" ]]; then
        extract_zip "${BIRD_ROOT}/train.zip" "${BIRD_ROOT}"
    else
        echo "Skip unzip: ${BIRD_ROOT}/train/train_databases already exists."
    fi

    if [[ -f "${BIRD_ROOT}/train/train_databases.zip" && ! -d "${BIRD_ROOT}/train/train_databases" ]]; then
        extract_zip "${BIRD_ROOT}/train/train_databases.zip" "${BIRD_ROOT}/train"
    else
        echo "Skip train database unzip: ${BIRD_ROOT}/train/train_databases already exists."
    fi

    download_file "${BIRD_TRAIN_FILTERED_URL}" "${BIRD_ROOT}/train/train_filtered.jsonl"
    download_file "${BIRD_TRAIN_COLUMN_MEANING_URL}" "${BIRD_ROOT}/train/train_column_meaning_filtered.json"

    uv run python - "${BIRD_ROOT}/train/train_filtered.jsonl" "${BIRD_ROOT}/train/train.json" "${BIRD_ROOT}/train/train_gold.sql" <<'PY'
import json
import sys
from pathlib import Path

jsonl_path = Path(sys.argv[1])
json_path = Path(sys.argv[2])
gold_path = Path(sys.argv[3])

records = []
with jsonl_path.open("r", encoding="utf-8") as f:
    for question_id, line in enumerate(f):
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        record.setdefault("question_id", question_id)
        record.setdefault("difficulty", "")
        records.append(record)

json_path.write_text(
    json.dumps(records, ensure_ascii=False, indent=2),
    encoding="utf-8",
)

with gold_path.open("w", encoding="utf-8") as f:
    for record in records:
        f.write(f"{record['SQL']}\t{record['db_id']}\n")

print(f"Wrote {len(records)} filtered BIRD train records to {json_path}")
PY
}

prepare_spider() {
    echo "#################### prepare Spider dataset ############################"
    mkdir -p "${SPIDER_ROOT}"

    if [[ -f "${SPIDER_ROOT}/train_spider.json" && -d "${SPIDER_ROOT}/database" ]]; then
        echo "Skip Spider download: ${SPIDER_ROOT} already looks prepared."
        return
    fi

    (
        cd "${SPIDER_ROOT}"
        if [[ ! -s spider_data.zip ]]; then
            uv run gdown "${SPIDER_GDRIVE_ID}"
        else
            echo "Skip download: ${SPIDER_ROOT}/spider_data.zip already exists."
        fi
        extract_zip spider_data.zip "${SPIDER_ROOT}"
        cp -R spider_data/. .
        rm -rf spider_data
    )
}

prepare_bird_dev
prepare_bird_train
prepare_spider
