# BIRD-Test Official Evaluation Reproduction Guide

This document describes how to reproduce the DeepEye-SQL BIRD-test submission from a clean machine. The setup uses fully local model serving with vLLM:

- 4 GPUs for the chat model: `Qwen3.6-27B`
- 1 GPU for the embedding model: `Qwen3-Embedding-0.6B`
- DeepEye-SQL calls both services through OpenAI-compatible local HTTP endpoints.

The repository already contains the BIRD-test runtime configuration at:

```text
config/config-bird-test-official.toml
```

No manual TOML editing is required for the run described below.

## 1. vLLM Setup, Model Download, And Model Serving

### 1.1 Hardware Assumption

The commands below assume one server with at least 5 visible CUDA GPUs:

```text
GPU 0,1,2,3  -> Qwen3.6-27B vLLM server, tensor parallel size 4, port 9999
GPU 4        -> Qwen3-Embedding-0.6B vLLM server, port 8000
```

Check the devices:

```bash
nvidia-smi
```

### 1.2 Install vLLM Environment

Use a separate Python environment for serving models:

```bash
python3 -m venv ~/venvs/vllm
source ~/venvs/vllm/bin/activate
python -m pip install --upgrade pip
pip install vllm modelscope
```

### 1.3 Download Models

The commands below place models under fixed paths used by the launch commands.

```bash
mkdir -p /data/models

modelscope download \
  --model Qwen/Qwen3.6-27B \
  --local_dir /data/models/Qwen3.6-27B

modelscope download \
  --model Qwen/Qwen3-Embedding-0.6B \
  --local_dir /data/models/Qwen3-Embedding-0.6B
```

If the models are already available locally, place or symlink them to:

```text
/data/models/Qwen3.6-27B
/data/models/Qwen3-Embedding-0.6B
```

### 1.4 Start The Qwen3.6-27B Chat Server

Open terminal 1:

```bash
source ~/venvs/vllm/bin/activate

CUDA_VISIBLE_DEVICES=0,1,2,3 vllm serve /data/models/Qwen3.6-27B \
  --served-model-name Qwen3.6-27B \
  --host 0.0.0.0 \
  --port 9999 \
  --tensor-parallel-size 4 \
  --max-model-len 65536 \
  --gpu-memory-utilization 0.90 \
  --trust-remote-code
```

The DeepEye-SQL config will call this endpoint as:

```text
http://127.0.0.1:9999/v1
```

### 1.5 Start The Qwen3-Embedding-0.6B Server

Open terminal 2:

```bash
source ~/venvs/vllm/bin/activate

CUDA_VISIBLE_DEVICES=4 vllm serve /data/models/Qwen3-Embedding-0.6B \
  --task embed \
  --served-model-name Qwen3-Embedding-0.6B \
  --host 0.0.0.0 \
  --port 8000 \
  --gpu-memory-utilization 0.90 \
  --trust-remote-code
```

The DeepEye-SQL config will call this endpoint as:

```text
http://127.0.0.1:8000/v1
```

### 1.6 Smoke Test The Local Endpoints

Run these checks from a third terminal:

```bash
curl http://127.0.0.1:9999/v1/models
curl http://127.0.0.1:8000/v1/models
```

Expected result: each command returns a JSON response listing the served model.

## 2. DeepEye-SQL Setup And BIRD-Test Run

### 2.1 Clone The Repository

```bash
git clone https://github.com/HKUSTDial/DeepEye-SQL.git
cd DeepEye-SQL
```

### 2.2 Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
```

### 2.3 Install The Project Environment

DeepEye-SQL requires Python `>=3.12`. `uv sync` creates the project environment from `pyproject.toml` and `uv.lock`.

```bash
uv sync
```

### 2.4 Prepare The BIRD-Test Data Directory

Place the official BIRD-test files under `data/bird/test` with this exact layout:

```text
data/
└── bird/
    └── test/
        ├── test.json
        └── test_databases/
            ├── <database_id_1>/
            │   └── <database_id_1>.sqlite
            ├── <database_id_2>/
            │   └── <database_id_2>.sqlite
            └── ...
```

The pipeline reads:

```text
data/bird/test/test.json
data/bird/test/test_databases/<database_id>/<database_id>.sqlite
```

If the official package is unpacked with different directory names, copy or symlink the files to the layout above before running the pipeline.

### 2.5 Use The Ready-To-Run BIRD-Test Config

The provided config is:

```text
config/config-bird-test-official.toml
```

It is already configured for:

- BIRD test split
- local Qwen3.6-27B chat endpoint on `http://127.0.0.1:9999/v1`
- local Qwen3-Embedding-0.6B embedding endpoint on `http://127.0.0.1:8000/v1`
- local vector index backend
- CPU local-index retrieval inside the DeepEye-SQL process, so the 5 GPUs remain dedicated to vLLM servers

Set the config path:

```bash
export CONFIG_PATH=config/config-bird-test-official.toml
```

### 2.6 Run The Full Pipeline

Run all stages from preprocessing to final SQL selection:

```bash
bash script/run_pipeline.sh config/config-bird-test-official.toml
```

This executes the following stages:

```text
1. preprocess_dataset
2. create_vector_db_parallel
3. run_value_retrieval
4. run_schema_linking
5. run_sql_generation
6. run_sql_revision
7. run_sql_selection
```

Logs are written to:

```text
logs/pipeline_<timestamp>.log
```

Intermediate snapshots are written under:

```text
workspace/dataset/bird/test.snapshot
workspace/value_retrieval/bird/test.snapshot
workspace/schema_linking/bird/test.snapshot
workspace/sql_generation/bird/test.snapshot
workspace/sql_revision/bird/test.snapshot
workspace/sql_selection/bird/test.snapshot
```

### 2.7 Resume From A Specific Stage

If the full script stops after a completed stage, rerun only the remaining stage commands. Keep the same `CONFIG_PATH`:

```bash
export CONFIG_PATH=config/config-bird-test-official.toml

uv run runner/preprocess_dataset.py
uv run runner/create_vector_db_parallel.py
uv run runner/run_value_retrieval.py
uv run runner/run_schema_linking.py
uv run runner/run_sql_generation.py
uv run runner/run_sql_revision.py
uv run runner/run_sql_selection.py
```

Each stage writes a structured snapshot and can be inspected independently.

### 2.8 Convert The Final Snapshot To BIRD Submission JSON

After SQL selection completes, convert the final snapshot:

```bash
mkdir -p results

uv run runner/convert_snapshot_to_sql.py \
  --snapshot_path workspace/sql_selection/bird/test.snapshot \
  --output results/bird-test-deepeye-sql-qwen3.6-27b.json \
  --format json
```

The output file is:

```text
results/bird-test-deepeye-sql-qwen3.6-27b.json
```

The JSON format is:

```json
{
  "0": "SELECT ...",
  "1": "SELECT ..."
}
```

where each key is the BIRD `question_id` and each value is the selected SQL string.

### 2.9 Final Artifacts To Submit Or Archive

The main official prediction file is:

```text
results/bird-test-deepeye-sql-qwen3.6-27b.json
```

For reproducibility, also archive:

```text
config/config-bird-test-official.toml
workspace/config/config-bird-test-official.toml
logs/pipeline_<timestamp>.log
workspace/sql_selection/bird/test.snapshot
workspace/sql_selection/bird/test.snapshot.data/items.jsonl
```

## Troubleshooting

### The config fails with an invalid BIRD split

Use the repository version that includes `config/config-bird-test-official.toml` and supports `bird/test` in `app/config/config.py`.

### The LLM server rejects requests with `n > 1`

The provided config sets:

```toml
n_call_strategy = "split"
```

This makes DeepEye-SQL request multiple candidates through repeated `n=1` calls, which is compatible with providers or local servers that do not allow `n > 1`.

### Vector DB creation is slow

BIRD-test vector DB construction scans SQLite values and calls the embedding endpoint. The provided config uses:

```toml
db_parallel = 4
column_parallel = 8
```

If the embedding server becomes the bottleneck, reduce these two values in a new copied config and rerun `runner/create_vector_db_parallel.py`.

### Ports Already In Use

The config expects:

```text
chat model:      127.0.0.1:9999
embedding model: 127.0.0.1:8000
```

Stop the existing process on those ports or launch vLLM on the same ports before running DeepEye-SQL.
