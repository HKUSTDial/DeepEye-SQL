# Run BIRD From Scratch

## 1. Prepare Environment

Install project dependencies from the repository root:

```bash
uv sync
```

Start your OpenAI-compatible LLM endpoint and embedding endpoint before running the pipeline.

## 2. Prepare Data

Run the dataset helper:

```bash
bash script/download_dataset.sh
```

This prepares:

- BIRD dev data under `data/bird/dev/`
- BIRD train databases from the official BIRD train package under `data/bird/train/train_databases/`
- Cleaned BIRD train question/SQL labels from `birdsql/bird23-train-filtered`, converted to `data/bird/train/train.json`

For BIRD test, place the official test files under the same layout expected by the loader:

```text
data/bird/test/test.json
data/bird/test/test_databases/<db_id>/<db_id>.sqlite
```

## 3. Create Local Configs

Choose a model template. For example, Qwen3.6-27B:

```bash
mkdir -p config/local/Qwen3.6-27B
cp config/template/Qwen3.6-27B/config-bird-dev.toml config/local/Qwen3.6-27B/config-bird-dev.toml
cp config/template/Qwen3.6-27B/config-bird-test.toml config/local/Qwen3.6-27B/config-bird-test.toml
```

Or use Qwen3-Coder-30B-A3B-Instruct:

```bash
mkdir -p config/local/Qwen3-Coder-30B-A3B-Instruct
cp config/template/Qwen3-Coder-30B-A3B-Instruct/config-bird-dev.toml config/local/Qwen3-Coder-30B-A3B-Instruct/config-bird-dev.toml
cp config/template/Qwen3-Coder-30B-A3B-Instruct/config-bird-test.toml config/local/Qwen3-Coder-30B-A3B-Instruct/config-bird-test.toml
```

Edit only the fields that depend on your machine or endpoints:

```toml
[llm_profiles.<profile_name>]
base_url = "your-llm-model-base-url"
api_key = "your-llm-api-key"

[embedding]
base_url = "your-embedding-model-base-url"
api_key = "your-embedding-api-key"

[few_shot_index]
similarity_device = "cuda:0"  # or cpu, auto, cuda:N

[value_retrieval]
local_index_device = "cuda:0"  # or cpu, auto, cuda:N
```

## 4. Run Dev

Set `CONFIG_PATH` and run the BIRD wrapper:

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-dev.toml
bash script/run_bird.sh run
bash script/run_bird.sh inspect
bash script/run_bird.sh eval
bash script/run_bird.sh export
```

For another model config directory, point `CONFIG_PATH` at that local config:

```bash
export CONFIG_PATH=config/local/Qwen3-Coder-30B-A3B-Instruct/config-bird-dev.toml
bash script/run_bird.sh run
```

`run` automatically builds the few-shot training index if it is missing. Use `rebuild-index` only when you intentionally want to overwrite the index:

```bash
CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-dev.toml bash script/run_bird.sh rebuild-index
```

## 5. Run Test

Use the same commands with the test config:

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-test.toml
bash script/run_bird.sh run
bash script/run_bird.sh inspect
bash script/run_bird.sh export
```

BIRD test has no public gold labels, so skip `eval` for official test runs. The exported predictions are written under the run directory, usually:

```text
workspace/runs/bird-test/predictions.json
```
