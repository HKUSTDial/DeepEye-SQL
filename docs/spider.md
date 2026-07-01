# Run Spider From Scratch

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

This downloads and prepares Spider under `data/spider/`. The test split loader expects:

```text
data/spider/test.json
data/spider/test_database/<db_id>/<db_id>.sqlite
```

The same Spider package also contains training data used for dynamic few-shot indexing:

```text
data/spider/train_spider.json
data/spider/train_others.json
data/spider/database/<db_id>/<db_id>.sqlite
```

## 3. Create Local Config

Choose a model template. For example, Qwen3.6-27B:

```bash
mkdir -p config/local/Qwen3.6-27B
cp config/template/Qwen3.6-27B/config-spider-test.toml config/local/Qwen3.6-27B/config-spider-test.toml
```

Or use Qwen3-Coder-30B-A3B-Instruct:

```bash
mkdir -p config/local/Qwen3-Coder-30B-A3B-Instruct
cp config/template/Qwen3-Coder-30B-A3B-Instruct/config-spider-test.toml config/local/Qwen3-Coder-30B-A3B-Instruct/config-spider-test.toml
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

## 4. Run Test

Set `CONFIG_PATH` and run the Spider wrapper:

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-spider-test.toml
bash script/run_spider.sh run
bash script/run_spider.sh inspect
bash script/run_spider.sh eval
bash script/run_spider.sh export
```

For another model config directory, point `CONFIG_PATH` at that local config:

```bash
export CONFIG_PATH=config/local/Qwen3-Coder-30B-A3B-Instruct/config-spider-test.toml
bash script/run_spider.sh run
```

`run` automatically builds the few-shot training index if it is missing. Use `rebuild-index` only when you intentionally want to overwrite the index:

```bash
CONFIG_PATH=config/local/Qwen3.6-27B/config-spider-test.toml bash script/run_spider.sh rebuild-index
```

The exported predictions are written under the run directory, usually:

```text
workspace/runs/spider-test/predictions.json
```
