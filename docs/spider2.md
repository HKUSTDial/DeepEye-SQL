# Run Spider2 From Scratch

## 1. Prepare Environment

Install project dependencies from the repository root:

```bash
uv sync
```

Start your OpenAI-compatible LLM endpoint before running the pipeline.

## 2. Prepare Data

Obtain Spider2 from the official project and follow its setup instructions:

```text
https://github.com/xlang-ai/Spider2
```

Place the prepared data under paths matching the config templates:

```text
data/spider2-lite/spider2-lite.jsonl
data/spider2-lite/resource/

data/spider2-snow/spider2-snow.jsonl
data/spider2-snow/resource/
```

If your Spider2 split references BigQuery or Snowflake databases, provide valid credentials at the paths configured in the template:

```text
bigquery_credential.json
snowflake_credential.json
```

## 3. Create Local Configs

Choose a model template. For example, Qwen3.6-27B:

```bash
mkdir -p config/local/Qwen3.6-27B
cp config/template/Qwen3.6-27B/config-spider2-lite.toml config/local/Qwen3.6-27B/config-spider2-lite.toml
cp config/template/Qwen3.6-27B/config-spider2-snow.toml config/local/Qwen3.6-27B/config-spider2-snow.toml
```

Or use Qwen3-Coder-30B-A3B-Instruct:

```bash
mkdir -p config/local/Qwen3-Coder-30B-A3B-Instruct
cp config/template/Qwen3-Coder-30B-A3B-Instruct/config-spider2-lite.toml config/local/Qwen3-Coder-30B-A3B-Instruct/config-spider2-lite.toml
cp config/template/Qwen3-Coder-30B-A3B-Instruct/config-spider2-snow.toml config/local/Qwen3-Coder-30B-A3B-Instruct/config-spider2-snow.toml
```

Edit only the LLM endpoint fields:

```toml
[llm_profiles.<profile_name>]
base_url = "your-llm-model-base-url"
api_key = "your-llm-api-key"
```

No local-device fields need editing for Spider2 configs.

## 4. Run Lite

Set `CONFIG_PATH` and run the Spider2 wrapper:

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-spider2-lite.toml
bash script/run_spider2.sh run
bash script/run_spider2.sh eval
bash script/run_spider2.sh export
```

For another model config directory, point `CONFIG_PATH` at that local config:

```bash
export CONFIG_PATH=config/local/Qwen3-Coder-30B-A3B-Instruct/config-spider2-lite.toml
bash script/run_spider2.sh run
```

## 5. Run Snow

Use the same commands with the snow config:

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-spider2-snow.toml
bash script/run_spider2.sh run
bash script/run_spider2.sh eval
bash script/run_spider2.sh export
```

For another model config directory:

```bash
export CONFIG_PATH=config/local/Qwen3-Coder-30B-A3B-Instruct/config-spider2-snow.toml
bash script/run_spider2.sh run
```

Spider2 exports SQL files under the run directory, usually:

```text
workspace/runs/spider2-lite/sql_output/
workspace/runs/spider2-snow/sql_output/
```
