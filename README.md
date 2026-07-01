<div align="center">

<h1>DeepEye-SQL</h1>
<p><strong>SIGMOD 2026 · Software-Engineering-Inspired Text-to-SQL</strong></p>

<p>
  <a href="https://deepeyesql-hahwwmgj.manus.space"><img src="https://img.shields.io/badge/Homepage-Visit-0ea5e9" alt="Homepage"></a>
  <a href="https://arxiv.org/abs/2510.17586"><img src="https://img.shields.io/badge/Paper-arXiv%3A2510.17586-b31b1b?logo=arxiv&logoColor=white" alt="Paper"></a>
  <img src="https://img.shields.io/badge/Task-Text--to--SQL-0f766e" alt="Task">
  <img src="https://img.shields.io/badge/Benchmarks-Spider%20%7C%20BIRD%20%7C%20Spider2-7c3aed" alt="Benchmarks">
</p>

<p>
  <img src="https://img.shields.io/badge/Architecture-Multi--Stage%20Pipeline-f97316" alt="Architecture">
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white" alt="Python"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/License-MIT-1f6feb" alt="License"></a>
</p>

<p>
  <a href="https://deepeyesql-hahwwmgj.manus.space">Homepage</a> ·
  <a href="#highlights">Highlights</a> ·
  <a href="#results">Results</a> ·
  <a href="#repository-tour">Repository Tour</a> ·
  <a href="#installation">Installation</a> ·
  <a href="#quick-start">Quick Start</a> ·
  <a href="#reproducibility-workflow">Reproducibility</a> ·
  <a href="#evaluation">Evaluation</a> ·
  <a href="#citation">Citation</a>
</p>

</div>

---

<table>
  <tr>
    <td width="52%">
      <h3>What is DeepEye-SQL?</h3>
      <p>
        DeepEye-SQL treats <strong>Text-to-SQL as a software engineering process</strong> rather than a single-shot generation task.
        Instead of asking one LLM call to solve everything, it decomposes the problem into grounding, reasoning, implementation,
        debugging, and final selection.
      </p>
      <p>
        The repository contains the full research pipeline used in our <strong>SIGMOD 2026</strong> submission, with support for
        <strong>Spider</strong>, <strong>BIRD</strong>, and <strong>Spider2</strong>, including cloud-backed databases in Spider2.
      </p>
    </td>
    <td width="48%">
      <h3>Core Idea</h3>
      <ul>
        <li><strong>Value Retrieval</strong>: ground mentions to concrete cell values</li>
        <li><strong>Schema Linking</strong>: combine direct, reversed, and value-based linking</li>
        <li><strong>SQL Generation</strong>: use diverse candidate generators</li>
        <li><strong>SQL Revision</strong>: run checker-style repair passes</li>
        <li><strong>SQL Selection</strong>: choose final SQL by execution-aware comparison</li>
      </ul>
    </td>
  </tr>
</table>

## Highlights

<table>
  <tr>
    <td>🧠</td>
    <td><strong>Software-engineering pipeline</strong> instead of monolithic prompt-only generation.</td>
  </tr>
  <tr>
    <td>🧪</td>
    <td><strong>Checker-based SQL revision</strong> for syntax, execution, and result-level repair.</td>
  </tr>
  <tr>
    <td>🎯</td>
    <td><strong>Execution-aware selection</strong> over multiple SQL candidates.</td>
  </tr>
  <tr>
    <td>🌐</td>
    <td><strong>Cross-benchmark support</strong> for Spider, BIRD, and Spider2.</td>
  </tr>
  <tr>
    <td>💾</td>
    <td><strong>Structured snapshot workflow</strong> for checkpointing, resume, conversion, and evaluation.</td>
  </tr>
  <tr>
    <td>📏</td>
    <td><strong>Built-in execution benchmark tooling</strong> for profiling SQL hotspots.</td>
  </tr>
</table>

## Results

DeepEye-SQL achieves strong performance with off-the-shelf LLMs and no task-specific fine-tuning.

| Benchmark | Metric | Score | Model | Public Outputs |
| --- | --- | ---: | --- | --- |
| BIRD-Dev | EX | **74.5** | Qwen3.6-27B | [results/bird-dev/qwen3.6-27b.json](results/bird-dev/qwen3.6-27b.json) |
| BIRD-Test | EX | **75.1** | Qwen3-Coder-30B-A3B | not released |
| Spider2-Lite | official score | **38.2** | DeepSeek-R1 | [results/spider2-lite/deepseek-r1](results/spider2-lite/deepseek-r1) |
| Spider2-Snow | official score | **50.5** | DeepSeek-R1 | [results/spider2-snow/deepseek-r1](results/spider2-snow/deepseek-r1) |

## Pipeline Overview

```text
Natural Language Question
        |
        v
1. Value Retrieval
   Ground relevant values from the database / vector index
        |
        v
2. Dynamic Few-shot Preparation
   Retrieve training examples with masked question and preliminary SQL similarity
        |
        v
3. Schema Linking
   Merge direct linking, reversed linking, and value linking
        |
        v
4. SQL Generation
   Produce diverse SQL candidates via multiple generators
        |
        v
5. SQL Revision
   Repair candidates using checker-style debugging passes
        |
        v
6. SQL Selection
   Execute, compare, and select the final SQL
```

### Why this design?

- A single generation pass is brittle on complex enterprise schemas.
- Different failure modes need different tools: grounding, linking, repair, and selection are not the same problem.
- Execution feedback is too valuable to reserve only for final evaluation.

## Repository Tour

```text
DeepEye-SQL
├── app/
│   ├── config/          # lazy config loading and typed settings
│   ├── dataset/         # Spider / BIRD / Spider2 datasets + structured snapshots
│   ├── db_utils/        # SQL execution, schema loading, cloud adapters
│   ├── llm/             # OpenAI-compatible LLM wrapper
│   ├── pipeline/        # five-stage Text-to-SQL pipeline
│   ├── services/        # schema service, execution service, artifact store
│   ├── prompt/          # prompt templates
│   └── vector_db/       # vector index creation for value retrieval
├── config/              # example experiment configs
├── docs/                # public from-scratch runbooks
├── runner/              # reproducible entry scripts
├── results/             # released predictions and few-shot seeds
├── script/              # helper shell scripts
└── workspace/           # generated snapshots and intermediate outputs
```

### Key entry points

- [script/run_pipeline.sh](script/run_pipeline.sh): full pipeline automation for one TOML config
- [script/run_bird.sh](script/run_bird.sh): standard BIRD full-run command wrapper for dev/test, inspection, evaluation, and export
- [runner/preprocess_dataset.py](runner/preprocess_dataset.py): build initial dataset snapshot
- [runner/create_vector_db_parallel.py](runner/create_vector_db_parallel.py): create value-retrieval vector indices
- [runner/run_value_retrieval.py](runner/run_value_retrieval.py)
- [runner/build_few_shot_index.py](runner/build_few_shot_index.py): build the masked training-example retrieval index
- [runner/run_few_shot_preparation.py](runner/run_few_shot_preparation.py): attach dynamic few-shot examples before schema linking
- [runner/inspect_few_shot_preparation.py](runner/inspect_few_shot_preparation.py): summarize preliminary SQL and retrieval quality
- [runner/run_schema_linking.py](runner/run_schema_linking.py)
- [runner/run_sql_generation.py](runner/run_sql_generation.py)
- [runner/run_sql_revision.py](runner/run_sql_revision.py)
- [runner/run_sql_selection.py](runner/run_sql_selection.py)
- [runner/convert_snapshot_to_sql.py](runner/convert_snapshot_to_sql.py): convert structured snapshots to evaluation outputs
- [runner/evaluation.py](runner/evaluation.py): unified evaluation entry
- [runner/benchmark_execution.py](runner/benchmark_execution.py): execution-layer benchmark runner

## Installation

### Requirements

- Python `>= 3.12`
- Linux/macOS environment recommended
- OpenAI-compatible LLM endpoint for each stage
- Embedding endpoint or local embedding model for value retrieval

### 1. Clone

```bash
git clone https://github.com/HKUSTDial/DeepEye-SQL.git
cd DeepEye-SQL
```

### 2. Install dependencies

We recommend `uv`.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

### 3. Optional cloud dependencies

Spider2 cloud evaluation may require valid:

- BigQuery credentials
- Snowflake credentials

The corresponding paths are configured in [config/template/Qwen3.6-27B/config-spider2-lite.toml](config/template/Qwen3.6-27B/config-spider2-lite.toml) and [config/template/Qwen3.6-27B/config-spider2-snow.toml](config/template/Qwen3.6-27B/config-spider2-snow.toml).

## Runbooks

For from-scratch dataset-specific instructions, see:

- [BIRD runbook](docs/bird.md)
- [Spider runbook](docs/spider.md)
- [Spider2 runbook](docs/spider2.md)

The templates are intended to run as-is after filling endpoint fields and local-device fields: LLM/embedding `base_url`, `api_key`, `similarity_device`, and `local_index_device` where applicable.

## Dataset Setup

### Spider and BIRD

Use the provided helper script:

```bash
bash script/download_dataset.sh
```

This downloads and prepares:

- Spider train/dev/test assets included in the Spider package
- BIRD dev split
- BIRD train databases from the official BIRD release
- BIRD train question/SQL labels from the cleaned `birdsql/bird23-train-filtered` Hugging Face dataset

### Spider2

Please obtain Spider2 data from the official repository:

- https://github.com/xlang-ai/Spider2

Follow the official setup instructions there, then place the prepared data under paths consistent with your config, for example:

- `data/spider2-lite`
- `data/spider2-snow`

You also need valid cloud credentials if your Spider2 split references BigQuery or Snowflake databases.

## Configuration

Tracked templates are grouped by model under `config/template/<model>/`:

- `config/template/Qwen3.6-27B/`
- `config/template/Qwen3-Coder-30B-A3B-Instruct/`

Each model directory contains dataset-specific templates:

- `config-bird-dev.toml`
- `config-bird-test.toml`
- `config-spider-test.toml`
- `config-spider2-lite.toml`
- `config-spider2-snow.toml`

Local experiment configs mirror the same model grouping under `config/local/<model>/`, which is ignored by git. Start by copying a template and filling in endpoint-specific fields such as API endpoints and API keys:

```bash
mkdir -p config/local/Qwen3.6-27B
cp config/template/Qwen3.6-27B/config-bird-dev.toml config/local/Qwen3.6-27B/config-bird-dev.toml
```

### Important config blocks

#### Run Outputs

```toml
[run]
save_root = "workspace/runs"
exp_name = "bird-dev"
parallelism = 16
embedding_batch_size = 256
llm_timeout = 300
progress_log_interval = 50
checkpoint_interval = 20
default_llm_profile = "qwen36_thinking"
```

By default, stage outputs are written under `save_root/exp_name`, for example `dataset.snapshot`, `value_retrieval.snapshot`, `schema_linking.snapshot`, and `sql_selection.snapshot`. Reusable training artifacts such as the few-shot index are written under `save_root/_shared/<dataset>/`.

`parallelism` is the global concurrency setting for LLM-heavy stages, value retrieval, and vector DB build work. `embedding_batch_size` controls the batch size used when sending texts to the embedding model or endpoint. `progress_log_interval` controls progress log cadence; `checkpoint_interval` controls intermediate pipeline checkpoint saves.

#### Dataset

```toml
[dataset]
type = "bird"                # spider | bird | spider2
split = "dev"
root_path = "data/bird"
```

#### Embedding / vector DB

```toml
[embedding]
api_type = "openai"
embedding_model_name_or_path = "Qwen3-Embedding-0.6B"
base_url = "your-embedding-model-base-url"
api_key = "your-embedding-api-key"

[vector_database]
max_value_length = 100
build_backend = "local_index"
```

#### Few-shot retrieval

```toml
[few_shot_index]
num_examples = 7
question_weight = 0.6
sql_weight = 0.4
similarity_device = "cuda:0"
llm_profile = "qwen36_instruct"
```

`similarity_device` controls where few-shot question/SQL vector similarity is computed. It is separate from the embedding endpoint or local embedding model.

Few-shot index builds are resumable: masking uses the JSONL mask cache, and embedding batches are checkpointed under `.build_checkpoint` until the final `manifest.json` is written.

#### LLM Profiles

```toml
[run]
default_llm_profile = "qwen36_thinking"

[llm_profiles.qwen36_thinking]
model = "Qwen3.6-27B"
base_url = "your-llm-model-base-url"
api_key = "your-llm-api-key"
max_tokens = 16384
temperature = 0.6
n_call_strategy = "single"
api_type = "openai"
max_model_len = 65536
extra_body = { top_p = 0.95, top_k = 20, min_p = 0.0, presence_penalty = 0.0, repetition_penalty = 1.0, chat_template_kwargs = { enable_thinking = true } }

[llm_profiles.qwen36_instruct]
model = "Qwen3.6-27B"
base_url = "your-llm-model-base-url"
api_key = "your-llm-api-key"
max_tokens = 16384
temperature = 0.7
n_call_strategy = "single"
api_type = "openai"
max_model_len = 65536
extra_body = { top_p = 0.8, top_k = 20, min_p = 0.0, presence_penalty = 1.5, repetition_penalty = 1.0, chat_template_kwargs = { enable_thinking = false } }

[sql_generation]
llm_profile = "qwen36_thinking"
```

### Notes

- `save_path`, `store_root_path`, and cache path fields are optional. Set them only when you need to override the managed paths from `[run]`.
- `[llm_profiles]` defines reusable LLM profiles. `[run].default_llm_profile` sets the default, and each stage can choose a profile with `llm_profile`.
- `[embedding]` provides the shared embedding endpoint/model for BIRD and Spider. For Spider2, configure only the LLM endpoint fields in the template.
- All stage outputs are stored as structured `.snapshot` manifests.
- Only structured `.snapshot` manifests are supported.

## Quick Start

### Option A: one-config full pipeline

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-dev.toml
bash script/run_pipeline.sh
```

### Option B: BIRD wrapper

Set `CONFIG_PATH` before running each wrapper command.

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-dev.toml
bash script/run_bird.sh run
bash script/run_bird.sh inspect
bash script/run_bird.sh eval
bash script/run_bird.sh export

export CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-test.toml
bash script/run_bird.sh run
bash script/run_bird.sh inspect
bash script/run_bird.sh export
```

The `run` command builds the few-shot training index automatically if it is missing. Use `rebuild-index` only for an intentional overwrite or an incomplete index directory. BIRD test has no public gold labels, so skip `eval` for official test runs.

### Option C: Spider wrapper

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-spider-test.toml
bash script/run_spider.sh run
bash script/run_spider.sh inspect
bash script/run_spider.sh eval
bash script/run_spider.sh export
```

### Option D: Spider2 wrapper

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-spider2-lite.toml
bash script/run_spider2.sh run
bash script/run_spider2.sh eval
bash script/run_spider2.sh export

export CONFIG_PATH=config/local/Qwen3.6-27B/config-spider2-snow.toml
bash script/run_spider2.sh run
bash script/run_spider2.sh eval
bash script/run_spider2.sh export
```

### Option E: stage-by-stage

```bash
export CONFIG_PATH=config/local/Qwen3.6-27B/config-bird-dev.toml

uv run runner/preprocess_dataset.py
uv run runner/create_vector_db_parallel.py
uv run runner/run_value_retrieval.py
uv run runner/build_few_shot_index.py
uv run runner/run_few_shot_preparation.py
uv run runner/run_schema_linking.py
uv run runner/run_sql_generation.py
uv run runner/run_sql_revision.py
uv run runner/run_sql_selection.py
```

### What gets produced?

With `[run] save_root = "workspace/runs"` and `exp_name = "bird-dev"`, stage outputs land under `workspace/runs/bird-dev/`:

- `dataset.snapshot`
- `value_retrieval.snapshot`
- `few_shot_preparation.snapshot`
- `schema_linking.snapshot`
- `sql_generation.snapshot`
- `sql_revision.snapshot`
- `sql_selection.snapshot`

Reusable training artifacts such as the few-shot train index are stored under `workspace/runs/_shared/<dataset>/`.

- active config copy:
  `workspace/runs/<exp_name>/config.toml`

## Reproducibility Workflow

DeepEye-SQL uses a structured snapshot format to make long-running experiments resumable and inspectable.

Each run that loads `app.config.get_config()` copies the active TOML config to `<run.save_dir>/config.toml`, so run outputs can be checked against the config used for the run.

### 1. Preprocess dataset

```bash
uv run runner/preprocess_dataset.py
```

This creates the initial dataset snapshot referenced by the resolved `dataset.save_path`.

### 2. Build value index

```bash
uv run runner/create_vector_db_parallel.py
```

Notes:

- This step is required for Spider/BIRD.
- For Spider2, start from the pipeline stages; vector DB creation is not required.

### 3. Run pipeline stages

Each stage consumes the previous stage snapshot and writes a new one.

### 4. Convert final snapshot to official submission format

```bash
uv run runner/convert_snapshot_to_sql.py \
  --snapshot_path workspace/runs/bird-dev/sql_selection.snapshot
```

### 5. Evaluate

```bash
uv run runner/evaluation.py \
  --snapshot_path workspace/runs/bird-dev/sql_selection.snapshot
```

## Evaluation

The unified evaluator supports Spider, BIRD, and Spider2.

```bash
uv run runner/evaluation.py --help
```

### Spider / BIRD

```bash
uv run runner/evaluation.py \
  --snapshot_path workspace/runs/bird-dev/sql_selection.snapshot \
  --dataset_type bird
```

### Spider2

```bash
uv run runner/evaluation.py \
  --snapshot_path workspace/runs/spider2-lite/sql_selection.snapshot \
  --dataset_type spider2 \
  --dataset_split lite
```

The evaluator will:

- auto-detect dataset type when possible
- convert snapshot outputs when needed
- call the official Spider2 evaluation entry for Spider2 workflows

## Execution Benchmarking

We include an execution-layer benchmark script for profiling SQL hotspots.

### Synthetic SQLite benchmark

```bash
uv run runner/benchmark_execution.py \
  --rows 20000 \
  --iterations 8 \
  --measure-repeat 5
```

### Real snapshot benchmark

```bash
uv run runner/benchmark_execution.py \
  --snapshot-path workspace/runs/bird-dev/sql_selection.snapshot \
  --snapshot-sample-size 20
```

This is useful when you want to quantify:

- cached vs uncached execution cost
- `measure_time()` overhead
- SQL selection scan cost
- execution invocation counts per item

## Public Artifacts

### Released predictions

- [results/bird-dev/qwen3.6-27b.json](results/bird-dev/qwen3.6-27b.json)
- [results/spider2-lite](results/spider2-lite)
- [results/spider2-snow](results/spider2-snow)

## FAQ

### Does Spider2 need vector retrieval?

No. For Spider2, use the Spider2-specific database and schema workflow.

### Can I use local models?

Yes, as long as the endpoint is OpenAI-compatible, or the embedding stack is configured to use local models for vector indexing.

### Why does a stage fail immediately saying an input snapshot is missing?

Run stages in order. Each stage expects the snapshot produced by the stage before it.

## Citation

If you find DeepEye-SQL useful in your research, please cite:

Paper: [https://doi.org/10.1145/3802035](https://doi.org/10.1145/3802035)

```bibtex
@article{10.1145/3802035,
author = {Li, Boyan and Chen, Chong and Xue, Zhujun and Mei, Yinan and Luo, Yuyu},
title = {DeepEye-SQL: A Software-Engineering-Inspired Text-to-SQL Framework},
year = {2026},
issue_date = {June 2026},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
volume = {4},
number = {3},
url = {https://doi.org/10.1145/3802035},
doi = {10.1145/3802035},
abstract = {Large language models (LLMs) have advanced Text-to-SQL, yet existing solutions still fall short of system-level reliability. The limitation is not merely in individual modules -- e.g., schema linking, reasoning, and verification -- but more critically in the lack of structured orchestration that enforces correctness across the entire workflow. This gap motivates a paradigm shift: treating Text-to-SQL not as free-form language generation but as a software-engineering problem that demands structured, verifiable orchestration. We present DeepEye-SQL, a software-engineering-inspired framework that reframes Text-to-SQL as the development of a small software program, executed through a verifiable process guided by the Software Development Life Cycle (SDLC). DeepEye integrates four synergistic stages: it grounds user intent through robust schema linking, enforcing relational closure; enhances fault tolerance with N-version SQL generation; ensures deterministic verification via a ''Syntax-Logic-Quality'' tool-chain that intercepts errors pre-execution; and introduces confidence-aware selection that leverages execution-guided adjudication to resolve ambiguity beyond simple majority voting. Leveraging open-source MoE LLMs (~30B total, ~3B activated parameters) without any fine-tuning, DeepEye achieves 73.5\% execution accuracy on BIRD-Dev, 75.07\% on the official BIRD-Test leaderboard, and 89.8\% on Spider-Test, outperforming state-of-the-art solutions that rely on larger models or extensive training. This highlights that principled orchestration, rather than LLM scaling alone, is key to achieving system-level reliability in Text-to-SQL.},
journal = {Proc. ACM Manag. Data},
month = may,
articleno = {158},
numpages = {28},
keywords = {text-to-sql, databases, large language models}
}
```

## License

This project is released under the MIT License. See [LICENSE](LICENSE).

## Acknowledgement

DeepEye-SQL builds on public benchmark ecosystems and OpenAI-compatible LLM serving stacks. We thank the maintainers of Spider, BIRD, Spider2, ChromaDB, OpenAI-compatible serving frameworks, and the broader Text-to-SQL research community.
