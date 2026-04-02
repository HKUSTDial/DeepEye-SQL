<div align="center">

<h1>DeepEye-SQL</h1>
<p><strong>SIGMOD 2026 · Software-Engineering-Inspired Text-to-SQL</strong></p>

<p>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white" alt="Python"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/License-MIT-1f6feb" alt="License"></a>
  <img src="https://img.shields.io/badge/Task-Text--to--SQL-0f766e" alt="Task">
  <img src="https://img.shields.io/badge/Benchmarks-Spider%20%7C%20BIRD%20%7C%20Spider2-7c3aed" alt="Benchmarks">
  <img src="https://img.shields.io/badge/Architecture-Multi--Stage%20Pipeline-f97316" alt="Architecture">
</p>

<p>
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
| BIRD-Dev | EX | **73.5** | Qwen3-Coder-30B-A3B | [results/bird-dev/qwen3-coder-30b-a3b.json](results/bird-dev/qwen3-coder-30b-a3b.json) |
| BIRD-Test | EX | **75.1** | Qwen3-Coder-30B-A3B | not released |
| Spider-Test | EX | **89.8** | Qwen3-Coder-30B-A3B | [results/spider-test/qwen3-coder-30b-a3b.json](results/spider-test/qwen3-coder-30b-a3b.json) |
| Spider2-Lite | official score | **38.2** | DeepSeek-R1 | [results/spider2-lite/deepseek-r1](results/spider2-lite/deepseek-r1) |
| Spider2-Snow | official score | **50.5** | DeepSeek-R1 | [results/spider2-snow/deepseek-r1](results/spider2-snow/deepseek-r1) |

<details>
<summary><strong>Additional released artifacts</strong></summary>

- BIRD few-shot seeds: [results/bird_dev_few_shots.json](results/bird_dev_few_shots.json)
- Spider few-shot seeds: [results/spider_test_few_shots.json](results/spider_test_few_shots.json)
- Alternative public predictions:
  [results/bird-dev/gemma3-27b.json](results/bird-dev/gemma3-27b.json),
  [results/bird-dev/qwen2.5-coder-32b.json](results/bird-dev/qwen2.5-coder-32b.json),
  [results/spider-test/gemma3-27b.json](results/spider-test/gemma3-27b.json),
  [results/spider-test/qwen2.5-coder-32b.json](results/spider-test/qwen2.5-coder-32b.json)

</details>

## Pipeline Overview

```text
Natural Language Question
        |
        v
1. Value Retrieval
   Ground relevant values from the database / vector index
        |
        v
2. Schema Linking
   Merge direct linking, reversed linking, and value linking
        |
        v
3. SQL Generation
   Produce diverse SQL candidates via multiple generators
        |
        v
4. SQL Revision
   Repair candidates using checker-style debugging passes
        |
        v
5. SQL Selection
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
├── runner/              # reproducible entry scripts
├── results/             # released predictions and few-shot seeds
├── script/              # helper shell scripts
└── workspace/           # generated snapshots and intermediate outputs
```

### Key entry points

- [script/run_pipeline.sh](script/run_pipeline.sh): full pipeline automation
- [runner/preprocess_dataset.py](runner/preprocess_dataset.py): build initial dataset snapshot
- [runner/create_vector_db_parallel.py](runner/create_vector_db_parallel.py): create value-retrieval vector indices
- [runner/run_value_retrieval.py](runner/run_value_retrieval.py)
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

The corresponding paths are configured in [config/config-spider2-example.toml](config/config-spider2-example.toml).

## Dataset Setup

### Spider and BIRD

Use the provided helper script:

```bash
bash script/download_dataset.sh
```

This downloads:

- Spider test split
- BIRD dev split

### Spider2

Please obtain Spider2 data from the official repository:

- https://github.com/xlang-ai/Spider2

Follow the official setup instructions there, then place the prepared data under paths consistent with your config, for example:

- `data/spider2-lite`
- `data/spider2-snow`

You also need valid cloud credentials if your Spider2 split references BigQuery or Snowflake databases.

## Configuration

Three examples are included:

- [config/config-spider-example.toml](config/config-spider-example.toml)
- [config/config-bird-example.toml](config/config-bird-example.toml)
- [config/config-spider2-example.toml](config/config-spider2-example.toml)

### Important config blocks

#### Dataset

```toml
[dataset]
type = "bird"                # spider | bird | spider2
split = "dev"
root_path = "data/bird"
save_path = "workspace/dataset/bird/dev.snapshot"
```

#### Embedding / vector DB

```toml
[vector_database]
api_type = "openai"          # or local
embedding_model_name_or_path = "your-embedding-model"
store_root_path = "workspace/vector_database/bird/dev"
db_parallel = 2
column_parallel = 8
```

#### Stage LLMs

```toml
[sql_generation.llm]
model = "your-model-name"
base_url = "https://your-openai-compatible-endpoint/v1"
api_key = "your-api-key"
max_tokens = 4096
temperature = 0.7
api_type = "openai"
max_model_len = 128000
```

### Notes

- Each stage can use a different model.
- All stage outputs are stored as structured `.snapshot` manifests.
- Only structured `.snapshot` manifests are supported.

## Quick Start

### Option A: full pipeline

```bash
export CONFIG_PATH=config/config-bird-example.toml
bash script/run_pipeline.sh
```

### Option B: stage-by-stage

```bash
export CONFIG_PATH=config/config-bird-example.toml

uv run runner/preprocess_dataset.py
uv run runner/create_vector_db_parallel.py
uv run runner/run_value_retrieval.py
uv run runner/run_schema_linking.py
uv run runner/run_sql_generation.py
uv run runner/run_sql_revision.py
uv run runner/run_sql_selection.py
```

### What gets produced?

Typical outputs land under `workspace/`:

- dataset snapshot:
  [workspace/dataset](workspace/dataset)
- stage snapshots:
  [workspace/value_retrieval](workspace/value_retrieval),
  [workspace/schema_linking](workspace/schema_linking),
  [workspace/sql_generation](workspace/sql_generation),
  [workspace/sql_revision](workspace/sql_revision),
  [workspace/sql_selection](workspace/sql_selection)

## Reproducibility Workflow

DeepEye-SQL uses a structured snapshot format to make long-running experiments resumable and inspectable.

### 1. Preprocess dataset

```bash
uv run runner/preprocess_dataset.py
```

This creates the initial dataset snapshot referenced by `dataset.save_path`.

### 2. Build value index

```bash
uv run runner/create_vector_db_parallel.py
```

Notes:

- This step is required for Spider/BIRD.
- Spider2 skips vector DB creation because the current workflow does not use vector retrieval there.

### 3. Run pipeline stages

Each stage consumes the previous stage snapshot and writes a new one.

### 4. Convert final snapshot to official submission format

```bash
uv run runner/convert_snapshot_to_sql.py \
  --snapshot_path workspace/sql_selection/bird/dev.snapshot
```

### 5. Evaluate

```bash
uv run runner/evaluation.py \
  --snapshot_path workspace/sql_selection/bird/dev.snapshot
```

## Evaluation

The unified evaluator supports Spider, BIRD, and Spider2.

```bash
uv run runner/evaluation.py --help
```

### Spider / BIRD

```bash
uv run runner/evaluation.py \
  --snapshot_path workspace/sql_selection/bird/dev.snapshot \
  --dataset_type bird
```

### Spider2

```bash
uv run runner/evaluation.py \
  --snapshot_path workspace/sql_selection/spider2/lite.snapshot \
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
  --snapshot-path workspace/sql_selection/bird/dev.snapshot \
  --snapshot-sample-size 20
```

This is useful when you want to quantify:

- cached vs uncached execution cost
- `measure_time()` overhead
- SQL selection scan cost
- execution invocation counts per item

## Public Artifacts

### Released predictions

- [results/bird-dev](results/bird-dev)
- [results/spider-test](results/spider-test)
- [results/spider2-lite](results/spider2-lite)
- [results/spider2-snow](results/spider2-snow)

## FAQ

### Does Spider2 need vector retrieval?

No. The current pipeline skips vector DB construction for Spider2 and relies on the Spider2-specific database and schema workflow.

### Can I use local models?

Yes, as long as the endpoint is OpenAI-compatible, or the embedding stack is configured to use local models for vector indexing.

### Why does a stage fail immediately saying an input snapshot is missing?

That is intentional fail-fast behavior. Each stage expects its predecessor snapshot to exist. Run the previous stage first.

## Citation

If you find DeepEye-SQL useful in your research, please cite:

```bibtex
@article{li2025deepeye,
  author  = {Boyan Li and Chong Chen and Zhujun Xue and Yinan Mei and Yuyu Luo},
  title   = {{DeepEye-SQL:} A Software-Engineering-Inspired Text-to-SQL Framework},
  journal = {Proc. {ACM} Manag. Data},
  volume  = {4},
  number  = {3},
  year    = {2026},
  doi     = {10.1145/3802035}
}
```

## License

This project is released under the MIT License. See [LICENSE](LICENSE).

## Acknowledgement

DeepEye-SQL builds on public benchmark ecosystems and OpenAI-compatible LLM serving stacks. We thank the maintainers of Spider, BIRD, Spider2, ChromaDB, OpenAI-compatible serving frameworks, and the broader Text-to-SQL research community.
