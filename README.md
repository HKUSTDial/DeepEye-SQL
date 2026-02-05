# 🚀 DeepEye-SQL

<div align="center">

**A Software-Engineering-Inspired Text-to-SQL Framework**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[Performance](#-performance) • [Installation](#-installation) • [Quick Start](#-quick-start) • [Documentation](#-documentation)

</div>

---

## 🌟 Overview

DeepEye-SQL is a cutting-edge Text-to-SQL framework that revolutionizes SQL generation by treating it as **software development** rather than simple text generation. Inspired by the Software Development Life Cycle (SDLC), DeepEye-SQL systematically transforms natural language questions into accurate SQL queries through a multi-stage pipeline.

### Why DeepEye-SQL?

- 🎯 **State-of-the-Art Performance**: Achieves **73.5% on BIRD-Dev** and **89.8% on Spider-Test** without fine-tuning
- 🏗️ **Software Engineering Principles**: Structured pipeline following SDLC methodology
- 🔄 **N-Version Programming**: Generates multiple SQL candidates with diverse strategies
- 🧪 **Tool-Based Verification**: Tests SQL queries with unit-test-like checkers
- 🎲 **Pairwise Selection**: Uses pairwise comparison for optimal SQL selection
- 🌐 **Multi-Database Support**: Works with Spider, BIRD, and Spider2.0 (including cloud databases)

---

## 📊 Performance

DeepEye-SQL achieves **state-of-the-art results** on major Text-to-SQL benchmarks without any model fine-tuning. Detailed prediction files are available in the `results/` directory for verification.

| Benchmark | Execution Accuracy (EX) | Model Used | Prediction File |
|-----------|------------------------|------------|-----------------|
| **BIRD-Dev** | **73.5%** | Qwen3-Coder-30B-A3B | [`results/bird-dev/qwen3...json`](results/bird-dev/qwen3-coder-30b-a3b.json) |
| **BIRD-Test** | **75.1%** | Qwen3-Coder-30B-A3B | - |
| **Spider-Test** | **89.8%** | Qwen3-Coder-30B-A3B | [`results/spider-test/qwen3...json`](results/spider-test/qwen3-coder-30b-a3b.json) |
| **Spider2.0-Lite** | **38.2%** | DeepSeek-R1 | [`results/spider2-lite/`](results/spider2-lite/deepseek-r1/) |
| **Spider2.0-Snow** | **50.5%** | DeepSeek-R1 | [`results/spider2-snow/`](results/spider2-snow) |

> 💡 **Note**: Results are achieved with off-the-shelf LLMs (no fine-tuning required). Performance may vary with different backbone models.

---

## 🏗️ Architecture

DeepEye-SQL follows a **5-stage pipeline** inspired by the Software Development Life Cycle:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Natural Language Question                   │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────▼──────────────┐
        │  1. Value Retrieval        │  Grounding Phase
        │     (Semantic Search)      │
        └─────────────┬──────────────┘
                      │
        ┌─────────────▼──────────────┐
        │  2. Schema Linking         │  Reasoning Phase
        │     (3-Stage Linking)      │
        └─────────────┬──────────────┘
                      │
        ┌─────────────▼──────────────┐
        │  3. SQL Generation         │  Implementation Phase
        │     (N-Version)            │
        └─────────────┬──────────────┘
                      │
        ┌─────────────▼──────────────┐
        │  4. SQL Revision           │  Testing/Debugging Phase
        │     (Checker-Based)        │
        └─────────────┬──────────────┘
                      │
        ┌─────────────▼──────────────┐
        │  5. SQL Selection          │  QA/Deployment Phase
        │     (Pairwise Eval)        │
        └─────────────┬──────────────┘
                      │
        ┌─────────────▼──────────────┐
        │     Final SQL Query        │
        └────────────────────────────┘
```

---

## 📁 Project Structure

```
DeepEye-SQL/
├── app/                        # Core application code
│   ├── config/                # Configuration management
│   ├── dataset/               # Dataset loaders (Spider/BIRD/Spider2)
│   ├── db_utils/              # Database utilities and schema parsing
│   ├── llm/                   # LLM interface (OpenAI-compatible)
│   ├── llm_extractor/         # Structured output extraction
│   ├── logger/                # Logging utilities
│   ├── pipeline/              # 5-stage pipeline implementation
│   │   ├── value_retrieval/  # Stage 1: Value grounding
│   │   ├── schema_linking/   # Stage 2: Schema reasoning
│   │   ├── sql_generation/   # Stage 3: SQL implementation
│   │   ├── sql_revision/     # Stage 4: SQL testing/debugging
│   │   └── sql_selection/    # Stage 5: Final selection
│   ├── prompt/                # Prompt template library
│   └── vector_db/             # Vector database for value retrieval
├── config/                    # Configuration files
│   ├── config-spider-example.toml
│   ├── config-bird-example.toml
│   └── config-spider2-example.toml
├── runner/                    # Entry point scripts
│   ├── preprocess_dataset.py
│   ├── create_vector_db_parallel.py
│   ├── run_value_retrieval.py
│   ├── run_schema_linking.py
│   ├── run_sql_generation.py
│   ├── run_sql_revision.py
│   ├── run_sql_selection.py
│   ├── convert_pkl_to_sql.py       # Unified conversion (auto-detects format)
│   └── evaluation.py               # Unified evaluation (all datasets)
├── results/                   # Few-shots and experimental prediction files
│   ├── spider_test_few_shots.json # DAIL-SQL few-shots for Spider
│   ├── bird_dev_few_shots.json   # DAIL-SQL few-shots for BIRD
│   ├── spider-test/              # Prediction SQLs for Spider
│   ├── bird-dev/                 # Prediction SQLs for BIRD
│   ├── spider2-lite/             # Prediction SQLs for Spider2-Lite
│   └── spider2-snow/             # Prediction SQLs for Spider2-Snow
├── script/                    # Automation scripts
│   ├── download_dataset.sh
│   └── run_pipeline.sh
└── workspace/                 # Output directory (auto-generated)
    ├── dataset/               # Preprocessed datasets
    ├── vector_database/       # Value indices
    ├── value_retrieval/       # Stage outputs
    ├── schema_linking/
    ├── sql_generation/
    ├── sql_revision/
    └── sql_selection/
```

---

## 🔧 Installation

### Prerequisites

- Python 3.10 or higher
- OpenAI-compatible LLM API access (e.g., OpenAI, DeepSeek, local vLLM)

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/DeepEye-SQL.git
cd DeepEye-SQL
```

### Step 2: Install Dependencies

DeepEye-SQL uses [**uv**](https://github.com/astral-sh/uv) for fast, reproducible dependency management:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies
uv sync
```

> 💡 **Alternative**: You can also use `pip install -r requirements.txt` if you prefer traditional pip.

### Step 3: Download Datasets

Use our automated script to download and prepare datasets:

```bash
bash script/download_dataset.sh
```

This will download:
- ✅ Spider dataset (dev + test splits)
- ✅ BIRD dataset (dev split)

For **Spider2.0**, please follow the [official instructions](https://spider2-sql.github.io/) to obtain the dataset.

---

## 🚀 Quick Start

### Running the Full Pipeline

The easiest way to get started is using our automated pipeline script:

```bash
# Set your config file
export CONFIG_PATH="config/config-spider-example.toml"

# Run the full pipeline
bash script/run_pipeline.sh
```

This will execute all 5 stages sequentially and save results to the `workspace/` directory.

---

## 📚 Documentation

### Configuration Guide

DeepEye-SQL uses TOML configuration files. We provide example configs for each dataset:

- `config/config-spider-example.toml` - Spider dataset configuration
- `config/config-bird-example.toml` - BIRD dataset configuration
- `config/config-spider2-example.toml` - Spider2.0 configuration

**Key Configuration Sections:**

1. **Dataset Configuration**
   ```toml
   [dataset]
   type = "spider"      # Options: "spider", "bird", "spider2"
   split = "dev"        # Varies by dataset type
   root_path = "data/spider"
   ```

2. **LLM Configuration** (appears in multiple stages)
   ```toml
   [value_retrieval.llm]
   model = "gpt-4o"
   base_url = "https://api.openai.com/v1"
   api_key = "your-api-key-here"
   max_tokens = 4096
   temperature = 0.7
   ```

3. **Parallelism Settings**
   ```toml
   n_parallel = 16      # Adjust based on API rate limits
   ```

### Running on Spider Dataset

#### Step 1: Prepare Configuration

```bash
# Copy example config
cp config/config-spider-example.toml config/config.toml

# Edit config.toml and set:
# - Your LLM API key and endpoint
# - Model name
# - Parallelism settings (n_parallel)
```

#### Step 2: Run the Pipeline

**Option A: Automated Pipeline (Recommended)**

```bash
export CONFIG_PATH="config/config.toml"
bash script/run_pipeline.sh
```

**Option B: Step-by-Step Execution**

```bash
# 1. Preprocess dataset
uv run runner/preprocess_dataset.py

# 2. Build vector database for value retrieval
uv run runner/create_vector_db_parallel.py

# 3. Run value retrieval
uv run runner/run_value_retrieval.py

# 4. Run schema linking
uv run runner/run_schema_linking.py

# 5. Generate SQL candidates
uv run runner/run_sql_generation.py

# 6. Revise SQL with checkers
uv run runner/run_sql_revision.py

# 7. Select final SQL
uv run runner/run_sql_selection.py
```

#### Step 3: Evaluate Results

```bash
# Convert results to JSON format (auto-detects format from config)
uv run runner/convert_pkl_to_sql.py

# Run evaluation (auto-detects dataset type from config)
uv run runner/evaluation.py
```

The evaluation script will automatically detect the dataset type and output execution accuracy metrics.

---

### Running on BIRD Dataset

#### Step 1: Prepare Configuration

```bash
# Copy BIRD example config
cp config/config-bird-example.toml config/config.toml

# Edit config.toml:
# - Set your LLM credentials
# - BIRD only has "dev" split
# - Adjust n_parallel based on your setup
```

#### Step 2: Run the Pipeline

```bash
export CONFIG_PATH="config/config.toml"
bash script/run_pipeline.sh
```

#### Step 3: Evaluate Results

```bash
# Convert to JSON format (auto-detects format from config)
uv run runner/convert_pkl_to_sql.py

# Run evaluation (auto-detects dataset type from config)
uv run runner/evaluation.py
```

**BIRD-Specific Notes:**
- BIRD dataset has more complex schemas and external knowledge
- Evidence/hints are automatically loaded from the dataset
- Consider using higher `max_tokens` for complex queries

---

### Running on Spider2.0 Dataset

Spider2.0 includes **cloud databases** (BigQuery, Snowflake) and requires additional setup.

#### Step 1: Obtain Spider2.0 Dataset

Follow the [official Spider2.0 instructions](https://spider2-sql.github.io/) to:
1. Download the Spider2-Lite or Spider2-Snow dataset
2. Place it in `data/spider2-lite/` or `data/spider2-snow/`
3. Download  [local database](https://drive.usercontent.google.com/download?id=1coEVsCZq-Xvj9p2TnhBFoFTsY-UoYGmG&export=download&authuser=0), unzip and put all the `.sqlite` files into directory `spider2-lite/resource/databases/spider2-localdb`

#### Step 2: Set Up Cloud Credentials (if needed)

For BigQuery and Snowflake databases, create credential files:

**BigQuery** (`bigquery_credential.json`):
```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key": "your-private-key",
  ...
}
```

**Snowflake** (`snowflake_credential.json`):
```json
{
  "account": "your-account",
  "user": "your-username",
  "password": "your-password",
  "warehouse": "your-warehouse",
  ...
}
```

#### Step 3: Prepare Configuration

```bash
# Copy Spider2 example config
cp config/config-spider2-example.toml config/config.toml

# Edit config.toml:
# - Set split to "lite" or "snow"
# - Update root_path to your Spider2 data directory
# - Set credential paths
# - Configure your LLM
```

#### Step 4: Run the Pipeline

```bash
export CONFIG_PATH="config/config.toml"
bash script/run_pipeline.sh
```

#### Step 5: Evaluate Results

```bash
# Convert to SQL files (auto-detects format and creates individual .sql files for Spider2)
uv run runner/convert_pkl_to_sql.py

# Run evaluation (auto-detects Spider2 and uses official evaluation script)
uv run runner/evaluation.py
```

**Spider2-Specific Notes:**
- Spider2 has larger schemas → ensure enough model context length
- Cloud database execution may be slower (set higher `sql_execution_timeout`, e.g., 120s)
- The evaluation script automatically converts to SQL files and calls the official Spider2 evaluator
- Some queries may require external knowledge documents
- Results will be compared against official gold SQLs using the Spider2 evaluation suite

---

## ⚙️ Advanced Usage

### Custom LLM Backends

DeepEye-SQL supports any OpenAI-compatible API:

**Using Local vLLM Server:**
```toml
[value_retrieval.llm]
model = "meta-llama/Llama-3-70B-Instruct"
base_url = "http://localhost:8000/v1"
api_key = "dummy"
api_type = "openai"
```

**Using Azure OpenAI:**
```toml
[value_retrieval.llm]
model = "gpt-4"
base_url = "https://your-resource.openai.azure.com/"
api_key = "your-azure-key"
api_type = "azure"
api_version = "2024-02-15-preview"
```

### Customizing Checkers

Enable/disable specific checkers in SQL revision:

```toml
[sql_revision]
checkers = [
    "SyntaxChecker",
    "ResultChecker",
    "SelectChecker",
    "JoinChecker",
    "MaxMinChecker",
    "OrderByLimitChecker",
    "OrderByNullChecker",
    "TimeChecker"
]
```

### Adjusting Sampling Budgets

Control the diversity vs. cost trade-off:

```toml
[sql_generation]
dc_sampling_budget = 8            # Divide-and-conquer samples
skeleton_sampling_budget = 8      # Skeleton-based samples
icl_sampling_budget = 8           # In-context learning samples
```

Lower budgets = faster + cheaper, Higher budgets = more diverse candidates.

### Reproducing Results (DAIL-SQL Few-shots & Predictions)

#### 1. DAIL-SQL Few-shots
For the **In-Context Learning (ICL) Generator**, we utilize few-shot examples selected using the **DAIL-SQL** strategy. To ensure easy reproduction of our results, we have provided the pre-processed few-shot files in the `results/` directory:

- **Spider**: `results/spider_test_few_shots.json`
- **BIRD**: `results/bird_dev_few_shots.json`

To use these files, update the `icl_few_shot_examples_path` in your configuration file:

```toml
[sql_generation]
icl_few_shot_examples_path = "results/spider_test_few_shots.json"
```

#### 2. Experimental Predictions
We have also uploaded the final prediction SQL files generated during our experiments. You can find them in the following directories:
- `results/spider-test/` (JSON format)
- `results/bird-dev/` (JSON format)
- `results/spider2-lite/` (SQL files)
- `results/spider2-snow/` (SQL files)

These files can be used to directly run the evaluation script to verify our reported performance:

```bash
# Example 1: Evaluate BIRD predictions
uv run runner/evaluation.py --dataset_type bird --split dev --sql_output_dir results/bird-dev/qwen3-coder-30b-a3b.json

# Example 2: Evaluate Spider2-Lite predictions
uv run runner/evaluation.py --dataset_type spider2 --split lite --sql_output_dir results/spider2-lite/deepseek-r1/
```


### Resume from Checkpoint

DeepEye-SQL automatically saves progress. If interrupted, simply rerun the script:

```bash
# Pipeline will resume from the last completed stage
bash script/run_pipeline.sh
```

---

## 🐛 Troubleshooting

### Common Issues

**1. API Rate Limit Errors**
- Reduce `n_parallel` in your config
- Add delays between requests
- Use a higher-tier API plan

**2. Out of Memory**
- Reduce `batch_size` in vector database config
- Process fewer samples with `max_samples` parameter
- Use a machine with more RAM

**3. Slow Execution**
- Enable parallel processing: set `n_parallel > 1`
- Use local vector database instead of API-based embedding
- Cache intermediate results (automatically handled)

**4. Spider2 Cloud Database Connection Errors**
- Verify credential files are correct
- Check network connectivity to cloud providers
- Ensure your account has necessary permissions

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Spider dataset: [Yu et al., 2018](https://arxiv.org/abs/1809.08887)
- BIRD dataset: [Li et al., 2023](https://arxiv.org/abs/2305.03111)
- Spider2.0 dataset: [Lei et al., 2024](https://arxiv.org/abs/2411.07763)

---


<div align="center">

**Made with ❤️ by the DeepEye-SQL Team**

[⬆ Back to Top](#-deepeye-sql)

</div>
