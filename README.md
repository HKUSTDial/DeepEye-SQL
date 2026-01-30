# DeepEye-SQL: A Software-Engineering-Inspired Text-to-SQL Framework

DeepEye-SQL is a Text-to-SQL framework inspired by software engineering principles. Rather than treating Text-to-SQL as free-form language generation, it reframes the process as the development of a small software program, following a structured Software Development Life Cycle (SDLC). By integrating semantic value retrieval, robust schema linking, N-version SQL generation, unit test verification, and game-theory-based pairwise SQL selection, DeepEye-SQL achieves state-of-the-art performance on BIRD and Spider datasets without any fine-tuning.

> **Note**: This framework was originally named Symph-SQL and has been renamed to DeepEye-SQL.

## Project Structure

```text
DeepEye-SQL/
├── app/                    # Core application code
│   ├── config/            # Configuration management
│   ├── dataset/           # Dataset loading and processing
│   ├── db_utils/          # Database execution and schema parsing utilities
│   ├── llm/               # LLM interface (OpenAI-compatible)
│   ├── pipeline/          # SDLC stage implementations
│   │   ├── value_retrieval/    # Value retrieval (Grounding)
│   │   ├── schema_linking/     # Schema linking (Reasoning)
│   │   ├── sql_generation/     # SQL generation (Implementation)
│   │   ├── sql_revision/       # SQL revision (Testing/Debugging)
│   │   └── sql_selection/      # SQL selection (Deployment/QA)
│   ├── vector_db/         # Vector database operations
│   └── prompt/            # Prompt template library
├── config/                # Configuration files
├── runner/                # Entry point scripts
├── script/                # Automation pipelines and installation scripts
└── workspace/             # Workspace (Outputs, indices, and logs)
```

## Installation

### 1. Clone the Project
```bash
git clone git@github.com:BugMaker-Boyan/Symph-SQL.git
cd Beta-SQL
```

### 2. Install Dependencies
This project uses `uv` for package management to ensure fast, reproducible environments:
```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync environment
uv sync
```

### 3. Download Datasets
Run the built-in script to automatically download and prepare the BIRD (Dev) and Spider datasets:
```bash
bash script/download_dataset.sh
```

## Configuration

Before running the pipeline, ensure your configuration is set up:
1.  **Copy Template**: `cp config/config-example.toml config/config.toml`
2.  **Configure LLM**: Edit `config/config.toml` and provide your `base_url`, `api_key`, and `model`.
3.  **Adjust Parallelism**: Modify `n_parallel` in the configuration sections based on your hardware and API limits.

## Running the Project

### Method A: Full Automated Pipeline (Recommended)
We provide an integrated script that executes all steps from preprocessing to SQL selection in order:
```bash
bash script/run_pipeline.sh
```

### Method B: Manual Step-by-Step Execution
If you need to debug or run specific stages, you can execute them individually:

1.  **Dataset Preprocessing**
    ```bash
    uv run runner/preprocess_dataset.py
    ```
2.  **Build Vector Database** (Parallelized version)
    ```bash
    uv run runner/create_vector_db_parallel.py
    ```
3.  **Value Retrieval**
    ```bash
    uv run runner/run_value_retrieval.py
    ```
4.  **Schema Linking**
    ```bash
    uv run runner/run_schema_linking.py
    ```
5.  **SQL Generation**
    ```bash
    uv run runner/run_sql_generation.py
    ```
6.  **SQL Revision** (Self-correction based on execution feedback)
    ```bash
    uv run runner/run_sql_revision.py
    ```
7.  **SQL Selection** (Pairwise adjudication for final selection)
    ```bash
    uv run runner/run_sql_selection.py
    ```

## Evaluation

Once the pipeline completes, convert the results to a standard format and run the evaluator:
```bash
# Convert to JSON format
uv run runner/convert_pkl_to_sql_file.py

# Run evaluation
uv run runner/evaluation.py
```

## Performance Results

Using **Qwen3-Coder-30B** as the backbone model (without any fine-tuning):
*   **BIRD-Dev**: 73.5% Execution Accuracy (EX)
*   **Spider-Test**: 89.8% Execution Accuracy (EX)

## Logging & Debugging
All runtime logs are stored in `workspace/logs/`. If the pipeline is interrupted by API rate limits or timeouts, the framework supports checkpoints—simply re-run the corresponding script to resume.


## To Be Released
- Pre-build Vector Databases for BIRD/Spider
- Evaluation Results/Scores/Tables on BIRD/Spider/Spider2.0