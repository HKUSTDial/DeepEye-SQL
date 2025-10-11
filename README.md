# Symph-SQL: A Software-Grounded Framework for Robust Text-to-SQL Generation

Symph-SQL is a systematic and modular framework that, inspired by established software engineering principles, orchestrates a pipeline of specialized components to collaboratively address the challenges in Text-to-SQL generation. The framework enhances semantic grounding through semantic value retrieval and robust schema linking, employs a hybrid of three distinct generation methods to produce diverse SQL candidates, implements a novel tool-based revision process for automated testing and debugging, and proposes a confidence-aware selection mechanism that integrates execution-based confidence scores with LLM voting to select the best candidates.

## Project Structure

```
Symph-SQL/
├── app/                    # Core application code
│   ├── config/            # Configuration management
│   ├── dataset/           # Dataset processing
│   ├── db_utils/          # Database utilities
│   ├── llm/               # Large language model interface
│   ├── logger/            # Logging utilities
│   ├── pipeline/          # Processing pipeline
│   │   ├── value_retrieval/    # Value retrieval
│   │   ├── schema_linking/     # Schema linking
│   │   ├── sql_generation/     # SQL generation
│   │   ├── sql_revision/       # SQL revision
│   │   └── sql_selection/      # SQL selection
│   ├── vector_db/         # Vector database
│   └── prompt/            # Prompt templates
├── config/                # Configuration files
├── data/                  # Datasets
├── runner/                # Runner scripts
├── script/                # Installation and download scripts
└── workspace/             # Workspace (output directory)
```

## Requirements

- Python >= 3.12
- CUDA-compatible GPU (recommended)
- Sufficient disk space for datasets and vector databases

## Installation

### 1. Clone the Project

```bash
git clone <repository-url>
cd Symph-SQL
```

### 2. Install Dependencies

Use the provided installation script:

```bash
bash script/install_env.sh
```

Or install manually:

```bash
# Install uv package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### 3. Download Datasets

Run the download script to automatically download BIRD and Spider datasets:

```bash
bash script/download_dataset.sh
```

This will download:
- BIRD dev dataset to `data/bird/dev/`
- Spider test dataset to `data/spider/`

## Configuration

### 1. Copy Configuration File

```bash
cp config/config-example.toml config/config.toml
```

### 2. Modify Configuration

Edit `config/config.toml`. Main configuration sections include:

#### Dataset Configuration
```toml
[dataset]
type = "bird"  # or "spider"
split = "dev"  # or "test"
root_path = "data/bird"
save_path = "workspace/dataset/bird/dev.pkl"
```

#### Vector Database Configuration
```toml
[vector_database]
embedding_model_name_or_path = "Qwen/Qwen3-Embedding-0.6B"
use_qwen3_embedding = true
local_files_only = false  # set to true if using local models
store_root_path = "workspace/vector_database/bird/dev"
```

#### LLM Configuration
Each processing stage requires LLM configuration:

```toml
[value_retrieval.llm]
model = "Qwen3-Coder-30B-A3B-Instruct"
base_url = "http://0.0.0.0:8000/v1"  # modify to your LLM service address
api_key = "dummy"  # modify to your API key
max_tokens = 4096
temperature = 0.7
api_type = "openai"
```

**Important**: Please modify the following parameters according to your LLM service configuration:
- `base_url`: API address of your LLM service
- `api_key`: Your API key
- `model`: Name of the model to use

## Running the Project

### Complete Pipeline Execution

The project uses a multi-stage processing pipeline. Execute the following steps in order:

#### 1. Preprocess Dataset

```bash
uv run runner/preprocess_dataset.py
```

#### 2. Create Vector Database

```bash
bash script/make_vector_db.sh
```

#### 3. Value Retrieval

```bash
uv run runner/run_value_retrieval.py
```

#### 4. Schema Linking

```bash
uv run runner/run_schema_linking.py
```

#### 5. SQL Generation

```bash
uv run runner/run_sql_generation.py
```

#### 6. SQL Revision

```bash
uv run runner/run_sql_revision.py
```

#### 7. SQL Selection

```bash
uv run runner/run_sql_selection.py
```

#### 8. Convert Result File

Convert the final SQL selection results to a readable JSON format:

```bash
uv run runner/convert_pkl_to_sql_file.py
```

#### 9. Run Evaluation

Evaluate the generated SQL statements against the ground truth:

```bash
uv run runner/evaluation.py
```

## Output Files

After completion, results will be saved in the `workspace/` directory:

```
workspace/
├── dataset/               # Preprocessed datasets
├── vector_database/       # Vector databases
├── value_retrieval/       # Value retrieval results
├── schema_linking/        # Schema linking results
├── sql_generation/        # SQL generation results
├── sql_revision/          # SQL revision results
├── sql_selection/         # SQL selection results
└── logs/                  # Log files
```

## Configuration Parameters

### Main Configuration Parameters

1. **Parallel Processing Parameters**:
   - `n_parallel`: Number of parallel processes
   - `n_results`: Number of retrieval results

2. **Sampling Budget Parameters**:
   - `dc_sampling_budget`: Data constraint sampling budget
   - `skeleton_sampling_budget`: Skeleton sampling budget
   - `icl_sampling_budget`: In-context learning sampling budget

3. **Threshold Parameters**:
   - `value_distance_threshold`: Value distance threshold
   - `shortcut_consistency_score_threshold`: Consistency score threshold

### Performance Results

With the Qwen3-Coder-30B-A3B model, Symph-SQL achieves state-of-the-art performance:
- **BIRD-Dev**: 73.5% execution accuracy
- **Spider-Test**: 89.8% execution accuracy

These results are achieved without any model fine-tuning, demonstrating the effectiveness of the software-grounded framework approach.

## Logging

Log files are located in the `workspace/logs/` directory for detailed runtime information.