#!/bin/bash

# ==============================================================================
# DeepEye-SQL General Pipeline Automation Script
# ==============================================================================
# This script runs the full pipeline from preprocessing to SQL selection.
# 
# Usage: 
#   CONFIG_PATH="config/your_config.toml" bash script/run_pipeline.sh
# or
#   bash script/run_pipeline.sh config/your_config.toml
# ==============================================================================

# Set the project root to the directory where the script is located's parent
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_ROOT"

# Set CONFIG_PATH if provided as an argument
if [ ! -z "$1" ]; then
    export CONFIG_PATH="$1"
fi

# Default CONFIG_PATH if not set
if [ -z "$CONFIG_PATH" ]; then
    export CONFIG_PATH="config/config.toml"
fi

# Create logs directory if it doesn't exist
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# Set log file name with timestamp
LOG_FILE="$LOG_DIR/pipeline_$(date +'%Y%m%d_%H%M%S').log"

# Redirect stdout and stderr to both the console and the log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=============================================================================="
echo "Starting the DeepEye-SQL Pipeline..."
echo "Project Root: $PROJECT_ROOT"
echo "Config Path:  $CONFIG_PATH"
echo "Log File:     $LOG_FILE"
echo "=============================================================================="

# 1. Dataset Preprocessing
echo -e "\nStep 1: Dataset Preprocessing..."
uv run runner/preprocess_dataset.py
if [ $? -ne 0 ]; then echo "Preprocessing failed!"; exit 1; fi

# 2. Create Vector Database
echo -e "\nStep 2: Creating Vector Database (Parallel)..."
uv run runner/create_vector_db_parallel.py
if [ $? -ne 0 ]; then echo "Vector DB creation failed!"; exit 1; fi

# 3. Value Retrieval
echo -e "\nStep 3: Value Retrieval..."
uv run runner/run_value_retrieval.py
if [ $? -ne 0 ]; then echo "Value retrieval failed!"; exit 1; fi

# 4. Dynamic Few-shot Preparation
if [ "${SKIP_FEW_SHOT_PREPARATION:-0}" = "1" ]; then
    echo -e "\nStep 4: Dynamic Few-shot Preparation skipped (SKIP_FEW_SHOT_PREPARATION=1)."
else
    FEW_SHOT_READY=$(uv run python - <<'PY'
from app.config import get_config

cfg = get_config()
ready = (
    cfg.dataset_config.type in {"bird", "spider"}
    and cfg.few_shot_index_config.embedding is not None
    and cfg.few_shot_index_config.llm is not None
)
print("1" if ready else "0")
PY
)
    if [ "$FEW_SHOT_READY" = "1" ]; then
        echo -e "\nStep 4a: Building Few-shot Training Index..."
        uv run runner/build_few_shot_index.py
        if [ $? -ne 0 ]; then echo "Few-shot index build failed!"; exit 1; fi

        echo -e "\nStep 4b: Dynamic Few-shot Preparation..."
        uv run runner/run_few_shot_preparation.py
        if [ $? -ne 0 ]; then echo "Few-shot preparation failed!"; exit 1; fi
    else
        echo -e "\nStep 4: Dynamic Few-shot Preparation skipped (unsupported dataset or missing [few_shot_index] model config)."
    fi
fi

# 5. Schema Linking
echo -e "\nStep 5: Schema Linking..."
uv run runner/run_schema_linking.py
if [ $? -ne 0 ]; then echo "Schema linking failed!"; exit 1; fi

# 6. SQL Generation
echo -e "\nStep 6: SQL Generation..."
uv run runner/run_sql_generation.py
if [ $? -ne 0 ]; then echo "SQL generation failed!"; exit 1; fi

# 7. SQL Revision
echo -e "\nStep 7: SQL Revision..."
uv run runner/run_sql_revision.py
if [ $? -ne 0 ]; then echo "SQL revision failed!"; exit 1; fi

# 8. SQL Selection
echo -e "\nStep 8: SQL Selection..."
uv run runner/run_sql_selection.py
if [ $? -ne 0 ]; then echo "SQL selection failed!"; exit 1; fi

echo -e "\n=============================================================================="
echo "Pipeline completed successfully!"
echo "=============================================================================="
