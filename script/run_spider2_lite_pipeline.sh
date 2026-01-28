#!/bin/bash

# ==============================================================================
# Spider2-Lite Full Pipeline Automation Script
# ==============================================================================
# Usage: 
#   CONFIG_PATH="config/your_config.toml" bash script/run_spider2_lite_pipeline.sh
# or
#   bash script/run_spider2_lite_pipeline.sh config/your_config.toml
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

echo "=============================================================================="
echo "Starting the Beta-SQL Spider2-Lite Pipeline..."
echo "Project Root: $PROJECT_ROOT"
echo "Config Path:  $CONFIG_PATH"
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

# 4. Schema Linking
echo -e "\nStep 4: Schema Linking..."
uv run runner/run_schema_linking.py
if [ $? -ne 0 ]; then echo "Schema linking failed!"; exit 1; fi

# 5. SQL Generation
echo -e "\nStep 5: SQL Generation..."
uv run runner/run_sql_generation.py
if [ $? -ne 0 ]; then echo "SQL generation failed!"; exit 1; fi

# 6. SQL Revision
echo -e "\nStep 6: SQL Revision..."
uv run runner/run_sql_revision.py
if [ $? -ne 0 ]; then echo "SQL revision failed!"; exit 1; fi

# 7. SQL Selection
echo -e "\nStep 7: SQL Selection..."
uv run runner/run_sql_selection.py
if [ $? -ne 0 ]; then echo "SQL selection failed!"; exit 1; fi

# 8. Final Evaluation
echo -e "\nStep 8: Spider2-Lite Evaluation..."
uv run runner/evaluation_spider2.py --dataset_type spider2-lite
if [ $? -ne 0 ]; then echo "Evaluation failed!"; exit 1; fi

echo -e "\n=============================================================================="
echo "Spider2-Lite Pipeline completed successfully!"
echo "=============================================================================="
