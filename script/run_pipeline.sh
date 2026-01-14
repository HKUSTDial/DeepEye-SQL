#!/bin/bash

# Set the project root to the directory where the script is located's parent
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_ROOT"

# # Default parallel workers for vector db creation
# N_PARALLEL=4

echo "Starting the Beta-SQL full pipeline..."
echo "Project Root: $PROJECT_ROOT"
# echo "Parallel workers for Vector DB: $N_PARALLEL"

# 1. Dataset Preprocessing
echo "Step 1: Dataset Preprocessing..."
uv run runner/preprocess_dataset.py
if [ $? -ne 0 ]; then echo "Preprocessing failed!"; exit 1; fi

# 2. Create Vector Database
echo "Step 2: Creating Vector Database (Parallel)..."
uv run runner/create_vector_db_parallel.py
if [ $? -ne 0 ]; then echo "Vector DB creation failed!"; exit 1; fi

# 3. Value Retrieval
echo "Step 3: Value Retrieval..."
uv run runner/run_value_retrieval.py
if [ $? -ne 0 ]; then echo "Value retrieval failed!"; exit 1; fi

# 4. Schema Linking
echo "Step 4: Schema Linking..."
uv run runner/run_schema_linking.py
if [ $? -ne 0 ]; then echo "Schema linking failed!"; exit 1; fi

# 5. SQL Generation
echo "Step 5: SQL Generation..."
uv run runner/run_sql_generation.py
if [ $? -ne 0 ]; then echo "SQL generation failed!"; exit 1; fi

# 6. SQL Revision
echo "Step 6: SQL Revision..."
uv run runner/run_sql_revision.py
if [ $? -ne 0 ]; then echo "SQL revision failed!"; exit 1; fi

# 7. SQL Selection
echo "Step 7: SQL Selection..."
uv run runner/run_sql_selection.py
if [ $? -ne 0 ]; then echo "SQL selection failed!"; exit 1; fi

echo "Full pipeline completed successfully!"

# Optional: Evaluation (uncomment if needed)
# echo "Step 8: Final Evaluation..."
# uv run runner/evaluation.py
