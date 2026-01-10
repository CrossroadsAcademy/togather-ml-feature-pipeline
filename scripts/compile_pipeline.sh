#!/bin/bash
# Compile Kubeflow Pipeline
# Compiles the batch_feature_pipeline.py to YAML for upload to Kubeflow.
# Uses a temporary venv to avoid dependency conflicts with main project.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."
PIPELINE_FILE="${PROJECT_DIR}/pipelines/batch_feature_pipeline.py"
OUTPUT_FILE="${PROJECT_DIR}/pipelines/batch_feature_pipeline.yaml"

echo "=== Compiling Kubeflow Pipeline ==="

# Check if kfp is available
if python -c "import kfp" 2>/dev/null; then
    echo "Using existing kfp installation"
    python "${PIPELINE_FILE}"
else
    echo "kfp not installed, using temporary venv..."

    # Create temp venv
    TEMP_VENV=$(mktemp -d)/kfp-venv
    python -m venv "${TEMP_VENV}"

    # Activate and install kfp
    source "${TEMP_VENV}/bin/activate"
    pip install -q "kfp==2.7.0" "pyyaml" "numpy"

    # Run pipeline compilation
    python "${PIPELINE_FILE}"

    # Cleanup
    deactivate
    rm -rf "${TEMP_VENV}"
fi

if [ -f "${OUTPUT_FILE}" ]; then
    echo " Pipeline compiled successfully: ${OUTPUT_FILE}"
else
    echo " Pipeline compilation failed"
    exit 1
fi
