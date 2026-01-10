#!/bin/bash
# Compile Training Data Pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."
PIPELINE_FILE="${PROJECT_DIR}/pipelines/training_data_pipeline.py"
OUTPUT_FILE="${PROJECT_DIR}/pipelines/training_data_pipeline.yaml"

echo "=== Compiling Training Data Pipeline ==="

if python -c "import kfp" 2>/dev/null; then
    echo "Using existing kfp installation"
    python "${PIPELINE_FILE}"
else
    echo "kfp not installed, using temporary venv..."
    TEMP_VENV=$(mktemp -d)/kfp-venv
    python -m venv "${TEMP_VENV}"
    source "${TEMP_VENV}/bin/activate"
    pip install -q "kfp==2.7.0" "pyyaml" "kubernetes"
    python "${PIPELINE_FILE}"
    deactivate
    rm -rf "${TEMP_VENV}"
fi

if [ -f "${OUTPUT_FILE}" ]; then
    echo "✓ Pipeline compiled successfully: ${OUTPUT_FILE}"
    echo "  Upload this YAML to Kubeflow Dashboard to run."
else
    echo "✗ Pipeline compilation failed"
    exit 1
fi
