#!/bin/bash

set -e

if [ -z "${1}" ]; then
    echo "Usage: ${0} VERSION [OUTPUT_DIR]"
    echo ""
    echo "Build fractal_tasks_mock-VERSION.tar.gz, to test apptainer task collection."
    echo ""
    echo "Arguments:"
    echo "  VERSION     Version of fractal-tasks-mock (required)"
    echo "  OUTPUT_DIR  Directory where the tar.gz file is written (default: \".\")"
    exit 1
fi

FRACTAL_TASKS_MOCK_VERSION="$1"
OUTPUT_DIR="${2:-.}"

if [ ! -d "${OUTPUT_DIR}" ]; then
    echo "ERROR: Output directory '${OUTPUT_DIR}' does not exist."
    exit 1
fi
OUTPUT_FILE="$(cd "${OUTPUT_DIR}" && pwd)/fractal_tasks_mock-${FRACTAL_TASKS_MOCK_VERSION}.tar.gz"
if [ -e "${OUTPUT_FILE}" ]; then
    echo "ERROR: File ${OUTPUT_FILE} already exists."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_TASK="${SCRIPT_DIR}/../tests/v2/fractal_tasks_mock"
APPTAINER_TASK="${ORIGINAL_TASK}-${FRACTAL_TASKS_MOCK_VERSION}"

# Remove the temporary folder on exit, also if a command fails
trap 'rm -rf "${APPTAINER_TASK}"' EXIT

cp -r ${ORIGINAL_TASK} ${APPTAINER_TASK}

sed -i \
    "s/^version = \".*\"/version = \"${FRACTAL_TASKS_MOCK_VERSION}\"/" \
    ${APPTAINER_TASK}/pyproject.toml

if ! pixi init --format pyproject ${APPTAINER_TASK}; then
    echo "ERROR: 'pixi init' failed (is '${FRACTAL_TASKS_MOCK_VERSION}' a valid version?)."
    exit 1
fi
pixi lock --manifest-path ${APPTAINER_TASK}

tar \
    --exclude='.*' \
    --exclude="__pycache__" \
    --exclude="*.egg-info" \
    --exclude="venv" \
    --exclude="dist" \
    --exclude="*.sh" \
    -C $(dirname ${APPTAINER_TASK}) \
    -vcz \
    -f ${OUTPUT_FILE} \
    $(basename ${APPTAINER_TASK})/
