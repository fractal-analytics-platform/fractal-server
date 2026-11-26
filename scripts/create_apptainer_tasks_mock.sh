#!/bin/bash

set -e

TASK_NAME="fractal_pixi_tasks_mock"
TASK_VERSION="0.0.1"
OUTPUT_DIR="."

if [ ! -d "${OUTPUT_DIR}" ]; then
    echo "ERROR: Output directory '${OUTPUT_DIR}' does not exist."
    exit 1
fi

OUTPUT_FILE="$(cd "${OUTPUT_DIR}" && pwd)/${TASK_NAME}-${TASK_VERSION}.tar.gz"
if [ -e "${OUTPUT_FILE}" ]; then
    echo "ERROR: File ${OUTPUT_FILE} already exists."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_TASK="${SCRIPT_DIR}/../tests/v2/fractal_tasks_mock"
APPTAINER_TASK="$(dirname ${ORIGINAL_TASK})/${TASK_NAME}-${TASK_VERSION}"

# Fail if the temporary folder already exists (without removing it),
# otherwise remove it on exit, also if a command fails
if [ -e "${APPTAINER_TASK}" ]; then
    echo "ERROR: Folder $(realpath ${APPTAINER_TASK}) already exists."
    exit 1
fi
trap 'rm -rf "${APPTAINER_TASK}"' EXIT

cp -r ${ORIGINAL_TASK} ${APPTAINER_TASK}

# Rename the package from `fractal_tasks_mock` to `${TASK_NAME}`
mv ${APPTAINER_TASK}/src/fractal_tasks_mock ${APPTAINER_TASK}/src/${TASK_NAME}
# Replace the package name in `pyproject.toml` and in the Python imports
sed -i \
    -e "s/fractal_tasks_mock/${TASK_NAME}/g" \
    -e "s/fractal-tasks-mock/${TASK_NAME//_/-}/g" \
    ${APPTAINER_TASK}/pyproject.toml \
    $(find ${APPTAINER_TASK}/src -name "*.py")
# Set the package version in `pyproject.toml`
sed -i \
    "s/^version = \".*\"/version = \"${TASK_VERSION}\"/" \
    ${APPTAINER_TASK}/pyproject.toml

# `pixi init` + `pixi lock`
if ! pixi init --format pyproject ${APPTAINER_TASK}; then
    echo "ERROR: 'pixi init' failed (is '${TASK_VERSION}' a valid version?)."
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

echo "Successfully created ${OUTPUT_FILE}."
