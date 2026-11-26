#!/bin/bash

set -e

TASK_NAME="fractal_tasks_mock_pixi"
TASK_VERSION="0.0.1"

OUTPUT_FILE="$(pwd)/${TASK_NAME}-${TASK_VERSION}.tar.gz"
if [ -e "${OUTPUT_FILE}" ]; then
    echo "ERROR: File ${OUTPUT_FILE} already exists."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_TASK="${SCRIPT_DIR}/../tests/v2/fractal_tasks_mock"
PIXI_TASK="$(dirname ${ORIGINAL_TASK})/${TASK_NAME}-${TASK_VERSION}"

# Fail if the temporary folder already exists (without removing it),
# otherwise remove it on exit, also if a command fails
if [ -e "${PIXI_TASK}" ]; then
    echo "ERROR: Folder $(realpath ${PIXI_TASK}) already exists."
    exit 1
fi
trap 'rm -rf "${PIXI_TASK}"' EXIT

cp -r ${ORIGINAL_TASK} ${PIXI_TASK}

# Rename the package from `fractal_tasks_mock` to `${TASK_NAME}`
mv ${PIXI_TASK}/src/fractal_tasks_mock ${PIXI_TASK}/src/${TASK_NAME}
# Replace the package name in `pyproject.toml` and in the Python imports
sed -i \
    -e "s/fractal_tasks_mock/${TASK_NAME}/g" \
    -e "s/fractal-tasks-mock/${TASK_NAME//_/-}/g" \
    ${PIXI_TASK}/pyproject.toml \
    $(find ${PIXI_TASK}/src -name "*.py")
# Set the package version in `pyproject.toml`
sed -i \
    "s/^version = \".*\"/version = \"${TASK_VERSION}\"/" \
    ${PIXI_TASK}/pyproject.toml

# `pixi init` + `pixi lock`
if ! pixi init --format pyproject ${PIXI_TASK}; then
    echo "ERROR: 'pixi init' failed (is '${TASK_VERSION}' a valid version?)."
    exit 1
fi
pixi lock --manifest-path ${PIXI_TASK}

tar \
    --exclude='.*' \
    --exclude="__pycache__" \
    --exclude="*.egg-info" \
    --exclude="venv" \
    --exclude="dist" \
    --exclude="*.sh" \
    -C $(dirname ${PIXI_TASK}) \
    -vcz \
    -f ${OUTPUT_FILE} \
    $(basename ${PIXI_TASK})/

echo "Successfully created ${OUTPUT_FILE}."
