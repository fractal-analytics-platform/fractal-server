#!/bin/bash

set -eu

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[after-build-apptainer, ${TIMESTAMP}] ${1}"
}

# Replacements
APPTAINER_BIN="__APPTAINER_BIN__"
PACKAGE_DIR="__PACKAGE_DIR__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"
CONTAINER_ROOT="__CONTAINER_ROOT__"
SIF_PATH="__SIF_PATH__"
IMPORT_PACKAGE_NAME="__IMPORT_PACKAGE_NAME__"
PYTHON_COMMAND="__PYTHON_COMMAND__"
MANIFEST_FILENAME="__MANIFEST_FILENAME__"
MANIFEST_PATH="__MANIFEST_PATH__"
PIXI_LOCK_PATH="__PIXI_LOCK_PATH__"

# Strip trailing `/`
PACKAGE_DIR=${PACKAGE_DIR%/}
CONTAINER_ROOT=${CONTAINER_ROOT%/}

# Known paths (inside the container)
CONTAINER_SOURCE_DIR="${CONTAINER_ROOT}/${SOURCE_DIR_NAME}"

TIME_START=$(date +%s)

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

# Python command (runs Python inside the container)
write_log "Python command: ${PYTHON_COMMAND}"
write_log "Python version: $(${PYTHON_COMMAND} --version)"
echo

# Find PACKAGE_FOLDER (a path inside the container)
PACKAGE_FOLDER=$(${PYTHON_COMMAND} -c "import ${IMPORT_PACKAGE_NAME} as p, os; print(os.path.dirname(p.__file__))")
write_log "Package folder: ${PACKAGE_FOLDER}"
echo

# Copy manifest and lockfile out of the image
write_log "START copying manifest to ${MANIFEST_PATH}"
${APPTAINER_BIN} exec "${SIF_PATH}" cat "${PACKAGE_FOLDER}/${MANIFEST_FILENAME}" > "${MANIFEST_PATH}"
write_log "END   copying manifest to ${MANIFEST_PATH}"
write_log "START copying pixi.lock to ${PIXI_LOCK_PATH}"
${APPTAINER_BIN} exec "${SIF_PATH}" cat "${CONTAINER_SOURCE_DIR}/pixi.lock" > "${PIXI_LOCK_PATH}"
write_log "END   copying pixi.lock to ${PIXI_LOCK_PATH}"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
