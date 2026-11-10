#!/bin/bash

set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[build-apptainer, ${TIMESTAMP}] ${1}"
}

# Replacements
APPTAINER_BIN="__APPTAINER_BIN__"
PACKAGE_DIR="__PACKAGE_DIR__"
DEF_FILE="__DEF_FILE__"
SIF_PATH="__SIF_PATH__"

# Strip trailing `/`
PACKAGE_DIR=${PACKAGE_DIR%/}

TIME_START=$(date +%s)

write_log "Hostname: $(hostname)"

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

write_log "START '${APPTAINER_BIN} build ${SIF_PATH} ${DEF_FILE}'"
${APPTAINER_BIN} build "${SIF_PATH}" "${DEF_FILE}"
write_log "END   '${APPTAINER_BIN} build ${SIF_PATH} ${DEF_FILE}'"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
