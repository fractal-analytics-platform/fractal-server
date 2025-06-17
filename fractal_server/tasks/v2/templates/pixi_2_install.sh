set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[install-tasks-pixi, ${TIMESTAMP}] ${1}"
}

# Replacements
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"
FROZEN_OPTION="__FROZEN_OPTION__"
TOKIO_WORKER_THREADS="__TOKIO_WORKER_THREADS__"
PIXI_CONCURRENT_SOLVES="__PIXI_CONCURRENT_SOLVES__"
PIXI_CONCURRENT_DOWNLOADS="__PIXI_CONCURRENT_DOWNLOADS__"

# Strip trailing `/` from `PACKAGE_DIR`
PIXI_HOME=${PIXI_HOME%/}
PACKAGE_DIR=${PACKAGE_DIR%/}

# Known paths
PIXI_EXECUTABLE="${PIXI_HOME}/bin/pixi"
SOURCE_DIR="${PACKAGE_DIR}/${SOURCE_DIR_NAME}"
PYPROJECT_TOML="${SOURCE_DIR}/pyproject.toml"

# Pixi env variable
export PIXI_HOME="${PIXI_HOME}"
export PIXI_CACHE_DIR="${PIXI_HOME}/cache"
export RATTLER_AUTH_FILE="${PIXI_HOME}/credentials.json"
export TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS}"

TIME_START=$(date +%s)

write_log "Hostname: $(hostname)"

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

write_log "START '${PIXI_EXECUTABLE} install ${FROZEN_OPTION} --manifest-path ${PYPROJECT_TOML}'"
${PIXI_EXECUTABLE} install \
    --concurrent-solves "${PIXI_CONCURRENT_SOLVES}" \
    --concurrent-downloads "${PIXI_CONCURRENT_DOWNLOADS}" \
    ${FROZEN_OPTION} --manifest-path "${PYPROJECT_TOML}"
write_log "END   '${PIXI_EXECUTABLE} install ${FROZEN_OPTION} --manifest-path ${PYPROJECT_TOML}'"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
