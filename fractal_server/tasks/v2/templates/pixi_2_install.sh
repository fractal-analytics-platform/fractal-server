set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task-pixi, ${TIMESTAMP}] ${1}"
}

# Replacements
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"
FROZEN_OPTION="__FROZEN_OPTION__"

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

TIME_START=$(date +%s)

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

FROZEN_FLAG=""
if [[ "${FROZEN_OPTION}" == "true" ]]; then
  FROZEN_FLAG="--frozen"
fi

write_log "START '${PIXI_EXECUTABLE} install ${FROZEN_FLAG} --manifest-path ${PYPROJECT_TOML}'"
${PIXI_EXECUTABLE} install ${FROZEN_FLAG} --manifest-path "${PYPROJECT_TOML}"
write_log "END   '${PIXI_EXECUTABLE} install ${FROZEN_FLAG} --manifest-path ${PYPROJECT_TOML}'"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
