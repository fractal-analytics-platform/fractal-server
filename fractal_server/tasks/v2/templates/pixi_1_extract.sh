set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task-pixi, ${TIMESTAMP}] ${1}"
}

# Replacements
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
TAR_GZ_PATH="__TAR_GZ_PATH__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"

# Strip trailing `/` from `PACKAGE_DIR`
PIXI_HOME=${PIXI_HOME%/}
PACKAGE_DIR=${PACKAGE_DIR%/}

# Known paths
SOURCE_DIR="${PACKAGE_DIR}/${SOURCE_DIR_NAME}"
TAR_GZ_BASENAME=$(basename "${TAR_GZ_PATH}" ".tar.gz")

# Pixi env variable
export PIXI_HOME="${PIXI_HOME}"
export PIXI_CACHE_DIR="${PIXI_HOME}/cache"
export RATTLER_AUTH_FILE="${PIXI_HOME}/credentials.json"


TIME_START=$(date +%s)

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

write_log "START 'tar xz -f ${TAR_GZ_PATH} ${TAR_GZ_BASENAME}'"
tar xz -f "${TAR_GZ_PATH}" "${TAR_GZ_BASENAME}"
write_log "END   'tar xz -f ${TAR_GZ_PATH} ${TAR_GZ_BASENAME}'"
echo

write_log "START 'mv ${PACKAGE_DIR}/${TAR_GZ_BASENAME} ${SOURCE_DIR}'"
mv "${PACKAGE_DIR}/${TAR_GZ_BASENAME}" "${SOURCE_DIR}"
write_log "END   'mv ${PACKAGE_DIR}/${TAR_GZ_BASENAME} ${SOURCE_DIR}'"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
