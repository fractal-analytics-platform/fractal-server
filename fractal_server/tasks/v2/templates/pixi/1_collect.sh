set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task-pixi, $TIMESTAMP] $1"
}

# Absolute paths
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
TAR_GZ_PATH="__TAR_GZ_PATH__"
# Names
PACKAGE_NAME="__PACKAGE_NAME__"

PIXI_EXECUTABLE="${PIXI_HOME}/bin/pixi"
SOURCE_DIR="${PACKAGE_DIR}/source_dir"
PYPROJECT_TOML="${SOURCE_DIR}/pyproject.toml"

export PIXI_HOME="${PIXI_HOME}"
export PIXI_CACHE_DIR="${PIXI_HOME}/cache"
export RATTLER_AUTH_FILE="${PIXI_HOME}/credentials.json"


TIME_START=$(date +%s)

ls -lh "$TAR_GZ_PATH"
write_log "START extract $TAR_GZ_PATH"


TAR_GZ_BASENAME=$(basename "$TAR_GZ_PATH" ".tar.gz")
echo "TAR_GZ_BASENAME: $TAR_GZ_BASENAME"
tar -xz -v -f "${TAR_GZ_PATH}" "$TAR_GZ_BASENAME"
ls -lh "$PACKAGE_DIR"
echo
ls -lh "$PACKAGE_DIR/*"
echo
mv "${PACKAGE_DIR}/${TAR_GZ_BASENAME}" "$SOURCE_DIR"  # FIXME: improve concatenation
ls -lh "$PACKAGE_DIR"
echo
ls -lh "$PACKAGE_DIR/*"
echo
write_log "END extract $TAR_GZ_PATH"
TIME_END_TAR=$(date +%s)
write_log "Elapsed: $((TIME_END_TAR - TIME_START)) seconds"
echo

write_log "START $PIXI_EXECUTABLE install --manifest-path $PYPROJECT_TOML"
${PIXI_EXECUTABLE} install --manifest-path "$PYPROJECT_TOML"
write_log "END $PIXI_EXECUTABLE install --manifest-path $PYPROJECT_TOML"
echo

PACKAGE_FOLDER=$(
    ${PIXI_EXECUTABLE} run --manifest-path "${PYPROJECT_TOML}" python \
    -c "import ${PACKAGE_NAME} as p, os; print(os.path.dirname(p.__file__))"
)
write_log "Package folder: $PACKAGE_FOLDER"
echo

ENV_DISK_USAGE=$(du -sk "${PACKAGE_DIR}" | cut -f1)
ENV_FILE_NUMBER=$(find "${PACKAGE_DIR}" -type f | wc -l)

write_log "Disk usage: $ENV_DISK_USAGE"
write_log "Number of files: $ENV_FILE_NUMBER"

write_log "All ok, exit."
