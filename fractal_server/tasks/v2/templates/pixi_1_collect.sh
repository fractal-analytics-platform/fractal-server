set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task-pixi, $TIMESTAMP] $1"
}

# Replacements
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
TAR_GZ_PATH="__TAR_GZ_PATH__"
IMPORT_PACKAGE_NAME="__IMPORT_PACKAGE_NAME__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"

# Strip trailing `/` from `PACKAGE_DIR`
PIXI_HOME=${PIXI_HOME%/}
PACKAGE_DIR=${PACKAGE_DIR%/}

# Known paths
PIXI_EXECUTABLE="${PIXI_HOME}/bin/pixi"
SOURCE_DIR="${PACKAGE_DIR}/${SOURCE_DIR_NAME}"
PYPROJECT_TOML="${SOURCE_DIR}/pyproject.toml"
TAR_GZ_BASENAME=$(basename "$TAR_GZ_PATH" ".tar.gz")

# Pixi env variable
export PIXI_HOME="$PIXI_HOME"
export PIXI_CACHE_DIR="${PIXI_HOME}/cache"
export RATTLER_AUTH_FILE="${PIXI_HOME}/credentials.json"


TIME_START=$(date +%s)

cd "$PACKAGE_DIR"
write_log "Changed working directory to $PACKAGE_DIR"


write_log "START 'tar xz -f $TAR_GZ_PATH $TAR_GZ_BASENAME'"
tar xz -f "$TAR_GZ_PATH" "$TAR_GZ_BASENAME"
write_log "END   'tar xz -f $TAR_GZ_PATH $TAR_GZ_BASENAME'"
echo

write_log "START 'mv ${PACKAGE_DIR}/${TAR_GZ_BASENAME} $SOURCE_DIR'"
mv "${PACKAGE_DIR}/${TAR_GZ_BASENAME}" "$SOURCE_DIR"
write_log "END   'mv ${PACKAGE_DIR}/${TAR_GZ_BASENAME} $SOURCE_DIR'"
echo
write_log "END extract $TAR_GZ_PATH"
TIME_END_TAR=$(date +%s)
write_log "Elapsed: $((TIME_END_TAR - TIME_START)) seconds"
echo

write_log "START '$PIXI_EXECUTABLE install --manifest-path $PYPROJECT_TOML'"
${PIXI_EXECUTABLE} install --manifest-path "$PYPROJECT_TOML"
write_log "END   '$PIXI_EXECUTABLE install --manifest-path $PYPROJECT_TOML'"
echo

PACKAGE_FOLDER=$(
    ${PIXI_EXECUTABLE} run --manifest-path "$PYPROJECT_TOML" python \
    -c "import $IMPORT_PACKAGE_NAME as p, os; print(os.path.dirname(p.__file__))"
)
write_log "Package folder: $PACKAGE_FOLDER"
echo

ENV_DISK_USAGE=$(du -sk "${PACKAGE_DIR}" | cut -f1)
ENV_FILE_NUMBER=$(find "${PACKAGE_DIR}" -type f | wc -l)

write_log "Disk usage: $ENV_DISK_USAGE"
write_log "Number of files: $ENV_FILE_NUMBER"

write_log "START chmod 755 $SOURCE_DIR -R"
chmod 755 "$SOURCE_DIR" -R
write_log "END chmod 755 $SOURCE_DIR -R"

write_log "All ok, exit."
echo
