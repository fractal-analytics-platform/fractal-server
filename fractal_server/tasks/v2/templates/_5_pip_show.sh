set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
PACKAGE_NAME=__PACKAGE_NAME__
PACKAGE=__PACKAGE__
PYTHON=__PYTHON__
INSTALL_STRING=__INSTAL_STRING__


TIME_START=$(date +%s)

VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python
write_log "Python interpreter: $VENVPYTHON"
echo

# FIXME: only run pip-show once!

# Extract information about paths
# WARNING: this block will fail for paths which inlcude whitespace characters
write_log "START pip show"
$VENVPYTHON -m pip show ${PACKAGE_NAME}
write_log "END   pip show"
echo
PACKAGE_NAME=$($VENVPYTHON -m pip show ${PACKAGE_NAME} | grep "Name:" | cut -d ":" -f 2 | tr -d "[:space:]")
write_log "Package name: $PACKAGE_NAME"
echo
PACKAGE_VERSION=$($VENVPYTHON -m pip show ${PACKAGE_NAME} | grep "Version:" | cut -d ":" -f 2 | tr -d "[:space:]")
write_log "Package version: $PACKAGE_VERSION"
echo
PACKAGE_PARENT_FOLDER=$($VENVPYTHON -m pip show ${PACKAGE_NAME} | grep "Location:" | cut -d ":" -f 2 | tr -d "[:space:]")
write_log "Package parent folder: $PACKAGE_PARENT_FOLDER"
echo
MANIFEST_RELATIVE_PATH=$($VENVPYTHON -m pip show ${PACKAGE_NAME} --files | grep "__FRACTAL_MANIFEST__.json" | tr -d "[:space:]")
write_log "Manifest relative path: $MANIFEST_RELATIVE_PATH"
echo
MANIFEST_ABSOLUTE_PATH="${PACKAGE_PARENT_FOLDER}/${MANIFEST_RELATIVE_PATH}"
write_log "Manifest absolute path: $MANIFEST_ABSOLUTE_PATH"
echo
if [ -f "$MANIFEST_ABSOLUTE_PATH" ]; then
    write_log "OK: manifest path exists"
    echo
else
    write_log "ERROR: manifest path not found at $MANIFEST_ABSOLUTE_PATH"
    exit 3
fi

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
