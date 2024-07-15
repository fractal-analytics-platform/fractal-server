set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
PACKAGE_ENV_DIR_TMP=__PACKAGE_ENV_DIR_TMP__
PYTHON=__PYTHON__

TIME_START=$(date +%s)

# Check that non-temporary folder does not exist
if [ -d "$PACKAGE_ENV_DIR" ]; then
    write_log "ERROR: Folder $PACKAGE_ENV_DIR already exists. Exit."
    exit 1
fi

# Create temporary folder
if [ -d "$PACKAGE_ENV_DIR_TMP" ]; then
    write_log "ERROR: Folder $PACKAGE_ENV_DIR_TMP already exists. Exit."
    exit 1
fi
write_log "START mkdir -p $PACKAGE_ENV_DIR_TMP"
mkdir -p $PACKAGE_ENV_DIR_TMP
write_log "END   mkdir -p $PACKAGE_ENV_DIR_TMP"
echo


# Create venv
write_log "START create venv in ${PACKAGE_ENV_DIR_TMP}"
"$PYTHON" -m venv "$PACKAGE_ENV_DIR_TMP" --copies
write_log "END   create venv in ${PACKAGE_ENV_DIR_TMP}"
echo
VENVPYTHON=${PACKAGE_ENV_DIR_TMP}/bin/python
if [ -f "$VENVPYTHON" ]; then
    write_log "OK: $VENVPYTHON exists."
    echo
else
    write_log "ERROR: $VENVPYTHON not found"
    exit 2
fi

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
