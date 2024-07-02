set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
PYTHON=__PYTHON__

TIME_START=$(date +%s)


# Create main folder
if [ -d "$PACKAGE_ENV_DIR" ]; then
    write_log "ERROR: Folder $PACKAGE_ENV_DIR already exists. Exit."
    exit 1
fi
write_log "START mkdir -p $PACKAGE_ENV_DIR"
mkdir -p $PACKAGE_ENV_DIR
write_log "END   mkdir -p $PACKAGE_ENV_DIR"
echo


# Create venv
write_log "START create venv in ${PACKAGE_ENV_DIR}"
"$PYTHON" -m venv "$PACKAGE_ENV_DIR" --copies
write_log "END   create venv in ${PACKAGE_ENV_DIR}"
echo
VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python
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
