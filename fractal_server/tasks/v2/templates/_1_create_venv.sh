set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
TASK_GROUP_DIR=__TASK_GROUP_DIR__
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
PYTHON=__PYTHON__

TIME_START=$(date +%s)

# Check that task-group and venv folders do not exist
for DIR_TO_BE_CHECKED in "$TASK_GROUP_DIR" "$PACKAGE_ENV_DIR";
do
    if [ -d "$DIR_TO_BE_CHECKED" ]; then
        write_log "ERROR: Folder $DIR_TO_BE_CHECKED already exists. Exit."
        exit 1
    fi
done

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
