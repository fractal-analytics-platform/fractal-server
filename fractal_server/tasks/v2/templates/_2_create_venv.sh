set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PYTHON=__PYTHON__
FRACTAL_TASKS_DIR=__FRACTAL_TASKS_DIR__
USER=fractal
PACKAGE=__PACKAGE__
PACKAGE_NAME=__PACKAGE_NAME__
# VERSION can be either an empty string or something like "==1.2.3"
VERSION="__VERSION__"
# EXTRAS can be either an empty string or something like "[myextra1,myextra2]"
EXTRAS="__EXTRAS__"


TIME_START=$(date +%s)

# Create main folder
PKG_ENV_DIR=$FRACTAL_TASKS_DIR/.${USER}/${PACKAGE_NAME}${VERSION}


# Create venv
write_log "START create venv in ${PKG_ENV_DIR}"
"$PYTHON" -m venv "$PKG_ENV_DIR" --copies
write_log "END   create venv in ${PKG_ENV_DIR}"
echo
VENVPYTHON=${PKG_ENV_DIR}/bin/python
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
