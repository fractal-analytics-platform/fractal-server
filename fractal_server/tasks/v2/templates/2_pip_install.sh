set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}

# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
INSTALL_STRING="__INSTALL_STRING__"
PINNED_PACKAGE_LIST="__PINNED_PACKAGE_LIST__"
FRACTAL_MAX_PIP_VERSION="__FRACTAL_MAX_PIP_VERSION__"
FRACTAL_PIP_CACHE_DIR_ARG="__FRACTAL_PIP_CACHE_DIR_ARG__"

TIME_START=$(date +%s)

VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python

# Upgrade `pip` and install `setuptools`
write_log "START upgrade pip and install setuptools"
"$VENVPYTHON" -m pip install ${FRACTAL_PIP_CACHE_DIR_ARG} "pip<=${FRACTAL_MAX_PIP_VERSION}" --upgrade
"$VENVPYTHON" -m pip install ${FRACTAL_PIP_CACHE_DIR_ARG} setuptools
write_log "END   upgrade pip and install setuptools"
echo

# Install package
write_log "START install with INSTALL_STRING=${INSTALL_STRING}"
"$VENVPYTHON" -m pip install ${FRACTAL_PIP_CACHE_DIR_ARG} "$INSTALL_STRING"
write_log "END   install with INSTALL_STRING=${INSTALL_STRING}"
echo

# Install pinned packages (note: do not quote $PINNED_PACKAGE_LIST since it could be e.g. "numpy==1.2.3 torch=3.2.1")
if [ "$PINNED_PACKAGE_LIST" != "" ]; then
    write_log "START install with PINNED_PACKAGE_LIST=${PINNED_PACKAGE_LIST}"
    "$VENVPYTHON" -m pip install ${FRACTAL_PIP_CACHE_DIR_ARG} $PINNED_PACKAGE_LIST
    write_log "END install with PINNED_PACKAGE_LIST=${PINNED_PACKAGE_LIST}"
    echo
else
    write_log "SKIP installing pinned versions $PINNED_PACKAGE_LIST (empty list)"
fi

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
