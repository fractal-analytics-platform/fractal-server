set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}

# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
PIP_FREEZE_FILE=__PIP_FREEZE_FILE__
FRACTAL_MAX_PIP_VERSION=__FRACTAL_MAX_PIP_VERSION__

TIME_START=$(date +%s)

VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python

# Upgrade `pip` and install `setuptools`
write_log "START upgrade pip and install setuptools"
"$VENVPYTHON" -m pip install --no-cache-dir "pip<=${FRACTAL_MAX_PIP_VERSION}" --upgrade
"$VENVPYTHON" -m pip install --no-cache-dir setuptools
write_log "END   upgrade pip and install setuptools"
echo

# Install from pip-freeze file
write_log "START installing requirements from ${PIP_FREEZE_FILE}"
"$VENVPYTHON" -m pip install -r "${PIP_FREEZE_FILE}"
write_log "END   installing requirements from ${PIP_FREEZE_FILE}"
echo

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
