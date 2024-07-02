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

# Upgrade pip
write_log "START upgrade pip"
"$VENVPYTHON" -m pip install pip --upgrade
write_log "END   upgrade pip"
echo

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
