set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}



# Variables to be filled within fractal-server
PACKAGE_ENV_DIR_TMP=__PACKAGE_ENV_DIR_TMP__

VENVPYTHON=${PACKAGE_ENV_DIR_TMP}/bin/python

"$VENVPYTHON" -m pip freeze
