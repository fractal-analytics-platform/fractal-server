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



VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python

"$VENVPYTHON" -m pip freeze
