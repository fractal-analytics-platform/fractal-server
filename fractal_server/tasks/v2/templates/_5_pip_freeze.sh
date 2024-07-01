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



PKG_ENV_DIR=$FRACTAL_TASKS_DIR/.${USER}/${PACKAGE_NAME}${VERSION}
VENVPYTHON=${PKG_ENV_DIR}/bin/python

"$VENVPYTHON" -m pip freeze
