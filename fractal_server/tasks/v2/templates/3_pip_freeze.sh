set -e

# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__

VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python

"$VENVPYTHON" -m pip freeze --all
