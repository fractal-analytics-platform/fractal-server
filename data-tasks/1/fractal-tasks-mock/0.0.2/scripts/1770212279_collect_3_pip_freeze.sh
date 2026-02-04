#!/bin/bash

set -e

# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=/home/yuri/lavoro/fractal/fractal-server/data-tasks/1/fractal-tasks-mock/0.0.2/venv

VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python

"$VENVPYTHON" -m pip freeze --all
