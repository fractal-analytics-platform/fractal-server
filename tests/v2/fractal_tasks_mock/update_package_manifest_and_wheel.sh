#!/bin/bash

set -e

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
python -m pip install -e ".[dev]"
fractal-manifest create --package fractal-tasks-mock
python -m build
deactivate
