#!/bin/bash

set -e

if [ ! -d "venv" ]; then
    python3.12 -m venv venv
fi
source venv/bin/activate
python3 -m pip install -e ".[dev]"
fractal-manifest create --package fractal-tasks-mock
python -m build
deactivate
