#!/bin/bash

set -e

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
python3 -m pip install -e ".[dev]"
fractal-manifest create --package fractal-tasks-non-canonical
python3 -m build
deactivate
