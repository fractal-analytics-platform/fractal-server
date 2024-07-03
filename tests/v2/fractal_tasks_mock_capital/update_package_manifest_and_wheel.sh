#!/bin/bash

set -e

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
python -m pip install -e ".[dev,my_extra]"
python create_manifest.py
python -m build
deactivate
