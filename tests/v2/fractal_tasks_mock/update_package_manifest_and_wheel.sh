#!/bin/bash

if [ ! -d "venv" ]; then
    python -m venv venv
fi
source venv/bin/activate
python -m pip install -e .
python create_manifest.py
python -m build
