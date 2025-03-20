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

mv dist/fractal_tasks_non_canonical-0.0.1-py3-none-any.whl dist/FrAcTaL_TaSkS_NoN_CaNoNiCaL-0.0.1-py3-none-any.whl
