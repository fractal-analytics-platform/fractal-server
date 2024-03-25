#!/bin/bash

poetry run --directory ../../../ python init_manifest.py
python -m build
