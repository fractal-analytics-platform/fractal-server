#!/bin/bash
uv sync --all-extras
uv run python3 -m build
rm uv.lock
