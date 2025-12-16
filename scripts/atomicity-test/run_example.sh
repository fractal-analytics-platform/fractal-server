#!/bin/bash

dropdb --if-exists fractal-atomicity-test
createdb fractal-atomicity-test
uv run --frozen fractalctl set-db --skip-init-data
uv run --frozen python tmp.py
