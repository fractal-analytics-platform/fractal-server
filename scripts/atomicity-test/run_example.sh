#!/bin/bash

dropdb --if-exists fractal-atomicity-test
createdb fractal-atomicity-test
poetry run fractalctl set-db --skip-init-data
poetry run python tmp.py
