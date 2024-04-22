#!/bin/bash

rm test.db
rm test.db-shm
rm test.db-wal
rm -r Tasks
rm -r Artifacts

poetry run fractalctl set-db
poetry run python example_1.py
