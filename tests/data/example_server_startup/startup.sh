#!/bin/bash

# CLEAN UP
# rm test.db
# rm -r FRACTAL_TASKS_DIR

# Create an empty db
poetry run fractalctl set-db

# Start the server
poetry run fractalctl start --port 8000
