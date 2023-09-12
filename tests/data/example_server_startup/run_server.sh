#!/bin/bash

# Create an empty db
poetry run fractalctl set-db

# Start the server
poetry run fractalctl start --port 8000
