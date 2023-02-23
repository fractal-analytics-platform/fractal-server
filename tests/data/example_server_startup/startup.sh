#!/bin/bash

# Create an empty db
export SQLITE_PATH=test.db
rm $SQLITE_PATH
fractalctl set-db

# Start the server
fractalctl start --port 8010
