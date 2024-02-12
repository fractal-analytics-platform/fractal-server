#!/bin/bash

# conda create --name fractal-client-env python=3.10
# conda activate fractal-client-env
# pip install fractal-client  # --> installs "fractal" CLI command


read -r -p "Write the user's email: " FRACTAL_USER
read -s -r -p "Write the user's password: " FRACTAL_PASSWORD
OPTS="--user $FRACTAL_USER --password $FRACTAL_PASSWORD"

# Set cache path and remove any previous file from there
CURRENT_DIRECTORY=$(pwd)
export FRACTAL_CACHE_PATH="$CURRENT_DIRECTORY/.cache"
rm -rv "${FRACTAL_CACHE_PATH}"  2> /dev/null

# Create project
PROJECT_ID=$(fractal $OPTS --batch project new "Test Project 1231")
# Create dataset
DATASET_ID=$(fractal $OPTS --batch project add-dataset  --type image "$PROJECT_ID" "Test Dataset")
# Add resource to dataset
fractal $OPTS dataset add-resource "$PROJECT_ID" "$DATASET_ID" "/invalid/path"
# Create workflow
WF_ID=$(fractal $OPTS --batch workflow new "Test Workflow" "$PROJECT_ID")
# Add task to workflow
fractal $OPTS --batch workflow add-task "$PROJECT_ID" "$WF_ID" --task-name "TestNewUser"
# Appply workflow
fractal $OPTS --batch workflow apply "$PROJECT_ID" "$WF_ID" "$DATASET_ID" "$DATASET_ID"
