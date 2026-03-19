#!/bin/bash

set -eu

FOLDER="mock_pixi_tasks-1.2.3"
TAR_GZ_FILE="mock_pixi_tasks-1.2.3.tar.gz"


(
    cd "$FOLDER"
    pixi lock
    pixi run fractal-manifest create
)

rm -f "$TAR_GZ_FILE"
tar --exclude='.*' --exclude='create_tar_gz.sh' --exclude='*.tar.gz' --exclude="__pycache__" --exclude="*.egg-info" -v -czf "$TAR_GZ_FILE" "$FOLDER"
