#!/bin/bash

set -eu

FOLDER="mock_pixi_tasks-1.2.3"
OUTFOLDER_LOCK="with_lock"
OUTFOLDER_NO_LOCK="no_lock"
TAR_GZ_FILE_LOCK="$OUTFOLDER_LOCK/mock_pixi_tasks-1.2.3.tar.gz"
TAR_GZ_FILE_NO_LOCK="$OUTFOLDER_NO_LOCK/mock_pixi_tasks-1.2.3.tar.gz"

(
    cd "$FOLDER"
    pixi lock
    pixi run fractal-manifest create
)

rm -fr "$OUTFOLDER_LOCK" "$OUTFOLDER_NO_LOCK"
mkdir "$OUTFOLDER_LOCK" "$OUTFOLDER_NO_LOCK"

tar --exclude='.*' --exclude="__pycache__" --exclude="*.egg-info" -v -czf "$TAR_GZ_FILE_LOCK" "$FOLDER"
tar --exclude='.*' --exclude="__pycache__" --exclude="*.egg-info" --exclude "pixi.lock" -v -czf "$TAR_GZ_FILE_NO_LOCK" "$FOLDER"
