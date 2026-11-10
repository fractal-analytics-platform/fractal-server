#!/bin/bash

set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[write-def-apptainer, ${TIMESTAMP}] ${1}"
}

# Replacements
PIXI_VERSION="__PIXI_VERSION__"
PACKAGE_DIR="__PACKAGE_DIR__"
TAR_GZ_PATH="__TAR_GZ_PATH__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"
CONTAINER_ROOT="__CONTAINER_ROOT__"
DEF_FILE="__DEF_FILE__"

# Strip trailing `/`
PACKAGE_DIR=${PACKAGE_DIR%/}
CONTAINER_ROOT=${CONTAINER_ROOT%/}

# Known paths
TAR_GZ_FILENAME=$(basename "${TAR_GZ_PATH}")
TAR_GZ_BASENAME=$(basename "${TAR_GZ_PATH}" ".tar.gz")
CONTAINER_SOURCE_DIR="${CONTAINER_ROOT}/${SOURCE_DIR_NAME}"
CONTAINER_PYPROJECT_TOML="${CONTAINER_SOURCE_DIR}/pyproject.toml"

TIME_START=$(date +%s)

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

write_log "START writing definition file ${DEF_FILE}"
cat <<EOF > "${DEF_FILE}"
Bootstrap: docker
From: ghcr.io/prefix-dev/pixi:${PIXI_VERSION}

%files
    ${TAR_GZ_PATH} ${CONTAINER_ROOT}/${TAR_GZ_FILENAME}

%post
    set -e
    cd ${CONTAINER_ROOT}
    tar xz -f ${TAR_GZ_FILENAME} ${TAR_GZ_BASENAME}
    mv ${TAR_GZ_BASENAME} ${SOURCE_DIR_NAME}
    rm ${TAR_GZ_FILENAME}
    pixi install --frozen --manifest-path ${CONTAINER_PYPROJECT_TOML}
    pixi clean cache --yes || true
EOF
write_log "END   writing definition file ${DEF_FILE}"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
