set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task-pixi, $TIMESTAMP] $1"
}

# Absolute paths
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
# Names
TAR_GZ_FILE_NAME="__TAR_GZ_FILE_NAME__"
PACKAGE_NAME="__PACKAGE_NAME__"

PIXI_EXECUTABLE="${PIXI_HOME}/bin/pixi"
TAR_FILE="${PACKAGE_DIR}/${TAR_GZ_FILE_NAME}"
SOURCE_DIR="${PACKAGE_DIR}/source_dir"
PYPROJECT_TOML="${SOURCE_DIR}/pyproject.toml"

export PIXI_HOME=${PIXI_HOME}
export PIXI_CACHE_DIR="${PIXI_HOME}/cache"
export RATTLER_AUTH_FILE="${PIXI_HOME}/credentials.json"

tar -xzfv ${TAR_FILE} -C ${SOURCE_DIR}
${PIXI_EXECUTABLE} install --manifest-path ${PYPROJECT_TOML}

TASK_DIR=$(
    ${PIXI_EXECUTABLE} run --manifest-path ${PYPROJECT_TOML} python \
    -c "import ${PACKAGE_NAME} as p, os; print(os.path.dirname(p.__file__))"
)
ENV_DISK_USAGE=$(du -sk "${PACKAGE_DIR}" | cut -f1)
ENV_FILE_NUMBER=$(find "${PACKAGE_DIR}" -type f | wc -l)

cat <<EOF
{
  "task_dir": ${TASK_DIR},
  "env_disk_usage": ${ENV_DISK_USAGE},
  "env_file_number": ${ENV_FILE_NUMBER}
}
EOF
