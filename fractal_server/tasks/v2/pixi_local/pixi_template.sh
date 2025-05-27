# To replace ------------------------
PACKAGE_ENV_DIR="__PACKAGE_ENV_DIR__"
TAR_GZ_PATH="__TAR_GZ_PATH__"
PACKAGE_NAME="__PACKAGE_NAME__"
# -----------------------------------

TOML="${PACKAGE_ENV_DIR}/pyproject.toml"

mkdir -p ${PACKAGE_ENV_DIR}
tar -xzfv ${TAR_GZ_PATH} -C ${PACKAGE_ENV_DIR}
pixi install --manifest-path ${TOML}

TASK_DIR=$(
    pixi run --manifest-path ${TOML} python \
    -c "import ${PACKAGE_NAME} as p, os; print(os.path.dirname(p.__file__))"
)
ENV_DISK_USAGE=$(du -sk "${PACKAGE_ENV_DIR}" | cut -f1)
ENV_FILE_NUMBER=$(find "${PACKAGE_ENV_DIR}" -type f | wc -l)
