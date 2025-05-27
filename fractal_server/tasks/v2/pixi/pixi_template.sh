# To replace
PACKAGE_ENV_DIR="__PACKAGE_ENV_DIR__"
TAR_GZ_PATH="__TAR_GZ_PATH__"
ENV_EXTRA="__ENV_EXTRA__"
PACKAGE_NAME="__PACKAGE_NAME__"

if [ -n "${ENV_EXTRA}" ]; then
    CMD_EXTRA="-e ${ENV_EXTRA}"
fi
TOML="${PACKAGE_ENV_DIR}/pyproject.toml"

mkdir -p ${PACKAGE_ENV_DIR}
tar -xzfv ${TAR_GZ_PATH} -C ${PACKAGE_ENV_DIR}

pixi install --manifest-path ${TOML} ${CMD_EXTRA}
pixi run --manifest-path ${TOML} python \
    -c "import ${PACKAGE_NAME}, os; print(os.path.dirname(${PACKAGE_NAME}.__file__))"
