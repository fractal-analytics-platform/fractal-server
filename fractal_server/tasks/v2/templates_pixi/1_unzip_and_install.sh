PACKAGE_ENV_DIR="__PACKAGE_ENV_DIR__"
PWHL_PATH="__PWHL_PATH__"
ENV_EXTRA="__ENV_EXTRA__"

if [ -n "${ENV_EXTRA}" ]; then
    CMD_EXTRA="-e ${ENV_EXTRA}"
fi

unzip ${PWHL_PATH} -d ${PACKAGE_ENV_DIR}
pixi install --manifest-path "${PACKAGE_ENV_DIR}/pyproject.toml" ${CMD_EXTRA}
