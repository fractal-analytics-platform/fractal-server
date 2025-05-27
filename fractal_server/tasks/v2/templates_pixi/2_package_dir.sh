PACKAGE_ENV_DIR="__PACKAGE_ENV_DIR__"
PACKAGE_NAME="__PACKAGE_NAME__"

pixi run --manifest-path ${PACKAGE_ENV_DIR}/pyproject.toml \
    python \
    -c "import ${PACKAGE_NAME}, os; print(os.path.dirname(${PACKAGE_NAME}.__file__))"
