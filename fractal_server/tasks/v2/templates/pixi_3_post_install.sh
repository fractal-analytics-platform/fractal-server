set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[after-install-pixi, ${TIMESTAMP}] ${1}"
}

# Replacements
PIXI_HOME="__PIXI_HOME__"
PACKAGE_DIR="__PACKAGE_DIR__"
SOURCE_DIR_NAME="__SOURCE_DIR_NAME__"
IMPORT_PACKAGE_NAME="__IMPORT_PACKAGE_NAME__"

# Strip trailing `/` from `PACKAGE_DIR`
PIXI_HOME=${PIXI_HOME%/}
PACKAGE_DIR=${PACKAGE_DIR%/}

# Known paths
PIXI_EXECUTABLE="${PIXI_HOME}/bin/pixi"
SOURCE_DIR="${PACKAGE_DIR}/${SOURCE_DIR_NAME}"
PYPROJECT_TOML="${SOURCE_DIR}/pyproject.toml"
ACTIVATION_FILE="${SOURCE_DIR}/activate_project.sh"
PROJECT_PYTHON_WRAPPER="${SOURCE_DIR}/project_python.sh"

# Pixi env variable
export PIXI_HOME="${PIXI_HOME}"
export PIXI_CACHE_DIR="${PIXI_HOME}/cache"
export RATTLER_AUTH_FILE="${PIXI_HOME}/credentials.json"


TIME_START=$(date +%s)

cd "${PACKAGE_DIR}"
write_log "Changed working directory to ${PACKAGE_DIR}"

# -----------------------------------------------------------------------------

write_log "START '${PIXI_EXECUTABLE} shell-hook --manifest-path ${PYPROJECT_TOML}'"
${PIXI_EXECUTABLE} shell-hook --manifest-path "${PYPROJECT_TOML}" > "${ACTIVATION_FILE}"
write_log "END   '${PIXI_EXECUTABLE} shell-hook --manifest-path ${PYPROJECT_TOML}'"
echo

PROJECT_PYTHON_BIN=$(${PIXI_EXECUTABLE} run --manifest-path "${PYPROJECT_TOML}" which python)
write_log "Found PROJECT_PYTHON_BIN=${PROJECT_PYTHON_BIN}"

# Write project-scoped Python wrapper
cat <<EOF > "${PROJECT_PYTHON_WRAPPER}"
#!/bin/bash
source ${ACTIVATION_FILE}
${PROJECT_PYTHON_BIN} "\$@"
EOF

chmod 755 "${PROJECT_PYTHON_WRAPPER}"
write_log "Written ${PROJECT_PYTHON_WRAPPER} with 755 permissions"
write_log "Project Python wrapper: ${PROJECT_PYTHON_WRAPPER}"
write_log "Project-Python version: $(${PROJECT_PYTHON_WRAPPER} --version)"
echo

# Find PACKAGE_FOLDER
FIND_PACKAGE_FOLDER_SCRIPT="${SOURCE_DIR}/find_package_folder.sh"
echo "source ${ACTIVATION_FILE}" > "${FIND_PACKAGE_FOLDER_SCRIPT}"
echo "${PROJECT_PYTHON_BIN} -c \"import ${IMPORT_PACKAGE_NAME} as p, os; print(os.path.dirname(p.__file__))\"" >> "${FIND_PACKAGE_FOLDER_SCRIPT}"
PACKAGE_FOLDER=$(bash "${FIND_PACKAGE_FOLDER_SCRIPT}")
write_log "Package folder: ${PACKAGE_FOLDER}"
echo

ENV_DISK_USAGE=$(du -sk "${PACKAGE_DIR}" | cut -f1)
ENV_FILE_NUMBER=$(find "${PACKAGE_DIR}" -type f | wc -l)
write_log "Disk usage: ${ENV_DISK_USAGE}"
write_log "Number of files: ${ENV_FILE_NUMBER}"
echo

TIME_END=$(date +%s)
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "All ok, exit."
echo
