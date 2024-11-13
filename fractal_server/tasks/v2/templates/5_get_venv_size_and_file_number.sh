set -e

# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__

# Find memory usage and file number
ENV_DISK_USAGE=$(du -sk "${PACKAGE_ENV_DIR}" | cut -f1)
ENV_FILE_NUMBER=$(find "${PACKAGE_ENV_DIR}" -type f | wc -l)

echo $ENV_DISK_USAGE $ENV_FILE_NUMBER
