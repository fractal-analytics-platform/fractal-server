set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__

TIME_START=$(date +%s)

# Remove venv
write_log "START remove venv in ${PACKAGE_ENV_DIR}"
rm -rf "$PACKAGE_ENV_DIR"
write_log "END   remove venv in ${PACKAGE_ENV_DIR}"
echo

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
