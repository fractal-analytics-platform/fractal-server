set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PACKAGE_ENV_DIR=__PACKAGE_ENV_DIR__
INSTALL_STRING=__INSTALL_STRING__
PINNED_PACKAGE_LIST=__PINNED_PACKAGE_LIST__
# Example of value: __PINNED_PACKAGE_LIST__="pkga==1.0 pkgb==2.0.3"

TIME_START=$(date +%s)

VENVPYTHON=${PACKAGE_ENV_DIR}/bin/python

# Install package
write_log "START install ${INSTALL_STRING}"
"$VENVPYTHON" -m pip install "$INSTALL_STRING"
write_log "END   install ${INSTALL_STRING}"
echo


# Optionally install pinned versions
if [ "$PINNED_PACKAGE_LIST" != "" ]; then
    write_log "START installing pinned versions $PINNED_PACKAGE_LIST"
    for PINNED_PKG_VERSION in $PINNED_PACKAGE_LIST; do

        PKGNAME=$(echo "$PINNED_PKG_VERSION" | cut -d '=' -f 1)
        RETCODE=$("$VENVPYTHON" -m pip show "$PKGNAME")
        if [ "$RETCODE" != 0 ];
        then
            write_log "ERROR: package $PKGNAME is not currently installed"
            exit 4
        fi
    done
    write_log "All packages in ${PINNED_PACKAGE_LIST} are already installed, proceed with specific versions."
    "$VENVPYTHON" -m pip install "$PINNED_PACKAGE_LIST"
    write_log "END installing pinned versions $PINNED_PACKAGE_LIST"
else
    write_log "SKIP installing pinned versions $PINNED_PACKAGE_LIST (empty list)"
fi


# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
