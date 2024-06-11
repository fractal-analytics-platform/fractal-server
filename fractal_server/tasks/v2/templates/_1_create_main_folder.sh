set -e

write_log(){
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "[collect-task, $TIMESTAMP] $1"
}


# Variables to be filled within fractal-server
PYTHON=__PYTHON__
FRACTAL_TASKS_DIR=__FRACTAL_TASKS_DIR__
USER=fractal
PACKAGE=__PACKAGE__
# VERSION can be either an empty string or something like "==1.2.3"
VERSION="__VERSION__"
# EXTRAS can be either an empty string or something like "[myextra1,myextra2]"
EXTRAS="__EXTRAS__"


TIME_START=$(date +%s)

# Create main folder
PKG_ENV_DIR=$FRACTAL_TASKS_DIR/.${USER}/${PACKAGE}${VERSION}
if [ -d "$PKG_ENV_DIR" ]; then
    write_log "ERROR: Folder $PKG_ENV_DIR already exists. Exit."
    exit 1
fi
write_log "START mkdir $PKG_ENV_DIR"
mkdir -p $PKG_ENV_DIR
write_log "END   mkdir $PKG_ENV_DIR"
echo

# End
TIME_END=$(date +%s)
write_log "All good up to here."
write_log "Elapsed: $((TIME_END - TIME_START)) seconds"
write_log "Exit."
echo
