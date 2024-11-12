#!/bin/bash

GREP_ARGS="../fractal_server --exclude-dir data_migrations --exclude-dir migrations --exclude-dir __pycache__ --exclude-dir routes"

SYNC_FUNCTIONS=$(
    grep -r "^def " $GREP_ARGS  --include \*.py | cut -d ":" -f 2 | cut -d ' ' -f 2 | cut -d '(' -f1 | sort | uniq
)
ASYNC_FUNCTIONS=$(
    grep -r "^async def " ${GREP_ARGS} --include \*.py | cut -d ":" -f 2 | cut -d ' ' -f 3 | cut -d '(' -f1 | sort | uniq
)

for FUNCTION in $SYNC_FUNCTIONS $ASYNC_FUNCTIONS; do
    echo "FUNCTION $FUNCTION"
    USAGE_NUMBER=$(grep -r "$FUNCTION" ../fractal_server --exclude-dir __pycache__ | wc -l)
    if [ "$USAGE_NUMBER" == "1" ]; then
        echo "BAD?"
        git grep "$FUNCTION" ../fractal_server
        echo
    fi
done
