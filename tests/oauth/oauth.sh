#!/bin/bash

AUTHORIZATION_URL=$(curl --silent \
    http://127.0.0.1:8001/auth/dexidp/authorize/ \
    | jq -r ".authorization_url"
)
TOKEN=$(
    curl -L --silent --output /dev/null --cookie-jar - $AUTHORIZATION_URL \
    | grep "fastapiusersauth" | awk '{print $NF}'
)
WHOAMI=$(
    curl --silent -H "Authorization: Bearer $TOKEN" \
    http://127.0.0.1:8001/auth/current-user/
)

# ----

assert_equal() {
  if [ "$1" != "$2" ]; then
    echo "Error: $1 != $2"
    exit 1
  fi
}

USER_ID=$(echo $WHOAMI | jq -r ".id")
EMAIL=$(echo $WHOAMI | jq -r ".email")
IS_ACTIVE=$(echo $WHOAMI | jq -r ".is_active")
IS_SUPERUSER=$(echo $WHOAMI | jq -r ".is_superuser")
IS_VERIFIED=$(echo $WHOAMI | jq -r ".is_verified")
SLURM_USER=$(echo $WHOAMI | jq -r ".slurm_user")
CACHE_DIR=$(echo $WHOAMI | jq -r ".cache_dir")
USER_NAME=$(echo $WHOAMI | jq -r ".user_name")
SLURM_ACCOUNTS=$(echo $WHOAMI | jq -r ".slurm_accounts")


assert_equal $USER_ID "2"
assert_equal $EMAIL "kilgore@kilgore.trout"
assert_equal $IS_ACTIVE "true"
assert_equal $IS_SUPERUSER "false"
assert_equal $IS_VERIFIED "false"
assert_equal $SLURM_USER "null"
assert_equal $CACHE_DIR "null"
assert_equal $USER_NAME "null"
assert_equal $SLURM_ACCOUNTS "[]"
