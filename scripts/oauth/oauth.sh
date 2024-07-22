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

EXPECTED_USER='{"id":2,"email":"kilgore@kilgore.trout","is_active":true,"is_superuser":false,"is_verified":false,"slurm_user":null,"cache_dir":null,"username":null,"slurm_accounts":[]}'

# Assert that WHOAMI is equal to EXPECTED_USER
diff <(echo $WHOAMI | jq --sort-keys .) <(echo $EXPECTED_USER | jq --sort-keys .)
if [ "$?" != "0" ]; then
  exit 1
fi
