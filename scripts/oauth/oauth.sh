#!/bin/bash

# This script was originally run as a job in the GitHub CI by `.github/workflows/oauth.yaml`
# Removed by PR #2929 and replaced with `tests/no_version/test_api_oauth.py`.

set -eu

# --- Functions

oauth_login(){
    AUTHORIZATION_URL=$(curl --silent \
        http://127.0.0.1:8001/auth/dexidp/authorize/ \
        | jq -r ".authorization_url"
    )
    TOKEN_OAUTH=$(
        curl -L --silent --output /dev/null --cookie-jar - "$AUTHORIZATION_URL" \
        | grep "fastapiusersauth" | awk '{print $NF}'
    )
    echo "$TOKEN_OAUTH"
}

standard_login(){
    # $1: username
    # $2: password
    TOKEN=$(curl --silent -X POST \
        http://127.0.0.1:8001/auth/token/login/ \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=$1&password=$2" | jq -r ".access_token"
    )
    echo "$TOKEN"
}

assert_users_and_oauth() {
    # $1 expected number of users
    # $2 expected number of oauth accounts
    USERS=$(psql -t -c "SELECT COUNT(*) FROM user_oauth;")
    OAUTH_ACCOUNTS=$(psql -t -c "SELECT COUNT(*) FROM oauthaccount;")
    if [ "$USERS" -ne "$1" ] || [ "$OAUTH_ACCOUNTS" -ne "$2" ]; then
        echo "ERROR: Expected (users, oauth_accounts)==(${1}, ${2}), got (${USERS}, ${OAUTH_ACCOUNTS})."
        exit 1
    fi
}

assert_email_and_id(){
    # $1 access token
    # $2 expected email
    # $3 expected user id
    USER=$(
        curl --silent -H "Authorization: Bearer $1" \
        http://127.0.0.1:8001/auth/current-user/
    )
    EMAIL=$(echo "$USER" | jq -r ".email")
    ID=$(echo "$USER" | jq -r ".id")
    echo "[assert_email_and_id] EMAIL=$EMAIL ID=$ID"
    if [ "$EMAIL" != "$2" ]; then
        echo "ERROR: Expected email==${2}, got ${EMAIL}."
        exit 1
    fi
    if [ "$ID" != "$3" ]; then
        echo "ERROR: Expected user_id==${3}, got ${ID}."
        exit 1
    fi
}

assert_email_count(){
    # $1 expected number of emails
    NUM_MESSAGES=$(
        curl --silent http://localhost:8025/api/v1/messages | jq -r ".total"
    )
    echo
    echo "[assert_email_count] Found NUM_MESSAGES=$NUM_MESSAGES"
    if [ "$NUM_MESSAGES" -ne "$1" ]; then
        echo "ERROR: Expected email_count==${1}, got ${NUM_MESSAGES}."
        exit 1
    fi
}

# --- Test
assert_users_and_oauth 1 0
assert_email_count 0

# Get superuser token
SUPERUSER_TOKEN=$(standard_login "admin@example.org" "1234")

# Register "kilgore@kilgore.trout" (the user from Dex) as regular account.
curl --silent -X POST \
    http://127.0.0.1:8001/auth/register/ \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $SUPERUSER_TOKEN" \
    -d '{"email": "kilgore@kilgore.trout", "password": "kilgore", "project_dir": "/fake"}'
assert_users_and_oauth 2 0

# Login with "kilgore@kilgore.trout" with standard login.
USER_TOKEN=$(standard_login "kilgore@kilgore.trout" "kilgore")
USER_ID=$(
    curl --silent -H "Authorization: Bearer $USER_TOKEN" \
    http://127.0.0.1:8001/auth/current-user/ | jq -r ".id"
)
assert_email_and_id "$USER_TOKEN" "kilgore@kilgore.trout" "$USER_ID"


# First oauth login:
# - create "kilgore@kilgore.trout" oauth account,
# - associate by email with existing user.
USER_TOKEN_OAUTH=$(oauth_login)

assert_users_and_oauth 2 1
assert_email_and_id "$USER_TOKEN_OAUTH" "kilgore@kilgore.trout" "$USER_ID"


# Change email into "kilgore@example.org".
curl --silent -X PATCH \
    "http://127.0.0.1:8001/auth/users/$USER_ID/" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $SUPERUSER_TOKEN" \
    -d '{"email": "kilgore@example.org"}'


# Test I can login as "kilgore@example.org" through standard login
USER_TOKEN=$(standard_login "kilgore@example.org" "kilgore")
assert_email_and_id "$USER_TOKEN" "kilgore@example.org" "$USER_ID"

# Test I can login as "kilgore@example.org" through oauth login
USER_TOKEN_OAUTH=$(oauth_login)
assert_email_and_id "$USER_TOKEN_OAUTH" "kilgore@example.org" "$USER_ID"

# Remove all oauth accounts from db.
assert_users_and_oauth 2 1
psql -c "DELETE FROM oauthaccount;"
assert_users_and_oauth 2 0

# Test I can login as "kilgore@example.org" with standard login.
USER_TOKEN=$(standard_login "kilgore@example.org" "kilgore")
assert_email_and_id "$USER_TOKEN" "kilgore@example.org" "$USER_ID"

# Use oauth login again, for non-existing user "kilgore@kilgore.trout".
# This will lead to an error, and to an email being sent to the Fractal admins

assert_users_and_oauth 2 0
assert_email_count 0

AUTHORIZATION_URL=$(curl --silent http://127.0.0.1:8001/auth/dexidp/authorize/ | jq -r ".authorization_url")
OUTCOME=$(curl --silent -L "$AUTHORIZATION_URL" | jq -r ".detail")

echo "$OUTCOME" | grep "Thank you for registering"
echo "$OUTCOME" | grep "https://example.org/info"

assert_users_and_oauth 2 0
assert_email_count 1

curl --silent http://localhost:8025/api/v1/messages | jq
