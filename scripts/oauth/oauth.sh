#!/bin/bash
set -e

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
    echo $TOKEN_OAUTH
}

standard_login(){
    # $1: username
    # $2: password
    TOKEN=$(curl --silent -X POST \
        http://127.0.0.1:8001/auth/token/login/ \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=$1&password=$2" | jq -r ".access_token"
    )
    echo $TOKEN
}

assert_users_and_oauth() {
    # $1 desired number of users
    # $2 desired number of oauth accounts
    USERS=$(psql -t -c "SELECT COUNT(*) FROM user_oauth;")
    OAUTH_ACCOUNTS=$(psql -t -c "SELECT COUNT(*) FROM oauthaccount;")
    if [ "$USERS" -ne "$1" ] || [ "$OAUTH_ACCOUNTS" -ne "$2" ]; then
        echo "❌ Expected (users, oauth_accounts)==(${1}, ${2}), got (${USERS}, ${OAUTH_ACCOUNTS})."
        exit 1
    fi
}

assert_email_and_id(){
    # $1 access token
    # $2 desired email
    # $3 desired user id
    USER=$(
        curl --silent -H "Authorization: Bearer $1" \
        http://127.0.0.1:8001/auth/current-user/
    )
    EMAIL=$(echo $USER | jq -r ".email")
    ID=$(echo $USER | jq -r ".id")
    if [ "$EMAIL" != "$2" ]; then
        echo "❌ Expected email==${2}, got ${EMAIL}."
        exit 1
    fi
    if [ "$ID" != "$3" ]; then
        echo "❌ Expected user_id==${3}, got ${ID}."
        exit 1
    fi
}

assert_email_count(){
    NUM_MESSAGE=$(
        curl --silent http://localhost:8025/api/v1/messages | jq -r ".total"
    )
    if [ "$NUM_MESSAGE" -ne "$1" ]; then
        echo "❌ Expected email_count==${1}, got ${NUM_MESSAGE}."
        exit 1
    fi
}

# --- Test
assert_users_and_oauth 1 0
assert_email_count 0

# Register "kilgore@kilgore.trout" (the user from Dex) as regular account.
SUPERUSER_TOKEN=$(standard_login "admin@fractal.xy" "1234")

curl -X POST \
    http://127.0.0.1:8001/auth/register/ \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $SUPERUSER_TOKEN" \
    -d '{"email": "kilgore@kilgore.trout", "password": "kilgore"}'
assert_users_and_oauth 2 0

# Login with "kilgore@kilgore.trout" with standard login.
USER_TOKEN=$(standard_login "kilgore@kilgore.trout" "kilgore")
USER_ID=$(
    curl --silent -H "Authorization: Bearer $USER_TOKEN" \
    http://127.0.0.1:8001/auth/current-user/ | jq -r ".id"
)

assert_email_and_id $USER_TOKEN "kilgore@kilgore.trout" $USER_ID

# First oauth login:
# - create "kilgore@kilgore.trout" oauth account,
# - associate by email with existing user.
USER_TOKEN_OAUTH=$(oauth_login)

assert_users_and_oauth 2 1
assert_email_and_id $USER_TOKEN_OAUTH "kilgore@kilgore.trout" $USER_ID

# Change email into "kilgore@fractal.xy".
curl -X PATCH \
    "http://127.0.0.1:8001/auth/users/$USER_ID/" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $SUPERUSER_TOKEN" \
    -d '{"email": "kilgore@fractal.xy"}'

# Test I can login as "kilgore@fractal.xy" with both standard and oauth login.
USER_TOKEN=$(standard_login "kilgore@fractal.xy" "kilgore")
assert_email_and_id $USER_TOKEN "kilgore@fractal.xy" $USER_ID

USER_TOKEN_OAUTH=$(oauth_login)
assert_email_and_id $USER_TOKEN_OAUTH "kilgore@fractal.xy" $USER_ID

# Remove all oauth accounts from db.
assert_users_and_oauth 2 1
psql -c "DELETE FROM oauthaccount;"
assert_users_and_oauth 2 0

# Test I can login as "kilgore@fractal.xy" with standard login.
USER_TOKEN=$(standard_login "kilgore@fractal.xy" "kilgore")
assert_email_and_id $USER_TOKEN "kilgore@fractal.xy" $USER_ID

# Using oauth login creates another user: "kilgore@kilgore.trout".
assert_users_and_oauth 2 0
assert_email_count 0
USER_TOKEN_OAUTH=$(oauth_login)
assert_users_and_oauth 3 1
assert_email_count 1
# Print emails
echo EMAIL LIST
curl --silent http://localhost:8025/api/v1/messages | jq

assert_email_and_id $USER_TOKEN_OAUTH "kilgore@kilgore.trout" $((USER_ID+1))
