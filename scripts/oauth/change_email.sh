#!/bin/bash

# --- Functions

oauth_login(){
    DEX_AUTHORIZATION_URL=$(curl --silent \
        http://127.0.0.1:8001/auth/dexidp/authorize/ \
        | jq -r ".authorization_url"
    )
    USER_TOKEN_OAUTH=$(
        curl -L --silent --output /dev/null --cookie-jar - "$DEX_AUTHORIZATION_URL" \
        | grep "fastapiusersauth" | awk '{print $NF}'
    )
    echo $USER_TOKEN_OAUTH
}

test_users_and_oauth() {
    # There must be two "user_oauth" and one "oauthaccount"
    NUM_USERS=$(sqlite3 $SQLITE_PATH "SELECT * FROM user_oauth;" | wc -l)
    NUM_OAUTH=$(sqlite3 $SQLITE_PATH "SELECT * FROM oauthaccount;" | wc -l)
    if [ "$NUM_USERS" -ne "$1" ] || [ "$NUM_OAUTH" -ne "$2" ]; then
    exit 1
    fi
}

standard_login(){
    TOKEN=$(curl --silent -X POST \
        http://127.0.0.1:8001/auth/token/login/ \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=$1&password=$2" | jq -r ".access_token"
    )
    echo $TOKEN
}

assert_email_and_id(){
    WHOAMI=$(
        curl --silent -H "Authorization: Bearer $1" \
        http://127.0.0.1:8001/auth/current-user/
    )
    EMAIL=$(echo $WHOAMI | jq -r ".email")
    ID=$(echo $WHOAMI | jq -r ".id")
    if [ "$EMAIL" != "$2" ]; then
        exit 1
    fi
    if [ "$ID" != "$3" ]; then
        exit 1
    fi
}

# --- Test

# There must be two "user_oauth" and zero "oauthaccount"
test_users_and_oauth 1 0

# Register "kilgore@kilgore.trout" (the user from Dex) as regular account
SUPERUSER_TOKEN=$(standard_login "admin@fractal.xy" "1234")
curl -X POST \
    http://127.0.0.1:8001/auth/register/ \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $SUPERUSER_TOKEN" \
    -d '{"email": "kilgore@kilgore.trout", "password": "kilgore"}'

# There must be two "user_oauth" and zero "oauthaccount"
test_users_and_oauth 2 0

# Test "kilgore@kilgore.trout" standard authentication
USER_TOKEN=$(standard_login "kilgore@kilgore.trout" "kilgore")
USER_ID=$(
    curl --silent -H "Authorization: Bearer $USER_TOKEN" \
    http://127.0.0.1:8001/auth/current-user/ | jq -r ".id"
)
assert_email_and_id $USER_TOKEN "kilgore@kilgore.trout" $USER_ID

# Authorize with Dex for the first time:
# - create "kilgore@kilgore.trout" OAuth account,
# - associate by email with existing user.
USER_TOKEN_OAUTH=$(oauth_login)
assert_email_and_id $USER_TOKEN_OAUTH "kilgore@kilgore.trout" $USER_ID

# There must be two "user_oauth" and one "oauthaccount"
test_users_and_oauth 2 1

# Change "kilgore@kilgore.trout" email into "kilgore@fractal.xy"
curl -X PATCH \
    "http://127.0.0.1:8001/auth/users/$USER_ID/" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $SUPERUSER_TOKEN" \
    -d '{"email": "kilgore@fractal.xy"}'

# There must (still) be two "user_oauth" and one "oauthaccount"
test_users_and_oauth 2 1

# Test I can login as "kilgore@fractal.xy" with both standard and oauth login
USER_TOKEN=$(standard_login "kilgore@fractal.xy" "kilgore")
assert_email_and_id $USER_TOKEN "kilgore@fractal.xy" $USER_ID
USER_TOKEN_OAUTH=$(oauth_login)
assert_email_and_id $USER_TOKEN_OAUTH "kilgore@fractal.xy" $USER_ID

# Remove all Oauth accounts from db
sqlite3 $SQLITE_PATH "DELETE FROM oauthaccount;"

# There must (still) be two "user_oauth" and zero "oauthaccount"
test_users_and_oauth 2 0

# Test I can login as "kilgore@fractal.xy" just with standard login.
# Logging in with OAuth will create another user, "kilgore@kilgore.trout"
USER_TOKEN=$(standard_login "kilgore@fractal.xy" "kilgore")
assert_email_and_id $USER_TOKEN "kilgore@fractal.xy" $USER_ID

test_users_and_oauth 2 0
USER_TOKEN_OAUTH=$(oauth_login)
test_users_and_oauth 3 1

assert_email_and_id $USER_TOKEN_OAUTH "kilgore@kilgore.trout" $((USER_ID+1))
