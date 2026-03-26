"""
Requires
```
docker compose -f scripts/oauth/docker-compose.yaml up
```
and the following environment variables:
```
export OAUTH_CLIENT_NAME=dexidp
export OAUTH_CLIENT_ID=client_test_id
export OAUTH_CLIENT_SECRET=client_test_secret
export OAUTH_REDIRECT_URL=http://localhost:8001/auth/dexidp/callback/
export OAUTH_OIDC_CONFIG_ENDPOINT=http://127.0.0.1:5556/dex/.well-known/openid-configuration
export FRACTAL_DISABLE_BASIC_AUTH=true
```
"""

import pytest
from httpx import AsyncClient

from fractal_server.app.security import _create_first_user
from fractal_server.config import get_oauth_settings
from fractal_server.syringe import Inject

from .test_api_oauth import _oauth_count
from .test_api_oauth import _oauth_login
from .test_api_oauth import _user_count
from .test_api_oauth import _verify_token

EMAIL = "kilgore@kilgore.trout"


@pytest.mark.basic_auth
async def test_no_basic_auth(
    db,
    client: AsyncClient,
    local_resource_profile_db,
):
    # No users
    assert await _user_count(db) == 0
    assert await _oauth_count(db) == 0

    # Register "kilgore@kilgore.trout" (the user from Dex) as regular account.
    resouce, profile = local_resource_profile_db
    await _create_first_user(
        email=EMAIL,
        password="kilgore",
        is_superuser=True,
        project_dir="/something",
        profile_id=profile.id,
    )
    assert await _user_count(db) == 1

    # Basic-auth login not enabled
    res = await client.post(
        "/auth/token/login/",
        data={"username": EMAIL, "password": "kilgore"},
    )
    assert res.status_code == 404
    res = await client.post("/auth/login/")
    assert res.status_code == 404

    # OAuth login still enabled
    oauth_settings = Inject(get_oauth_settings)
    token = await _oauth_login(client, oauth_settings)
    await _verify_token(client, token, EMAIL)
    assert await _oauth_count(db) == 1

    # Make a `GET /auth/current-user/` call via Authorization header
    res = await client.get(
        "/auth/current-user/",
        headers={
            "Authorization": f"Bearer {token}",
        },
    )
    assert res.status_code == 200
    assert res.json()["email"] == EMAIL

    # Make a `GET /auth/current-user/` call via `fastapiusersauth` cookie
    client.cookies = {"fastapiusersauth": token}
    res = await client.get("/auth/current-user/")
    assert res.status_code == 200
    assert res.json()["email"] == EMAIL
