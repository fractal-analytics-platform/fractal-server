"""
Requires `scripts/oauth/docker-compose.yaml` running and the following
environment variables:
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
from httpx import Client
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import _create_first_user
from fractal_server.config import OAuthSettings
from fractal_server.config import get_oauth_settings
from fractal_server.syringe import Inject

DEX_URL = "http://127.0.0.1:5556"


async def _user_count(db: AsyncSession) -> int:
    res = await db.execute(select(func.count(UserOAuth.id)))
    return res.scalar_one()


async def _oauth_count(db: AsyncSession) -> int:
    res = await db.execute(select(func.count(OAuthAccount.id)))
    return res.scalar_one()


async def _oauth_login(
    client: AsyncClient, oauth_settings: OAuthSettings
) -> str:
    """
    Login via Dex as 'kilgore@kilgore.trout'
    """
    res = await client.get("/auth/dexidp/authorize/")
    client.cookies = {
        "fastapiusersoauthcsrf": res.cookies.get("fastapiusersoauthcsrf")
    }
    authorization_url = res.json()["authorization_url"]

    with Client() as httpx_client:
        res = httpx_client.get(authorization_url)

        assert res.status_code == 302
        location = res.headers["location"]

        res = httpx_client.get(f"{DEX_URL}{location}")
        assert res.status_code == 302
        location = res.headers["location"]

        res = httpx_client.get(location)
        assert res.status_code == 303
        location = res.headers["location"]

        assert location.startswith(oauth_settings.OAUTH_REDIRECT_URL)
        code_and_state = location[len(oauth_settings.OAUTH_REDIRECT_URL) :]

    res = await client.get(f"/auth/dexidp/callback/{code_and_state}")
    assert res.status_code == 204
    assert res.headers["set-cookie"].startswith("fastapiusersauth=")

    return res.headers["set-cookie"][len("fastapiusersauth=") :].split(";")[0]


async def _verify_token(client: AsyncClient, token: str, expected_email: str):
    res = await client.get(
        "/auth/current-user/", headers={"Authorization": f"Bearer {token}"}
    )
    assert res.status_code == 200
    assert res.json()["email"] == expected_email


@pytest.mark.basic_auth
async def test_no_basic_auth(
    db,
    client,
    local_resource_profile_db,
):
    # No users
    assert await _user_count(db) == 0
    assert await _oauth_count(db) == 0

    # Register "kilgore@kilgore.trout" (the user from Dex) as regular account.
    resouce, profile = local_resource_profile_db
    await _create_first_user(
        email="kilgore@kilgore.trout",
        password="kilgore",
        is_superuser=True,
        project_dir="/something",
        profile_id=profile.id,
    )
    assert await _user_count(db) == 1

    # Basic-auth login not enabled
    res = await client.post(
        "/auth/token/login/",
        data={"username": "kilgore@kilgore.trout", "password": "kilgore"},
    )
    assert res.status_code == 404
    res = await client.post("/auth/login/")
    assert res.status_code == 404

    # OAuth login still enabled
    oauth_settings = Inject(get_oauth_settings)
    token = await _oauth_login(client, oauth_settings)
    await _verify_token(client, token, "kilgore@kilgore.trout")
    assert await _oauth_count(db) == 1
