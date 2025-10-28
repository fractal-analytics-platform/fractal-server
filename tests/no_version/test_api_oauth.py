"""
The following tests require this docker-compose.yaml running:

```yaml
version: "3.8"

services:
  mailpit:
    image: axllent/mailpit
    container_name: mailpit
    ports:
      - "1025:1025"   # SMTP
      - "8025:8025"   # Web UI
    environment:
      MP_SMTP_AUTH: "sender@example.org:fakepassword"
      MP_SMTP_AUTH_ALLOW_INSECURE: "true"

  dexidp:
    image: ghcr.io/fractal-analytics-platform/oauth:0.1
    container_name: dexidp
    ports:
      - "5556:5556"
```
"""
from urllib.parse import urlparse

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import delete
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth


async def _user_count(db: AsyncSession) -> int:
    res = await db.execute(select(func.count(UserOAuth.id)))
    return res.scalar_one()


async def _oauth_count(db: AsyncSession) -> int:
    res = await db.execute(select(func.count(OAuthAccount.id)))
    return res.scalar_one()


def _email_count() -> int:
    with httpx.Client() as client:
        response = client.get("http://localhost:8025/api/v1/messages")
        response.raise_for_status()
        data = response.json()
        total = data.get("total")
    return total


async def _standard_login(client, username, password) -> str:
    res = await client.post(
        "/auth/token/login/",
        data={"username": username, "password": password},
    )
    assert res.status_code == 200
    return res.json()["access_token"]


async def _oauth_login(client) -> str:
    res = await client.get("/auth/dexidp/authorize/")
    authorization_url = res.json()["authorization_url"]

    with httpx.Client() as httpx_client:
        res = httpx_client.get(authorization_url)
        assert res.status_code == 302
        location = res.headers["location"]

        res = httpx_client.get(f"http://127.0.0.1:5556{location}")
        assert res.status_code == 302
        location = res.headers["location"]

        res = httpx_client.get(location)
        assert res.status_code == 303
        location = res.headers["location"]
        parsed_location = urlparse(location)

    res = await client.get(f"{parsed_location.path}?{parsed_location.query}")
    assert res.status_code == 204
    assert res.headers["set-cookie"].startswith("fastapiusersauth=")

    return res.headers["set-cookie"][len("fastapiusersauth=") :].split(";")[0]


@pytest.mark.oauth
async def test_oauth(registered_superuser_client, db, client):
    assert await _user_count(db) == 1
    assert await _oauth_count(db) == 0
    assert _email_count() == 0

    # Standard Login (fail)
    with pytest.raises(AssertionError):
        await _standard_login(client, "kilgore@kilgore.trout", "kilgore")

    # Register "kilgore@kilgore.trout" (the user from Dex) as regular account.
    res = await registered_superuser_client.post(
        "/auth/register/",
        json=dict(
            email="kilgore@kilgore.trout",
            password="kilgore",
            project_dir="/fake",
        ),
    )
    assert res.status_code == 201
    kilgore = res.json()

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert _email_count() == 0

    # Standard Login
    await _standard_login(client, "kilgore@kilgore.trout", "kilgore")
    # First OAuth Login
    await _oauth_login(client)

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 1
    assert _email_count() == 0

    # Change email into "kilgore@example.org".
    res = await registered_superuser_client.patch(
        f"/auth/users/{kilgore['id']}/",
        json=dict(email="kilgore@example.org"),
    )
    assert res.status_code == 200

    # Standard Login
    with pytest.raises(AssertionError):
        await _standard_login(client, "kilgore@kilgore.trout", "kilgore")
    await _standard_login(client, "kilgore@example.org", "kilgore")
    # OAuth Login
    await _oauth_login(client)

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 1
    assert _email_count() == 0

    # Remove all OAuth accounts from db.
    await db.execute(delete(OAuthAccount))
    await db.commit()

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert _email_count() == 0

    # Standard Login
    await _standard_login(client, "kilgore@example.org", "kilgore")
    # OAuth Login, for non-existing user "kilgore@kilgore.trout".
    # This will lead to an error,
    # and to an email being sent to the Fractal admins
    with pytest.raises(AssertionError):
        await _oauth_login(client)

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert _email_count() == 1
