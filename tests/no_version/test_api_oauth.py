from urllib.parse import urlparse

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
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


def get_email_count() -> int:
    with httpx.Client() as client:
        response = client.get("http://localhost:8025/api/v1/messages")
        response.raise_for_status()
        data = response.json()
        total = data.get("total")
    return total


@pytest.mark.oauth
async def test_oauth(registered_superuser_client, db, client):
    assert await _user_count(db) == 1
    assert await _oauth_count(db) == 0
    assert get_email_count() == 0

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

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert get_email_count() == 0

    # First OAuth Login

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

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 1
    assert get_email_count() == 0
