"""
The following containers are required:

```yaml
version: "3.8"

services:
  mailpit:
    image: axllent/mailpit
    container_name: mailpit
    ports:
      - "1025:1025"
      - "8025:8025"
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
from fractal_server.config import get_email_settings
from fractal_server.syringe import Inject


email_settings = Inject(get_email_settings)


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


def read_mailpit_messages() -> list[dict]:
    base_url = "http://localhost:8025/api/v1"
    messages = []

    with httpx.Client() as client:
        # Get message list
        r = client.get(f"{base_url}/messages")
        r.raise_for_status()
        data = r.json()

        for msg in data.get("messages", []):
            msg_id = msg["ID"]

            # Get message details
            r_detail = client.get(f"{base_url}/message/{msg_id}")
            r_detail.raise_for_status()
            detail = r_detail.json()

            # Get plain text or HTML body (optional)
            r_body = client.get(f"{base_url}/message/{msg_id}/plain")
            if r_body.status_code == 200:
                detail["body"] = r_body.text
            else:
                detail["body"] = "(no body)"

            messages.append(detail)

    return messages


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

    email = read_mailpit_messages()[0]
    assert email["Subject"] == "[Fractal, test] New OAuth self-registration"
    assert email["From"]["Address"] == email_settings.FRACTAL_EMAIL_SENDER
    assert [
        e["Address"] for e in email["To"]
    ] == email_settings.FRACTAL_EMAIL_RECIPIENTS.split(",")
    assert email["ReturnPath"] == email_settings.FRACTAL_EMAIL_SENDER
    assert email["Text"] == (
        "User 'kilgore@kilgore.trout' tried to self-register "
        "through OAuth.\r\n"
        "Please create the Fractal account manually.\r\n"
        "Here is the error message displayed to the user:\r\n"
        "Thank you for registering for the Fractal service. "
        "Administrators have been informed to configure your account "
        "and will get back to you.\r\n"
        "You can find more information about the onboarding process "
        "at https://example.org/info.\r\n"
    )
