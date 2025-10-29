"""
Required: scripts/oauth/docker-compose.yaml
"""
import httpx
import pytest
from httpx import AsyncClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import delete
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth.oauth import _create_client_oidc
from fractal_server.config import get_email_settings
from fractal_server.config import get_oauth_settings
from fractal_server.config import get_settings
from fractal_server.config import OAuthSettings
from fractal_server.syringe import Inject


DEX_URL = "http://127.0.0.1:5556"
MAILPIT_URL = "http://localhost:8025"


async def _user_count(db: AsyncSession) -> int:
    res = await db.execute(select(func.count(UserOAuth.id)))
    return res.scalar_one()


async def _oauth_count(db: AsyncSession) -> int:
    res = await db.execute(select(func.count(OAuthAccount.id)))
    return res.scalar_one()


def _mailpit_email_count() -> int:
    with httpx.Client() as client:
        response = client.get(f"{MAILPIT_URL}/api/v1/messages")
        response.raise_for_status()
        data = response.json()
        total = data.get("total")
    return total


def _mailpit_read_messages() -> list[dict]:
    messages = []

    with httpx.Client() as client:
        # Get message list
        r = client.get(f"{MAILPIT_URL}/api/v1/messages")
        r.raise_for_status()
        data = r.json()

        for msg in data.get("messages", []):
            msg_id = msg["ID"]

            # Get message details
            r_detail = client.get(f"{MAILPIT_URL}/api/v1/message/{msg_id}")
            r_detail.raise_for_status()
            detail = r_detail.json()

            # Get plain text or HTML body (optional)
            r_body = client.get(f"{MAILPIT_URL}/api/v1/message/{msg_id}/plain")
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


async def _oauth_login(client, oauth_settings: OAuthSettings) -> str:
    res = await client.get("/auth/dexidp/authorize/")
    authorization_url = res.json()["authorization_url"]

    with httpx.Client() as httpx_client:
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


@pytest.mark.oauth
async def test_oauth(registered_superuser_client, db, client):
    settings = Inject(get_settings)
    email_settings = Inject(get_email_settings)
    oauth_settings = Inject(get_oauth_settings)
    try:
        email_count = _mailpit_email_count()
        assert email_count == 0
    except httpx.ConnectError as e:
        raise RuntimeError(
            f"Cannot connect to Mailpit. Original error: '{e}'. "
            "Hint: is Mailpit container running? "
            "Try `docker compose -f scripts/oauth/docker-compose.yaml up -d`"
        )
    except AssertionError:
        raise RuntimeError(
            f"Mailpit has {email_count} messages in memory. Hint: try "
            "`docker compose -f scripts/oauth/docker-compose.yaml restart`"
        )

    assert await _user_count(db) == 1
    assert await _oauth_count(db) == 0
    assert _mailpit_email_count() == 0

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
    assert _mailpit_email_count() == 0

    # Standard Login
    token = await _standard_login(client, "kilgore@kilgore.trout", "kilgore")
    await _verify_token(client, token, "kilgore@kilgore.trout")
    # First OAuth Login
    token = await _oauth_login(client, oauth_settings)
    await _verify_token(client, token, "kilgore@kilgore.trout")

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 1
    assert _mailpit_email_count() == 0

    # Change email into "kilgore@example.org".
    res = await registered_superuser_client.patch(
        f"/auth/users/{kilgore['id']}/",
        json=dict(email="kilgore@example.org"),
    )
    assert res.status_code == 200

    # Standard Login
    with pytest.raises(AssertionError):
        await _standard_login(client, "kilgore@kilgore.trout", "kilgore")
    token = await _standard_login(client, "kilgore@example.org", "kilgore")
    await _verify_token(client, token, "kilgore@example.org")
    # OAuth Login
    token = await _oauth_login(client, oauth_settings)
    await _verify_token(client, token, "kilgore@example.org")

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 1
    assert _mailpit_email_count() == 0

    # Remove all OAuth accounts from db.
    await db.execute(delete(OAuthAccount))
    await db.commit()

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert _mailpit_email_count() == 0

    # Standard Login
    token = await _standard_login(client, "kilgore@example.org", "kilgore")
    await _verify_token(client, token, "kilgore@example.org")
    # OAuth Login, for non-existing user "kilgore@kilgore.trout".
    # This will lead to an error,
    # and to an email being sent to the Fractal admins
    with pytest.raises(AssertionError):
        await _oauth_login(client, oauth_settings)

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert _mailpit_email_count() == 1

    email = _mailpit_read_messages()[0]
    assert email["Subject"] == "[Fractal, test] New OAuth self-registration"
    assert email["From"]["Address"] == email_settings.FRACTAL_EMAIL_SENDER
    assert [
        e["Address"] for e in email["To"]
    ] == email_settings.FRACTAL_EMAIL_RECIPIENTS.split(",")
    assert email["ReturnPath"] == email_settings.FRACTAL_EMAIL_SENDER
    email_msg = email["Text"]
    assert "tried to self-register" in email_msg
    assert str(settings.FRACTAL_HELP_URL) in email_msg

    with pytest.raises(AssertionError):
        await _oauth_login(client, oauth_settings)

    assert await _user_count(db) == 2
    assert await _oauth_count(db) == 0
    assert _mailpit_email_count() == 2

    for msg in _mailpit_read_messages():
        msg["Text"] == email_msg


@pytest.mark.oauth
def test_unit_create_client_oidc():
    oauth_settings = Inject(get_oauth_settings)
    _create_client_oidc(oauth_settings)

    oauth_settings.OAUTH_OIDC_CONFIG_ENDPOINT = SecretStr("http://error.org")
    with pytest.raises(RuntimeError) as e:
        _create_client_oidc(oauth_settings)
    assert "Cannot initialize OpenID client" in str(e)
