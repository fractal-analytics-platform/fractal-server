import base64
import json
import time
from uuid import uuid4

import httpx
import pytest
from httpx_oauth.oauth2 import RefreshTokenError

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.runner.executors.slurm_rest import sso_token
from fractal_server.runner.executors.slurm_rest.settings import (
    SlurmRestSettings,
)
from fractal_server.runner.executors.slurm_rest.sso_token import SlurmTokenError
from fractal_server.runner.executors.slurm_rest.sso_token import (
    SsoTokenProvider,
)
from fractal_server.runner.executors.slurm_rest.sso_token import (
    check_token_user,
)
from fractal_server.runner.executors.slurm_rest.sso_token import jwt_claims
from fractal_server.runner.executors.slurm_rest.token import (
    ScontrolTokenProvider,
)
from fractal_server.runner.executors.slurm_rest.token import StaticTokenProvider
from fractal_server.runner.executors.slurm_rest.token import get_token_provider

USERNAME = "slurm-user"
# Shared SSH service account (`Profile.username`)
SSH_USERNAME = "ssh-service-user"


def _jwt(**claims) -> str:
    def encode(data: dict) -> str:
        raw = base64.urlsafe_b64encode(json.dumps(data).encode())
        return raw.decode().rstrip("=")

    return f"{encode(dict(alg='RS256'))}.{encode(claims)}.signature"


def test_jwt_claims_and_token_user():
    assert jwt_claims(_jwt(sub="a", exp=1)) == dict(sub="a", exp=1)
    assert jwt_claims("opaque-token") == {}
    assert jwt_claims("a.not-base64-json.c") == {}

    check_token_user(_jwt(sub=USERNAME), USERNAME)
    check_token_user(_jwt(sun=USERNAME), USERNAME)
    check_token_user("opaque-token", USERNAME)
    with pytest.raises(SlurmTokenError, match="differs"):
        check_token_user(_jwt(sub="other"), USERNAME)
    with pytest.raises(SlurmTokenError, match="differs"):
        check_token_user(_jwt(sun="other"), USERNAME)


def test_static_token_provider():
    token = _jwt(sub=USERNAME)
    provider = StaticTokenProvider(token=token, default_username=SSH_USERNAME)
    assert provider.slurm_username == USERNAME
    assert provider.get_token() == token
    with pytest.raises(SlurmTokenError, match="rejected"):
        provider.get_token(force_refresh=True)
    provider = StaticTokenProvider(
        token=_jwt(sun=USERNAME), default_username=SSH_USERNAME
    )
    assert provider.slurm_username == USERNAME
    provider = StaticTokenProvider(
        token="opaque-token", default_username=SSH_USERNAME
    )
    assert provider.slurm_username == SSH_USERNAME


def test_get_token_provider(monkeypatch, tmp_path, oauth_user):
    # Avoid reading a local `.fractal_server.env`
    monkeypatch.chdir(tmp_path)
    for key in ("FRACTAL_SLURM_REST_TOKEN", "FRACTAL_SLURM_REST_TOKEN_SOURCE"):
        monkeypatch.delenv(key, raising=False)
    user_id = oauth_user(access_token=_jwt(sub=USERNAME), expires_at=None)
    kwargs = dict(
        fractal_ssh=None, user_id=user_id, profile_username=SSH_USERNAME
    )

    settings = SlurmRestSettings()
    assert settings.FRACTAL_SLURM_REST_TOKEN_SOURCE == "scontrol"
    provider = get_token_provider(settings=settings, **kwargs)
    assert isinstance(provider, ScontrolTokenProvider)
    assert provider.slurm_username == SSH_USERNAME

    settings = SlurmRestSettings(FRACTAL_SLURM_REST_TOKEN_SOURCE="sso")
    provider = get_token_provider(settings=settings, **kwargs)
    assert isinstance(provider, SsoTokenProvider)
    assert provider.user_id == user_id
    assert provider.slurm_username == USERNAME

    token = _jwt(sub=USERNAME)
    monkeypatch.setenv("FRACTAL_SLURM_REST_TOKEN", token)
    monkeypatch.setenv("FRACTAL_SLURM_REST_TOKEN_SOURCE", "sso")
    settings = SlurmRestSettings()
    assert token not in repr(settings)
    provider = get_token_provider(settings=settings, **kwargs)
    assert isinstance(provider, StaticTokenProvider)
    assert provider.get_token() == token


@pytest.fixture
def oauth_user(db_sync):
    def _create(
        *,
        access_token: str,
        expires_at: int | None,
        refresh_token: str | None = "refresh-1",
        account_id: str = USERNAME,
        oauth_names: tuple[str, ...] = ("openid",),
    ) -> int:
        user = UserOAuth(
            email=f"sso-{uuid4().hex}@example.org",
            hashed_password="xxx",
            project_dirs=["/fake"],
            oauth_accounts=[
                OAuthAccount(
                    oauth_name=oauth_name,
                    access_token=access_token,
                    expires_at=expires_at,
                    refresh_token=refresh_token,
                    account_id=account_id,
                    account_email="sso-user@example.org",
                )
                for oauth_name in oauth_names
            ],
        )
        db_sync.add(user)
        db_sync.commit()
        return user.id

    return _create


def _get_account(db_sync, user_id: int) -> OAuthAccount:
    db_sync.expire_all()
    user = db_sync.get(UserOAuth, user_id)
    return user.oauth_accounts[0]


@pytest.fixture
def fake_idp(monkeypatch):
    """
    Replace the refresh request to the identity provider.
    """
    calls = []
    responses = []

    def _refresh(refresh_token: str) -> dict:
        calls.append(refresh_token)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(sso_token, "_refresh_with_identity_provider", _refresh)
    return calls, responses


def test_sso_token_valid(db_sync, oauth_user, fake_idp):
    calls, _ = fake_idp
    token = _jwt(sub=USERNAME)
    user_id = oauth_user(access_token=token, expires_at=int(time.time()) + 3600)
    provider = SsoTokenProvider(user_id=user_id)
    assert provider.get_token() == token
    assert provider.get_token() == token
    assert calls == []


def test_sso_token_expiry_from_jwt(db_sync, oauth_user, fake_idp):
    calls, responses = fake_idp
    old = _jwt(sub=USERNAME, exp=int(time.time()) + 30)
    new = _jwt(sub=USERNAME, n=2)
    user_id = oauth_user(access_token=old, expires_at=None)
    responses.append(dict(access_token=new, expires_at=None))
    provider = SsoTokenProvider(user_id=user_id)
    assert provider.get_token() == new
    assert calls == ["refresh-1"]
    # No `exp` in the new token, nor `expires_at`: used until rejected
    assert provider.get_token() == new
    assert calls == ["refresh-1"]


def test_sso_token_refresh(db_sync, oauth_user, fake_idp):
    calls, responses = fake_idp
    now = int(time.time())
    old = _jwt(sub=USERNAME, n=1)
    new = _jwt(sub=USERNAME, n=2)
    newer = _jwt(sub=USERNAME, n=3)
    # About to expire
    user_id = oauth_user(access_token=old, expires_at=now + 30)
    # Short-lived tokens (300 s) are refreshed only once
    responses.append(
        dict(access_token=new, expires_at=now + 300, refresh_token="refresh-2")
    )
    provider = SsoTokenProvider(user_id=user_id)
    assert provider.get_token() == new
    assert provider.get_token() == new
    assert calls == ["refresh-1"]
    account = _get_account(db_sync, user_id)
    assert account.access_token == new
    assert account.expires_at == now + 300
    assert account.refresh_token == "refresh-2"

    # Rejected by slurmrestd: refreshed even if not expired; the refresh
    # token is kept if the identity provider does not rotate it
    responses.append(dict(access_token=newer, expires_at=now + 7200))
    assert provider.get_token(force_refresh=True) == newer
    assert calls == ["refresh-1", "refresh-2"]
    account = _get_account(db_sync, user_id)
    assert account.access_token == newer
    assert account.refresh_token == "refresh-2"


def test_sso_token_refreshed_elsewhere(db_sync, oauth_user, fake_idp):
    """
    After a rejection, a token refreshed by another runner (or a new login)
    is used without a new refresh.
    """
    calls, _ = fake_idp
    now = int(time.time())
    old = _jwt(sub=USERNAME, n=1)
    new = _jwt(sub=USERNAME, n=2)
    user_id = oauth_user(access_token=old, expires_at=now + 3600)
    provider = SsoTokenProvider(user_id=user_id)
    assert provider.get_token() == old

    account = _get_account(db_sync, user_id)
    account.access_token = new
    db_sync.add(account)
    db_sync.commit()

    # Cached
    assert provider.get_token() == old
    assert provider.get_token(force_refresh=True) == new
    assert calls == []


def test_sso_token_errors(db_sync, oauth_user, fake_idp):
    calls, responses = fake_idp
    now = int(time.time())

    # No SSO account
    user_id = oauth_user(
        access_token=_jwt(sub=USERNAME), expires_at=None, oauth_names=()
    )
    with pytest.raises(SlurmTokenError, match="No SSO account"):
        SsoTokenProvider(user_id=user_id)

    # Several SSO accounts
    user_id = oauth_user(
        access_token=_jwt(sub=USERNAME),
        expires_at=None,
        oauth_names=("openid", "github"),
    )
    with pytest.raises(SlurmTokenError, match="several SSO accounts"):
        SsoTokenProvider(user_id=user_id)

    # Token of another user than the account
    user_id = oauth_user(access_token=_jwt(sub="other"), expires_at=now + 3600)
    provider = SsoTokenProvider(user_id=user_id)
    assert provider.slurm_username == USERNAME
    with pytest.raises(SlurmTokenError, match="differs"):
        provider.get_token()

    # Expired, no refresh token
    account = _get_account(db_sync, user_id)
    account.access_token = _jwt(sub=USERNAME)
    account.expires_at = now - 10
    account.refresh_token = None
    db_sync.add(account)
    db_sync.commit()
    with pytest.raises(SlurmTokenError, match="cannot be refreshed"):
        provider.get_token()

    # Refresh refused by the identity provider: database unchanged
    account = _get_account(db_sync, user_id)
    account.refresh_token = "refresh-1"
    db_sync.add(account)
    db_sync.commit()
    responses.append(RefreshTokenError("invalid_grant"))
    with pytest.raises(SlurmTokenError, match="log in to Fractal again"):
        provider.get_token()
    assert calls == ["refresh-1"]
    account = _get_account(db_sync, user_id)
    assert account.expires_at == now - 10
    assert account.refresh_token == "refresh-1"


def test_refresh_with_identity_provider(
    monkeypatch, override_oauth_settings_factory
):
    override_oauth_settings_factory(
        OAUTH_CLIENT_NAME=None, OAUTH_CLIENT_ID=None, OAUTH_CLIENT_SECRET=None
    )
    with pytest.raises(SlurmTokenError, match="not configured"):
        sso_token._refresh_with_identity_provider("refresh-1")

    class FakeClient:
        async def refresh_token(self, refresh_token: str) -> dict:
            return dict(access_token=f"new-from-{refresh_token}")

    monkeypatch.setattr(sso_token, "_get_oauth_client", lambda _: FakeClient())
    override_oauth_settings_factory(
        OAUTH_CLIENT_NAME="github",
        OAUTH_CLIENT_ID="id",
        OAUTH_CLIENT_SECRET="secret",
    )
    assert sso_token._refresh_with_identity_provider("refresh-1") == dict(
        access_token="new-from-refresh-1"
    )


def test_sso_token_refused_refresh_not_retried(db_sync, oauth_user, fake_idp):
    """
    A refresh token refused by the identity provider is not sent again,
    until a new login replaces it; network errors are retried.
    """
    calls, responses = fake_idp
    now = int(time.time())
    user_id = oauth_user(access_token=_jwt(sub=USERNAME), expires_at=now - 10)
    provider = SsoTokenProvider(user_id=user_id)

    def _idp_error(status_code: int, **body) -> RefreshTokenError:
        response = httpx.Response(
            status_code,
            json=body or None,
            request=httpx.Request("POST", "https://idp.example.org/token"),
        )
        return RefreshTokenError("refresh failed", response)

    # Retried at the next call: network error, client misconfiguration,
    # server error, non-JSON answer
    responses.extend(
        [
            RefreshTokenError("connection failed"),
            _idp_error(401, error="invalid_client"),
            _idp_error(500),
            _idp_error(400),
        ]
    )
    for _ in range(4):
        with pytest.raises(SlurmTokenError, match="log in to Fractal again"):
            provider.get_token()
    assert calls == ["refresh-1"] * 4

    # Refused (answer of Roche WAM): not sent again
    responses.append(
        _idp_error(
            400,
            error="invalid_grant",
            error_description="unknown, invalid, or expired refresh token",
        )
    )
    with pytest.raises(SlurmTokenError, match="log in to Fractal again"):
        provider.get_token()
    with pytest.raises(SlurmTokenError, match="already refused"):
        provider.get_token()
    assert calls == ["refresh-1"] * 5

    # New login, with an expired access token: refreshed again
    account = _get_account(db_sync, user_id)
    account.refresh_token = "refresh-2"
    db_sync.add(account)
    db_sync.commit()
    new = _jwt(sub=USERNAME, n=2)
    responses.append(dict(access_token=new, expires_at=now + 3600))
    assert provider.get_token() == new
    assert calls == ["refresh-1"] * 5 + ["refresh-2"]
