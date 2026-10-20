"""
SLURM JWTs from the OAuth/OIDC (SSO) login of the Fractal user.

At every login, fractal-server stores the access token and refresh token of
the identity provider in the `oauthaccount` table. When `slurmrestd` trusts
the identity provider (`AuthAltTypes=auth/jwt` with a `jwks` file and
`userclaimfield=sub`), this access token is a valid SLURM token for the user
whose name is the `sub` claim.
"""

import asyncio
import base64
import json
import time
from typing import Any

import httpx
from httpx_oauth.oauth2 import BaseOAuth2
from httpx_oauth.oauth2 import OAuth2Error
from sqlalchemy.orm import Session
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import OAuthAccount
from fractal_server.config import OAuthSettings
from fractal_server.config import get_oauth_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)

# A token closer than this to its expiration is refreshed before use. It only
# has to cover clock skew and the request itself: it must stay well below the
# token lifetime (e.g. 300 s by default in Keycloak), or every request would
# trigger a refresh
REFRESH_MARGIN = 60


class SlurmTokenError(RuntimeError):
    """
    No valid SLURM token is available for the user.
    """


def jwt_claims(token: str) -> dict[str, Any]:
    """
    Claims of a JWT, without verifying its signature (`{}` if not a JWT).
    """
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    payload = parts[1]
    try:
        return json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
    except ValueError:
        return {}


def check_token_user(token: str, slurm_username: str) -> None:
    """
    Fail if the token is a JWT for a user other than `slurm_username`.

    `slurmrestd` runs the request as the user in the token (`sub` for
    identity-provider tokens, `sun` for `scontrol token` ones), not as the
    one in the `X-SLURM-USER-NAME` header.
    """
    claims = jwt_claims(token)
    token_user = claims.get("sub", claims.get("sun"))
    if token_user is not None and token_user != slurm_username:
        raise SlurmTokenError(
            f"Token user '{token_user}' differs from the SLURM user "
            f"'{slurm_username}'."
        )


def _expires_at(account: OAuthAccount) -> int | None:
    if account.expires_at is not None:
        return account.expires_at
    return jwt_claims(account.access_token).get("exp")


def _get_oauth_client(oauth_settings: OAuthSettings) -> BaseOAuth2:
    """
    Same OAuth client as in `get_oauth_router`, which is not reused so that
    `fractal_server.app.routes` is not modified.
    """
    # Imported here, to avoid loading the API routes in the runner modules
    from fractal_server.app.routes.auth.oauth import _create_client_github
    from fractal_server.app.routes.auth.oauth import _create_client_google
    from fractal_server.app.routes.auth.oauth import _create_client_oidc

    match oauth_settings.OAUTH_CLIENT_NAME:
        case "google":
            return _create_client_google(oauth_settings)
        case "github":
            return _create_client_github(oauth_settings)
        case _:
            return _create_client_oidc(oauth_settings)


def _refresh_with_identity_provider(refresh_token: str) -> dict[str, Any]:
    oauth_settings = Inject(get_oauth_settings)
    if not oauth_settings.is_set:
        raise SlurmTokenError("OAuth client is not configured.")
    client = _get_oauth_client(oauth_settings)
    # The runner runs in a worker thread, without a running event loop
    return asyncio.run(client.refresh_token(refresh_token))


class SsoTokenProvider:
    """
    Provide the SSO access token of a Fractal user as SLURM token.

    The token is read from the database and refreshed (with the stored
    refresh token) when it is about to expire or after `slurmrestd` rejects
    it. The refresh locks the database row, so that runners in other threads
    or server workers do not refresh it at the same time.

    Args:
        user_id: ID of the Fractal user.
        slurm_username:
            SLURM user (`Profile.username`); must match the `account_id`
            (`sub`) of the OAuth account.
    """

    source = "sso"

    def __init__(self, *, user_id: int, slurm_username: str) -> None:
        self.user_id = user_id
        self.slurm_username = slurm_username
        self._token: str | None = None
        self._expires_at: int | None = None

    def _is_usable(
        self, token: str, expires_at: int | None, rejected: str | None
    ) -> bool:
        if token == rejected:
            return False
        return expires_at is None or expires_at - time.time() > REFRESH_MARGIN

    def get_token(self, force_refresh: bool = False) -> str:
        """
        Args:
            force_refresh:
                If `True`, the last returned token was rejected: return a
                different one.
        """
        if (
            not force_refresh
            and self._token is not None
            and self._is_usable(self._token, self._expires_at, None)
        ):
            return self._token
        rejected = self._token if force_refresh else None
        with next(get_sync_db()) as db:
            account = self._get_account(db, lock=False)
            if not self._is_usable(
                account.access_token, _expires_at(account), rejected
            ):
                account = self._get_account(db, lock=True)
                # Another runner may have refreshed it in the meantime
                if not self._is_usable(
                    account.access_token, _expires_at(account), rejected
                ):
                    self._refresh(db, account)
            token, expires_at = account.access_token, _expires_at(account)
            # Release the row lock, if any
            db.commit()
        check_token_user(token, self.slurm_username)
        if token != self._token:
            expires_in = (
                None if expires_at is None else expires_at - int(time.time())
            )
            logger.info(
                f"Using SSO token of user {self.user_id} ({expires_in=} s)."
            )
        self._token, self._expires_at = token, expires_at
        return token

    def _get_account(self, db: Session, *, lock: bool) -> OAuthAccount:
        stm = (
            select(OAuthAccount)
            .where(OAuthAccount.user_id == self.user_id)
            .where(OAuthAccount.account_id == self.slurm_username)
        )
        if lock:
            # `populate_existing`: reload the row already in the session
            stm = stm.with_for_update().execution_options(
                populate_existing=True
            )
        accounts = db.execute(stm).scalars().all()
        if len(accounts) != 1:
            raise SlurmTokenError(
                f"Expected one OAuth account of user {self.user_id} for "
                f"SLURM user '{self.slurm_username}', found {len(accounts)}. "
                "Hint: the user must log in to Fractal via SSO."
            )
        return accounts[0]

    def _refresh(self, db: Session, account: OAuthAccount) -> None:
        if account.refresh_token is None:
            raise SlurmTokenError(
                f"SSO token of user {self.user_id} expired and cannot be "
                "refreshed. Hint: the user must log in to Fractal again."
            )
        try:
            new = _refresh_with_identity_provider(account.refresh_token)
        except (OAuth2Error, httpx.HTTPError) as e:
            raise SlurmTokenError(
                f"Cannot refresh the SSO token of user {self.user_id} "
                f"({type(e).__name__}: {e}). "
                "Hint: the user must log in to Fractal again."
            ) from None
        account.access_token = new["access_token"]
        account.expires_at = new.get("expires_at")
        # Some identity providers rotate the refresh token
        if new.get("refresh_token"):
            account.refresh_token = new["refresh_token"]
        db.add(account)
        db.flush()
        logger.info(
            f"Refreshed SSO token of user {self.user_id} "
            f"(expires_at={account.expires_at})."
        )
