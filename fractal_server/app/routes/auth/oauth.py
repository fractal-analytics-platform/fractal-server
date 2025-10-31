from fastapi import APIRouter
from httpx_oauth.clients.github import GitHubOAuth2
from httpx_oauth.clients.google import GoogleOAuth2
from httpx_oauth.clients.openid import OpenID
from httpx_oauth.clients.openid import OpenIDConfigurationError

from . import cookie_backend
from . import fastapi_users
from fractal_server.config import get_oauth_settings
from fractal_server.config import get_settings
from fractal_server.config import OAuthSettings
from fractal_server.syringe import Inject


def _create_client_github(cfg: OAuthSettings) -> GitHubOAuth2:
    return GitHubOAuth2(
        client_id=cfg.OAUTH_CLIENT_ID.get_secret_value(),
        client_secret=cfg.OAUTH_CLIENT_SECRET.get_secret_value(),
    )


def _create_client_google(cfg: OAuthSettings) -> GoogleOAuth2:
    return GoogleOAuth2(
        client_id=cfg.OAUTH_CLIENT_ID.get_secret_value(),
        client_secret=cfg.OAUTH_CLIENT_SECRET.get_secret_value(),
    )


def _create_client_oidc(cfg: OAuthSettings) -> OpenID:
    try:
        open_id = OpenID(
            client_id=cfg.OAUTH_CLIENT_ID.get_secret_value(),
            client_secret=cfg.OAUTH_CLIENT_SECRET.get_secret_value(),
            openid_configuration_endpoint=cfg.OAUTH_OIDC_CONFIG_ENDPOINT.get_secret_value(),  # noqa
        )
    except OpenIDConfigurationError as e:
        OAUTH_OIDC_CONFIG_ENDPOINT = (
            cfg.OAUTH_OIDC_CONFIG_ENDPOINT.get_secret_value()
        )
        raise RuntimeError(
            f"Cannot initialize OpenID client. Original error: '{e}'. "
            f"Hint: is {OAUTH_OIDC_CONFIG_ENDPOINT=} reachable?"
        )
    return open_id


def get_oauth_router() -> APIRouter | None:
    """
    Get the `APIRouter` object for OAuth endpoints.
    """
    router_oauth = APIRouter()
    settings = Inject(get_settings)
    oauth_settings = Inject(get_oauth_settings)
    if not oauth_settings.is_set:
        return None

    client_name = oauth_settings.OAUTH_CLIENT_NAME
    if client_name == "google":
        client = _create_client_google(oauth_settings)
    elif client_name == "github":
        client = _create_client_github(oauth_settings)
    else:
        client = _create_client_oidc(oauth_settings)

    router_oauth.include_router(
        fastapi_users.get_oauth_router(
            client,
            cookie_backend,
            settings.JWT_SECRET_KEY,
            is_verified_by_default=False,
            associate_by_email=True,
            redirect_url=oauth_settings.OAUTH_REDIRECT_URL,
        ),
        prefix=f"/{client_name}",
    )

    # Add trailing slash to all routes' paths
    for route in router_oauth.routes:
        if not route.path.endswith("/"):
            route.path = f"{route.path}/"

    return router_oauth
