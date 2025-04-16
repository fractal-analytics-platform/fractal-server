from fastapi import APIRouter

from . import cookie_backend
from . import fastapi_users
from ....config import get_settings
from ....syringe import Inject

router_oauth = APIRouter()


# OAUTH CLIENTS

# NOTE: settings.OAUTH_CLIENTS are collected by
# Settings.collect_oauth_clients(). If no specific client is specified in the
# environment variables (e.g. by setting OAUTH_FOO_CLIENT_ID and
# OAUTH_FOO_CLIENT_SECRET), this list is empty

# FIXME:Dependency injection should be wrapped within a function call to make
# it truly lazy. This function could then be called on startup of the FastAPI
# app (cf. fractal_server.main)
settings = Inject(get_settings)

for client_config in settings.OAUTH_CLIENTS_CONFIG:
    client_name = client_config.CLIENT_NAME.lower()

    if client_name == "google":
        from httpx_oauth.clients.google import GoogleOAuth2

        client = GoogleOAuth2(
            client_config.CLIENT_ID,
            client_config.CLIENT_SECRET.get_secret_value(),
        )
    elif client_name == "github":
        from httpx_oauth.clients.github import GitHubOAuth2

        client = GitHubOAuth2(
            client_config.CLIENT_ID,
            client_config.CLIENT_SECRET.get_secret_value(),
        )
    else:
        from httpx_oauth.clients.openid import OpenID

        client = OpenID(
            client_config.CLIENT_ID,
            client_config.CLIENT_SECRET.get_secret_value(),
            client_config.OIDC_CONFIGURATION_ENDPOINT,
        )

    router_oauth.include_router(
        fastapi_users.get_oauth_router(
            client,
            cookie_backend,
            settings.JWT_SECRET_KEY,
            is_verified_by_default=False,
            associate_by_email=True,
            redirect_url=client_config.REDIRECT_URL,
        ),
        prefix=f"/{client_name}",
    )


# Add trailing slash to all routes' paths
for route in router_oauth.routes:
    if not route.path.endswith("/"):
        route.path = f"{route.path}/"
