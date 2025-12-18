from typing import Annotated
from typing import Self

from pydantic import SecretStr
from pydantic import StringConstraints
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from fractal_server.types import NonEmptyStr

from ._settings_config import SETTINGS_CONFIG_DICT


class OAuthSettings(BaseSettings):
    """
    Settings for integration with an OAuth identity provider.

    Attributes:
        OAUTH_CLIENT_NAME: Name of the client.
        OAUTH_CLIENT_ID: ID of client.
        OAUTH_CLIENT_SECRET:
            Secret to authorise against the identity provider.
        OAUTH_OIDC_CONFIG_ENDPOINT:
            OpenID Connect configuration endpoint, for autodiscovery of
            relevant endpoints.
        OAUTH_REDIRECT_URL:
            String to be used as `redirect_url` argument in
            `fastapi_users.get_oauth_router`, and then in
            `httpx_oauth.integrations.fastapi.OAuth2AuthorizeCallback`.
        OAUTH_EMAIL_CLAIM:
            Name of the OIDC claim with the user's email address. This is
            `"email"` by default, but can be customized (e.g. to `"mail"`) to
            fit with the response from the userinfo endpoint - see
            https://openid.net/specs/openid-connect-core-1_0.html#UserInfoResponse
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    OAUTH_CLIENT_NAME: (
        Annotated[
            NonEmptyStr,
            StringConstraints(to_lower=True),
        ]
        | None
    ) = None
    OAUTH_CLIENT_ID: SecretStr | None = None
    OAUTH_CLIENT_SECRET: SecretStr | None = None
    OAUTH_OIDC_CONFIG_ENDPOINT: SecretStr | None = None
    OAUTH_REDIRECT_URL: str | None = None
    OAUTH_EMAIL_CLAIM: str = "email"

    @model_validator(mode="after")
    def check_configuration(self: Self) -> Self:
        if (
            self.OAUTH_CLIENT_NAME not in ["google", "github", None]
            and self.OAUTH_OIDC_CONFIG_ENDPOINT is None
        ):
            raise ValueError(
                f"self.OAUTH_OIDC_CONFIG_ENDPOINT=None but "
                f"{self.OAUTH_CLIENT_NAME=}"
            )
        return self

    @property
    def is_set(self) -> bool:
        return None not in (
            self.OAUTH_CLIENT_NAME,
            self.OAUTH_CLIENT_ID,
            self.OAUTH_CLIENT_SECRET,
        )
