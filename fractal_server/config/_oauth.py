from typing import Annotated
from typing import Self

from pydantic import model_validator
from pydantic import SecretStr
from pydantic import StringConstraints
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.types import NonEmptyStr


class OAuthSettings(BaseSettings):
    """
    Minimal set of configurations needed for operating on the database (e.g
    for schema migrations).
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    OAUTH_CLIENT_NAME: (
        Annotated[
            NonEmptyStr,
            StringConstraints(to_lower=True),
        ]
        | None
    ) = None
    """
    The name of the client.
    """
    OAUTH_CLIENT_ID: SecretStr | None = None
    """
    ID of client.
    """
    OAUTH_CLIENT_SECRET: SecretStr | None = None
    """
    Secret to authorise against the identity provider.
    """
    OAUTH_OIDC_CONFIG_ENDPOINT: SecretStr | None = None
    """
    OpenID configuration endpoint, for autodiscovery of relevant endpoints.
    """
    OAUTH_REDIRECT_URL: str | None = None
    """
    String to be used as `redirect_url` argument in
    `fastapi_users.get_oauth_router`, and then in
    `httpx_oauth.integrations.fastapi.OAuth2AuthorizeCallback`
    """

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
