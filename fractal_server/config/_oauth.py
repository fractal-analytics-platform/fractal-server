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

    FRACTAL_OAUTH_CLIENT_NAME: (
        Annotated[
            NonEmptyStr,
            StringConstraints(to_lower=True),
        ]
        | None
    ) = None
    """
    The name of the client.
    """
    FRACTAL_OAUTH_CLIENT_ID: SecretStr | None = None
    """
    ID of client.
    """
    FRACTAL_OAUTH_CLIENT_SECRET: SecretStr | None = None
    """
    Secret to authorise against the identity provider.
    """
    FRACTAL_OIDC_CONFIG_ENDPOINT: str | None = None
    """
    OpenID configuration endpoint, for autodiscovery of relevant endpoints.
    """
    FRACTAL_OAUTH_REDIRECT_URL: str | None = None
    """
    String to be used as `redirect_url` argument in
    `fastapi_users.get_oauth_router`, and then in
    `httpx_oauth.integrations.fastapi.OAuth2AuthorizeCallback`
    """

    @model_validator(mode="after")
    def check_configuration(self: Self) -> Self:
        if (
            self.FRACTAL_OAUTH_CLIENT_NAME not in ["google", "github", None]
            and self.FRACTAL_OIDC_CONFIG_ENDPOINT is None
        ):
            raise ValueError(
                f"{self.FRACTAL_OIDC_CONFIG_ENDPOINT=} but "
                f"{self.FRACTAL_OAUTH_CLIENT_NAME=}"
            )
        return self

    @property
    def is_set(self):
        return all(
            [
                self.FRACTAL_OAUTH_CLIENT_NAME is not None,
                self.FRACTAL_OAUTH_CLIENT_ID is not None,
                self.FRACTAL_OAUTH_CLIENT_SECRET is not None,
            ]
        )
