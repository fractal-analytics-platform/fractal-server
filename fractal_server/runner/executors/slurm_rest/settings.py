from typing import Literal
from typing import Self

from pydantic import SecretStr
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from fractal_server.config._settings_config import SETTINGS_CONFIG_DICT


class SlurmRestSettings(BaseSettings):
    """
    Settings for the SLURM REST runner.

    Kept separate from `fractal_server.config.Settings`, so that the existing
    configuration is not modified.

    Attributes:
        FRACTAL_SLURM_REST_ENABLED:
            If `"true"`, all `slurm_ssh` resources use the REST runner.
        FRACTAL_SLURM_REST_URL:
            Base URL of `slurmrestd` (without `/slurm/<version>`).
        FRACTAL_SLURM_REST_API_VERSION:
            OpenAPI plugin version, e.g. `v0.0.44`.
        FRACTAL_SLURM_REST_VERIFY_TLS:
            If `"true"`, verify the TLS certificate of `slurmrestd`.
        FRACTAL_SLURM_REST_CA_BUNDLE:
            Optional CA file (PEM) for TLS verification, e.g. for an
            internal CA.
        FRACTAL_SLURM_REST_TOKEN_SOURCE:
            TEMPORARY, to be removed: in the end only `"sso"` is used.
            How SLURM tokens are obtained: `"scontrol"` (`scontrol token`
            over SSH, as the profile user) or `"sso"` (the OAuth/OIDC access
            token stored at the user's Fractal login, refreshed with the
            stored refresh token; `slurmrestd` must trust the identity
            provider and map its `sub` claim to `Profile.username`).
        FRACTAL_SLURM_REST_TOKEN:
            TEMPORARY, development only. If set, this token is used for all
            requests, instead of `FRACTAL_SLURM_REST_TOKEN_SOURCE`. Its user
            must match `Profile.username`, and it is never refreshed.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_SLURM_REST_ENABLED: Literal["true", "false"] = "false"
    # TODO: move URL, API version and CA bundle to `Resource`
    FRACTAL_SLURM_REST_URL: str | None = None
    FRACTAL_SLURM_REST_API_VERSION: str = "v0.0.44"
    FRACTAL_SLURM_REST_CA_BUNDLE: str | None = None
    FRACTAL_SLURM_REST_VERIFY_TLS: Literal["true", "false"] = "true"
    # TODO: remove this temporary setting (and `scontrol`), keeping only SSO
    FRACTAL_SLURM_REST_TOKEN_SOURCE: Literal["scontrol", "sso"] = "scontrol"
    # TODO: remove this temporary override
    FRACTAL_SLURM_REST_TOKEN: SecretStr | None = None

    @model_validator(mode="after")
    def _url_required_if_enabled(self) -> Self:
        if (
            self.FRACTAL_SLURM_REST_ENABLED == "true"
            and self.FRACTAL_SLURM_REST_URL is None
        ):
            raise ValueError(
                "FRACTAL_SLURM_REST_URL is required when "
                "FRACTAL_SLURM_REST_ENABLED=true."
            )
        return self


def get_slurm_rest_settings() -> SlurmRestSettings:
    return SlurmRestSettings()


def is_slurm_rest_enabled() -> bool:
    return get_slurm_rest_settings().FRACTAL_SLURM_REST_ENABLED == "true"
