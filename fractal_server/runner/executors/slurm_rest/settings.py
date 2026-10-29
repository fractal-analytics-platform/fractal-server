from pathlib import Path
from typing import Literal
from typing import Self

from pydantic import HttpUrl
from pydantic import SecretStr
from pydantic import field_validator
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from fractal_server.config._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.syringe import Inject


class SlurmRestSettings(BaseSettings):
    """
    Settings for the SLURM REST runner.

    Kept separate from `fractal_server.config.Settings`, so that the existing
    configuration is not modified.

    Attributes:
        FRACTAL_SLURM_REST_ENABLED:
            If `"true"`, all `slurm_ssh` resources use the REST runner.
        FRACTAL_SLURM_REST_URL:
            HTTPS base URL of `slurmrestd` (without `/slurm/<version>`).
        FRACTAL_SLURM_REST_CA_BUNDLE:
            Optional CA file (PEM) for TLS verification, e.g. for an
            internal CA. If unset, the system CAs are used.
        FRACTAL_SLURM_REST_TOKEN:
            Development only, never in deployments. If set, this token is
            used for all requests of all Fractal users, instead of their
            SSO token. SLURM jobs run as its user (`sub`/`sun` claim, else
            `Profile.username`), and it is never refreshed.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_SLURM_REST_ENABLED: Literal["true", "false"] = "false"
    # TODO: move URL and CA bundle to `Resource` (e.g. with a
    # dedicated `slurm_rest` resource type), instead of global settings
    FRACTAL_SLURM_REST_URL: HttpUrl | None = None
    FRACTAL_SLURM_REST_CA_BUNDLE: str | None = None
    FRACTAL_SLURM_REST_TOKEN: SecretStr | None = None

    @field_validator("FRACTAL_SLURM_REST_URL")
    @classmethod
    def _https_only(cls, value: HttpUrl | None) -> HttpUrl | None:
        # Tokens are sent in request headers
        if value is not None and value.scheme != "https":
            raise ValueError("FRACTAL_SLURM_REST_URL must use https.")
        return value

    @field_validator("FRACTAL_SLURM_REST_CA_BUNDLE")
    @classmethod
    def _ca_bundle_exists(cls, value: str | None) -> str | None:
        if value is not None and not Path(value).is_file():
            raise ValueError(f"FRACTAL_SLURM_REST_CA_BUNDLE={value} not found.")
        return value

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

    @property
    def base_url(self) -> str:
        return str(self.FRACTAL_SLURM_REST_URL).rstrip("/")


def get_slurm_rest_settings(
    slurm_rest_settings: SlurmRestSettings = SlurmRestSettings(),
) -> SlurmRestSettings:
    return slurm_rest_settings


def is_slurm_rest_enabled() -> bool:
    settings = Inject(get_slurm_rest_settings)
    return settings.FRACTAL_SLURM_REST_ENABLED == "true"
