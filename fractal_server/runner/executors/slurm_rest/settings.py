from typing import Literal
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from fractal_server.config._settings_config import SETTINGS_CONFIG_DICT


class SlurmRestSettings(BaseSettings):
    """
    Settings for the prototype SLURM REST runner.

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
            Whether to verify the TLS certificate of `slurmrestd`.
        FRACTAL_SLURM_REST_CA_BUNDLE:
            Optional CA file (PEM) for TLS verification, e.g. for an
            internal CA.
        FRACTAL_SLURM_REST_TIMEOUT:
            Timeout (seconds) of each HTTP request.
        FRACTAL_SLURM_REST_TOKEN_LIFESPAN:
            Lifespan (seconds) of tokens obtained via `scontrol token`.
        FRACTAL_SLURM_REST_JOB_PATH:
            Value of `PATH` in the SLURM-job environment.
        FRACTAL_SLURM_REST_MAX_POLL_FAILURES:
            Number of consecutive failed status queries before giving up.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_SLURM_REST_ENABLED: Literal["true", "false"] = "false"
    FRACTAL_SLURM_REST_URL: str | None = None
    FRACTAL_SLURM_REST_API_VERSION: str = "v0.0.44"
    FRACTAL_SLURM_REST_VERIFY_TLS: bool = True
    FRACTAL_SLURM_REST_CA_BUNDLE: str | None = None
    FRACTAL_SLURM_REST_TIMEOUT: float = 30.0
    FRACTAL_SLURM_REST_TOKEN_LIFESPAN: int = 3600
    FRACTAL_SLURM_REST_JOB_PATH: str = "/usr/bin:/bin"
    FRACTAL_SLURM_REST_MAX_POLL_FAILURES: int = 20

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
