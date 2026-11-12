from typing import Annotated

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict

from fractal_server.runner.executors.slurm_rest.client import (
    SUPPORTED_API_VERSIONS,
)
from fractal_server.types import AbsolutePathStr
from fractal_server.types import HttpUrlStr
from fractal_server.types import NonEmptyStr

from ._slurm import JobRunnerConfigSLURM


def _val_url(value: str) -> str:
    # Tokens are sent in request headers
    if not value.startswith("https://"):
        raise ValueError(f"Invalid `url` (https only): '{value}'.")
    return value.rstrip("/")


def _val_api_version(value: str) -> str:
    if value not in SUPPORTED_API_VERSIONS:
        raise ValueError(
            f"Unsupported `api_version` '{value}' "
            f"(supported: {list(SUPPORTED_API_VERSIONS)})."
        )
    return value


class SlurmRestConfig(BaseModel):
    """
    Connection to `slurmrestd`, for a `slurm_rest` resource.

    Attributes:
        url:
            HTTPS base URL of `slurmrestd` (without `/slurm/<version>`).
        api_version:
            `slurmrestd` OpenAPI version (e.g. `v0.0.44`), one of
            `SUPPORTED_API_VERSIONS` and served by `slurmrestd`.
        ca_bundle:
            Optional CA file (PEM) on the `fractal-server` machine, to verify
            the TLS certificate of `slurmrestd` (e.g. for an internal CA).
            If unset, the system CAs are used.
    """

    model_config = ConfigDict(extra="forbid")

    url: Annotated[HttpUrlStr, AfterValidator(_val_url)]
    api_version: Annotated[NonEmptyStr, AfterValidator(_val_api_version)]
    ca_bundle: AbsolutePathStr | None = None


class JobRunnerConfigSlurmRest(JobRunnerConfigSLURM):
    """
    Runner-configuration specifications, for a `slurm_rest` resource.

    Same as `JobRunnerConfigSLURM`, plus the `slurmrestd` connection.

    Valid JSON example
    ```json
    {
        "default_slurm_config": {
            "partition": "partition-name",
            "cpus_per_task": 1,
            "mem": "100M"
        },
        "batching_config": {
            "target_cpus_per_job": 1,
            "max_cpus_per_job": 1,
            "target_mem_per_job": 200,
            "max_mem_per_job": 500,
            "target_num_jobs": 2,
            "max_num_jobs": 4
        },
        "rest": {
            "url": "https://slurmrestd.example.org",
            "api_version": "v0.0.44",
            "ca_bundle": "/etc/ssl/internal-ca.pem"
        }
    }
    ```

    Attributes:
        rest: Connection to `slurmrestd`.
    """

    rest: SlurmRestConfig
