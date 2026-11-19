from pydantic import BaseModel

from fractal_server.types import NonEmptyStr

from ._slurm import SLURMConfig


class TasksApptainerSettings(BaseModel):
    """
    Configuration for `apptainer`-based Task collection.
    """

    pixi_version: NonEmptyStr
    """
    `pixi` version used by apptainer.
    """
    apptainer_bin: NonEmptyStr
    """
    Command used to invoke `apptainer` (e.g. `apptainer` or
    `/usr/bin/apptainer`).
    """
    SLURM_CONFIG: SLURMConfig | None = None
    """
    SLURM configuration for the apptainer build. Required when using
    `apptainer` in a SSH/SLURM deployment.
    """
