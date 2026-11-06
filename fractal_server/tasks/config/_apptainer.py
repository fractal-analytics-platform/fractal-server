from pydantic import BaseModel

from fractal_server.types import AbsolutePathStr
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
    apptainer_bin: AbsolutePathStr
    """
    Absolute path to the `apptainer` executable.
    """
    SLURM_CONFIG: SLURMConfig | None = None
    """
    SLURM configuration for the apptainer build. Required when using
    `apptainer` in a SSH/SLURM deployment.
    """
