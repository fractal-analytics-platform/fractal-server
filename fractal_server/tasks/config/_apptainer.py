from pydantic import BaseModel

from fractal_server.types import NonEmptyStr

from ._slurm import TasksSLURMConfig


class TasksApptainerSettings(BaseModel):
    """
    Configuration for `apptainer`-based Task collection.
    """

    pixi_version: NonEmptyStr
    """
    `pixi` version used by apptainer.
    """
    apptainer_bin: NonEmptyStr = "apptainer"
    """
    Command used to invoke `apptainer` (e.g. `apptainer` or
    `/usr/bin/apptainer`).
    """
    pre_submission_cmd: str = ""
    """
    Command to run (on the SSH host) right before submitting the SLURM job
    for `apptainer build` (e.g. `module load apptainer`). If set, the
    `sbatch` command is run within a wrapper script, after this command.
    """
    build_env_vars: list[tuple[NonEmptyStr, str]] = [("APPTAINER_BINDPATH", "")]
    """
    Environment variables (as `(name, value)` pairs) for `apptainer build`.
    """
    SLURM_CONFIG: TasksSLURMConfig | None = None
    """
    SLURM configuration for the apptainer build. Required when using
    `apptainer` in a SSH/SLURM deployment.
    """

    @property
    def build_env_prefix(self) -> str:
        """
        Prefix string for the `apptainer build` command.
        """
        return "".join(
            f'{name}="{value}" ' for name, value in self.build_env_vars
        )
