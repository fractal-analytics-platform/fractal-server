# TODO: move this file to the appropriate path
from pydantic import BaseModel


class SlurmSshUserSettings(BaseModel):
    """
    Subset of user settings which must be present for task collection and job
    execution when using the Slurm-SSH runner.

    Attributes:
        ssh_host: SSH-reachable host where a SLURM client is available.
        ssh_username: User on `ssh_host`.
        ssh_private_key_path: Path of private SSH key for `ssh_username`.
        ssh_tasks_dir: Task-venvs base folder on `ssh_host`.
        ssh_jobs_dir: Jobs base folder on `ssh_host`.
        slurm_accounts:
            List of SLURM accounts, to be used upon Fractal job submission.
    """

    ssh_host: str
    ssh_username: str
    ssh_private_key_path: str
    ssh_tasks_dir: str
    ssh_jobs_dir: str
    slurm_accounts: list[str]


class SlurmSudoUserSettings(BaseModel):
    """
    Subset of user settings which must be present for task collection and job
    execution when using the Slurm-sudo runner.

    Attributes:
        slurm_user: User to be impersonated via `sudo -u`.
        cache_dir: Folder where `slurm_user` can write.
        slurm_accounts:
            List of SLURM accounts, to be used upon Fractal job submission.
    """

    slurm_user: str
    cache_dir: str
    slurm_accounts: list[str]
