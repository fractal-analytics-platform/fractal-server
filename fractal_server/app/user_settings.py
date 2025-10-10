# TODO: move this file to the appropriate path
from pydantic import BaseModel


class SlurmSshUserSettings(BaseModel):
    """
    Subset of user settings which must be present for task collection and job
    execution when using the Slurm-SSH runner.

    Attributes:
        project_dir: Folder where `slurm_user` can write.
        slurm_accounts:
            List of SLURM accounts, to be used upon Fractal job submission.
    """

    project_dir: str
    slurm_accounts: list[str]


class SlurmSudoUserSettings(BaseModel):
    """
    Subset of user settings which must be present for task collection and job
    execution when using the Slurm-sudo runner.

    Attributes:
        project_dir: Folder where `slurm_user` can write.
        slurm_accounts:
            List of SLURM accounts, to be used upon Fractal job submission.
    """

    project_dir: str
    slurm_accounts: list[str]
