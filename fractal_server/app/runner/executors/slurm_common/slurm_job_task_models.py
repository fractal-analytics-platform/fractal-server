from pathlib import Path
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict

from fractal_server.app.runner.task_files import TaskFiles


class SlurmTask(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    component: str
    prefix: str
    workdir_local: Path
    workdir_remote: Path
    parameters: dict[str, Any]
    zarr_url: Optional[str] = None
    task_files: TaskFiles
    index: int

    @property
    def input_pickle_file_local(self) -> str:
        return (
            self.workdir_local / f"{self.prefix}-{self.component}-input.pickle"
        ).as_posix()

    @property
    def input_pickle_file_remote(self) -> str:
        return (
            self.workdir_remote
            / f"{self.prefix}-{self.component}-input.pickle"
        ).as_posix()

    @property
    def output_pickle_file_local(self) -> str:
        return (
            self.workdir_local
            / f"{self.prefix}-{self.component}-output.pickle"
        ).as_posix()

    @property
    def output_pickle_file_remote(self) -> str:
        return (
            self.workdir_remote
            / f"{self.prefix}-{self.component}-output.pickle"
        ).as_posix()


class SlurmJob(BaseModel):
    slurm_job_id: Optional[str] = None
    prefix: str
    workdir_local: Path
    workdir_remote: Path
    tasks: list[SlurmTask]

    @property
    def slurm_submission_script_local(self) -> str:
        return (
            self.workdir_local / f"{self.prefix}-slurm-submit.sh"
        ).as_posix()

    @property
    def slurm_submission_script_remote(self) -> str:
        return (
            self.workdir_remote / f"{self.prefix}-slurm-submit.sh"
        ).as_posix()

    @property
    def slurm_job_id_placeholder(self) -> str:
        if self.slurm_job_id:
            return self.slurm_job_id
        else:
            return "%j"

    @property
    def slurm_stdout_remote(self) -> str:
        return (
            self.workdir_remote
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.out"
        ).as_posix()

    @property
    def slurm_stderr_remote(self) -> str:
        return (
            self.workdir_remote
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.err"
        ).as_posix()

    @property
    def slurm_stdout_local(self) -> str:
        return (
            self.workdir_local
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.out"
        ).as_posix()

    @property
    def slurm_stderr_local(self) -> str:
        return (
            self.workdir_local
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.err"
        ).as_posix()
