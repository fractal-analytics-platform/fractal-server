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
    def input_pickle_file_local_path(self) -> Path:
        return (
            self.workdir_local / f"{self.prefix}-{self.component}-input.pickle"
        )

    @property
    def input_pickle_file_remote_path(self) -> Path:
        return (
            self.workdir_remote
            / f"{self.prefix}-{self.component}-input.pickle"
        )

    @property
    def output_pickle_file_local_path(self) -> Path:
        return (
            self.workdir_local
            / f"{self.prefix}-{self.component}-output.pickle"
        )

    @property
    def output_pickle_file_remote_path(self) -> Path:
        return (
            self.workdir_remote
            / f"{self.prefix}-{self.component}-output.pickle"
        )

    @property
    def input_pickle_file_local(self) -> str:
        return self.input_pickle_file_local_path.as_posix()

    @property
    def input_pickle_file_remote(self) -> str:
        return self.input_pickle_file_remote_path.as_posix()

    @property
    def output_pickle_file_local(self) -> str:
        return self.output_pickle_file_local_path.as_posix()

    @property
    def output_pickle_file_remote(self) -> str:
        return self.output_pickle_file_remote_path.as_posix()


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
    def slurm_stdout_remote_path(self) -> Path:
        return (
            self.workdir_remote
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.out"
        )

    @property
    def slurm_stdout_remote(self) -> str:
        return self.slurm_stdout_remote_path.as_posix()

    @property
    def slurm_stderr_remote_path(self) -> Path:
        return (
            self.workdir_remote
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.err"
        )

    @property
    def slurm_stderr_remote(self) -> str:
        return self.slurm_stderr_remote_path.as_posix()

    @property
    def slurm_stdout_local_path(self) -> str:
        return (
            self.workdir_local
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.out"
        )

    @property
    def slurm_stdout_local(self) -> str:
        return self.slurm_stdout_local_path.as_posix()

    @property
    def slurm_stderr_local_path(self) -> Path:
        return (
            self.workdir_local
            / f"{self.prefix}-slurm-{self.slurm_job_id_placeholder}.err"
        )

    @property
    def slurm_stderr_local(self) -> str:
        return self.slurm_stderr_local_path.as_posix()
