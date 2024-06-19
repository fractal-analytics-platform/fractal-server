from pathlib import Path
from typing import Optional

from cfut.util import random_string

from fractal_server.app.runner.executors.slurm._slurm_config import (
    SlurmConfig,
)


class SlurmJob:
    """
    Collect information related to a FractalSlurmSSHExecutor job

    This includes three groups of attributes:

    1. Attributes related to the (possibly multi-task) SLURM job, e.g.
       submission-file path.
    2. Attributes related to single tasks, e.g. the paths of their input/output
       pickle files.
    3. SLURM configuration options, encoded in a SlurmConfig object.

    Note: A SlurmJob object is generally defined as a multi-task job. Jobs
    coming from the `map` method must have `single_task_submission=False` (even
    if `num_tasks_tot=1`), while jobs coming from `submit` must have it set to
    `True`.

    Attributes:
        num_tasks_tot:
            Total number of tasks to be executed as part of this SLURM job.
        single_task_submission:
            This must be `True` for jobs submitted as part of the `submit`
            method, and `False` for jobs coming from the `map` method.
        slurm_file_prefix:
            Prefix for SLURM-job related files (submission script and SLURM
            stdout/stderr); this is also needed in the
            `_copy_files_from_remote_to_local` method.
        wftask_file_prefixes:
            Prefix for files that are created as part of the functions
            submitted for execution on the `FractalSlurmSSHExecutor`; this is
            needed in the `_copy_files_from_remote_to_local` method, and also
            to construct the names of per-task input/output pickle files.
        wftask_subfolder_name:
            Name of the per-task subfolder (e.g. `7_task_name`).
        slurm_script:
            Path of SLURM submission script.
        slurm_stdout:
            Path of SLURM stdout file; if this includes `"%j"`, then this
            string will be replaced by the SLURM job ID upon `sbatch`
            submission.
        slurm_stderr:
            Path of SLURM stderr file; see `slurm_stdout` concerning `"%j"`.
        workerids:
            IDs that enter in the per-task input/output pickle files (one per
            task).
        input_pickle_files:
            Input pickle files (one per task).
        output_pickle_files:
            Output pickle files (one per task).
        slurm_config:
            `SlurmConfig` object.
    """

    # Job-related attributes
    num_tasks_tot: int
    single_task_submission: bool
    slurm_file_prefix: str
    slurm_script_local: Path
    slurm_script_remote: Path
    slurm_stdout_local: Path
    slurm_stdout_remote: Path
    slurm_stderr_local: Path
    slurm_stderr_remote: Path

    # Per-task attributes
    wftask_subfolder_name: str
    workerids: tuple[str, ...]
    wftask_file_prefixes: tuple[str, ...]
    input_pickle_files_local: tuple[Path, ...]
    input_pickle_files_remote: tuple[Path, ...]
    output_pickle_files_local: tuple[Path, ...]
    output_pickle_files_remote: tuple[Path, ...]

    # Slurm configuration
    slurm_config: SlurmConfig

    def __init__(
        self,
        num_tasks_tot: int,
        slurm_config: SlurmConfig,
        workflow_task_file_prefix: Optional[str] = None,
        slurm_file_prefix: Optional[str] = None,
        wftask_file_prefixes: Optional[tuple[str, ...]] = None,
        single_task_submission: bool = False,
    ):
        if single_task_submission and num_tasks_tot > 1:
            raise ValueError(
                "Trying to initialize SlurmJob with"
                f"{single_task_submission=} and {num_tasks_tot=}."
            )
        self.num_tasks_tot = num_tasks_tot
        self.single_task_submission = single_task_submission
        self.slurm_file_prefix = slurm_file_prefix or "default_slurm_prefix"
        if wftask_file_prefixes is None:
            self.wftask_file_prefixes = tuple(
                "default_wftask_prefix" for i in range(self.num_tasks_tot)
            )
        else:
            self.wftask_file_prefixes = wftask_file_prefixes
        self.workerids = tuple(
            random_string() for i in range(self.num_tasks_tot)
        )
        self.slurm_config = slurm_config

    def get_clean_output_pickle_files(self) -> tuple[str, ...]:
        """
        Transform all pathlib.Path objects in self.output_pickle_files to
        strings
        """
        return tuple(str(f.as_posix()) for f in self.output_pickle_files_local)
