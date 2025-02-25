import json
import logging
import shlex
import subprocess  # nosec
import sys
import time
from pathlib import Path
from typing import Any
from typing import Optional

import cloudpickle
from pydantic import BaseModel

from ._check_jobs_status import get_finished_jobs
from ._subprocess_run_as_user import _mkdir_as_user
from ._subprocess_run_as_user import _path_exists_as_user
from ._subprocess_run_as_user import _run_command_as_user
from fractal_server import __VERSION__
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_all_images
from fractal_server.app.history import update_single_image
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)


def _handle_exception_proxy(proxy):  # FIXME
    if proxy.exc_type_name == "JobExecutionError":
        return JobExecutionError(str(proxy))
    else:
        kwargs = {}
        for key in [
            "workflow_task_id",
            "workflow_task_order",
            "task_name",
        ]:
            if key in proxy.kwargs.keys():
                kwargs[key] = proxy.kwargs[key]
        return TaskExecutionError(proxy.tb, **kwargs)


class SlurmTask(BaseModel):
    label: str
    workdir_local: Path
    workdir_remote: Path
    zarr_url: Optional[str] = None

    log_file_local: Optional[str] = None
    log_file_remote: Optional[str] = None
    args_file_local: Optional[str] = None
    args_file_remote: Optional[str] = None

    @property
    def input_pickle_file_local(self) -> str:
        return (self.workdir_local / f"{self.label}-input.pickle").as_posix()

    @property
    def output_pickle_file_local(self) -> str:
        return (self.workdir_local / f"{self.label}-output.pickle").as_posix()

    @property
    def input_pickle_file_remote(self) -> str:
        return (self.workdir_remote / f"{self.label}-input.pickle").as_posix()

    @property
    def output_pickle_file_remote(self) -> str:
        return (self.workdir_remote / f"{self.label}-output.pickle").as_posix()


class SlurmJob(BaseModel):
    slurm_job_id: Optional[str] = None
    label: str
    workdir_local: Path
    workdir_remote: Path
    tasks: tuple[SlurmTask]

    @property
    def slurm_log_file_local(self) -> str:
        return (
            self.workdir_local / f"slurm-{self.label}-{self.slurm_job_id}.log"
        ).as_posix()

    @property
    def slurm_log_file_remote(self) -> str:
        if self.slurm_job_id:
            return (
                self.workdir_remote
                / f"slurm-{self.label}-{self.slurm_job_id}.log"
            ).as_posix()
        else:
            return (
                self.workdir_remote / f"slurm-{self.label}-%j.log"
            ).as_posix()

    @property
    def slurm_submission_script_local(self) -> str:
        return (
            self.workdir_local / f"slurm-{self.label}-submit.sh"
        ).as_posix()

    @property
    def slurm_submission_script_remote(self) -> str:
        return (
            self.workdir_remote / f"slurm-{self.label}-submit.sh"
        ).as_posix()


def _subprocess_run_or_raise(
    full_command: str,
) -> Optional[subprocess.CompletedProcess]:
    try:
        output = subprocess.run(  # nosec
            shlex.split(full_command),
            capture_output=True,
            check=True,
            encoding="utf-8",
        )
        return output
    except subprocess.CalledProcessError as e:
        error_msg = (
            f"Submit command `{full_command}` failed. "
            f"Original error:\n{str(e)}\n"
            f"Original stdout:\n{e.stdout}\n"
            f"Original stderr:\n{e.stderr}\n"
        )
        logging.error(error_msg)
        raise JobExecutionError(info=error_msg)


class RunnerSlurmSudo(BaseRunner):
    slurm_user: str
    slurm_user: str
    shutdown_file: str
    common_script_lines: list[str]
    user_cache_dir: str
    root_dir_local: Path
    root_dir_remote: Path
    slurm_account: Optional[str] = None
    poll_interval: int
    python_worker_interpreter: str

    def __init__(
        self,
        *,
        slurm_user: str,
        root_dir_local: Path,
        root_dir_remote: Path,
        slurm_account: Optional[str] = None,
        common_script_lines: Optional[list[str]] = None,
        user_cache_dir: Optional[str] = None,
        slurm_poll_interval: Optional[int] = None,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """

        self.slurm_user = slurm_user
        self.slurm_account = slurm_account
        self.common_script_lines = common_script_lines or []

        # Check that SLURM account is not set here
        try:
            invalid_line = next(
                line
                for line in self.common_script_lines
                if line.startswith("#SBATCH --account=")
            )
            self._stop_and_join_wait_thread()
            raise RuntimeError(
                "Invalid line in `FractalSlurmExecutor.common_script_lines`: "
                f"'{invalid_line}'.\n"
                "SLURM account must be set via the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
        except StopIteration:
            pass

        # Check Python versions
        settings = Inject(get_settings)
        if settings.FRACTAL_SLURM_WORKER_PYTHON is not None:
            self.check_remote_python_interpreter()

        self.root_dir_local = root_dir_local
        self.root_dir_remote = root_dir_remote
        if not _path_exists_as_user(
            path=root_dir_remote.as_posix(),
            user=self.slurm_user,
        ):
            logger.info(f"Missing folder {root_dir_remote=}")

        self.user_cache_dir = user_cache_dir

        self.slurm_poll_interval = (
            slurm_poll_interval or settings.FRACTAL_SLURM_POLL_INTERVAL
        )

        self.shutdown_file = (
            self.root_dir_local / SHUTDOWN_FILENAME
        ).as_posix()

        self.python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def submit_single_sbatch(
        self,
        func,
        parameters,
        slurm_job: SlurmJob,
    ) -> str:
        if len(slurm_job.tasks) > 1:
            raise NotImplementedError()

        # Prepare input pickle(s)
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        for task in slurm_job.tasks:
            _args = []
            # TODO: make parameters task-dependent
            _kwargs = dict(parameters=parameters)
            funcser = cloudpickle.dumps((versions, func, _args, _kwargs))
            with open(task.input_pickle_file_local, "wb") as f:
                f.write(funcser)

        # Prepare commands to be included in SLURM submission script

        preamble_lines = [
            "#!/bin/bash",
            "#SBATCH --partition=main",
            "#SBATCH --ntasks=1",
            "#SBATCH --cpus-per-task=1",
            "#SBATCH --mem=10M",
            f"#SBATCH --err={slurm_job.slurm_log_file_remote}",
            f"#SBATCH --out={slurm_job.slurm_log_file_remote}",
            f"#SBATCH -D {slurm_job.workdir_remote}",
            "#SBATCH --job-name=test",
            "\n",
        ]

        cmdlines = []
        for task in slurm_job.tasks:
            cmd = (
                f"{self.python_worker_interpreter}"
                " -m fractal_server.app.runner.executors.slurm_common.remote "
                f"--input-file {task.input_pickle_file_local} "
                f"--output-file {task.output_pickle_file_remote}"
            )
            cmdlines.append(
                f"srun --ntasks=1 --cpus-per-task=1 --mem=10MB {cmd} &"
            )
        cmdlines.append("wait\n")

        # Write submission script
        submission_script_contents = "\n".join(preamble_lines + cmdlines)
        with open(slurm_job.slurm_submission_script_local, "w") as f:
            f.write(submission_script_contents)

        # Run sbatch
        pre_command = f"sudo --set-home --non-interactive -u {self.slurm_user}"
        submit_command = (
            f"sbatch --parsable {slurm_job.slurm_submission_script_local}"
        )
        full_command = f"{pre_command} {submit_command}"

        # Submit SLURM job and retrieve job ID
        res = _subprocess_run_or_raise(full_command)
        job_id = int(res.stdout)
        return str(job_id)

    def copy_files_from_remote_to_local(self, job: SlurmJob) -> None:
        source_target_list = [
            (job.slurm_log_file_remote, job.slurm_log_file_local)
        ]
        for task in job.tasks:
            source_target_list.append(
                (task.output_pickle_file_remote, task.output_pickle_file_local)
            )
            # FIXME: add logfile
            # FIXME: add args file (or not?)

        for source, target in source_target_list:
            # NOTE: By setting encoding=None, we read/write bytes instead
            # of strings; this is needed to also handle pickle files.
            try:
                res = _run_command_as_user(
                    cmd=f"cat {source}",
                    user=self.slurm_user,
                    encoding=None,
                    check=True,
                )
                # Write local file
                with open(target, "wb") as f:
                    f.write(res.stdout)
            except RuntimeError as e:
                logger.warning(
                    f"SKIP copy {source} into {target}. "
                    f"Original error: {str(e)}"
                )
            logger.debug(f"Copied {source} into {target}")

    def postprocess_single_task(
        self, *, task: SlurmTask
    ) -> tuple[Any, Exception]:
        try:
            if not Path(task.output_pickle_file_local).exists():
                # TODO: Prepare appropriate exception
                exception = JobExecutionError(
                    f"Missing {task.output_pickle_file_local}"
                )
                return None, exception
            else:
                with open(task.output_pickle_file_local, "rb") as f:
                    outdata = f.read()
                success, output = cloudpickle.loads(outdata)
                if success:
                    result = output
                    return result, None
                else:
                    exception = _handle_exception_proxy(output)
                    return None, exception
        except Exception as e:
            exception = JobExecutionError(f"unclear, {str(e)}")
            return None, exception
        finally:
            Path(task.input_pickle_file_local).unlink(missing_ok=True)
            Path(task.output_pickle_file_local).unlink(missing_ok=True)

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_item_id: int,
        workdir_local: Path,
        workdir_remote: Path,
        in_compound_task: bool = False,
        **kwargs,
    ) -> tuple[Any, Exception]:
        # Validation phase
        self.validate_submit_parameters(parameters)

        # Create folders
        workdir_local.mkdir(parents=True, exist_ok=True)
        _mkdir_as_user(
            folder=workdir_remote.as_posix(),
            user=self.slurm_user,
        )

        # Submission phase
        slurm_job = SlurmJob(
            label="0",
            workdir_local=workdir_local,
            workdir_remote=workdir_remote,
            tasks=[
                SlurmTask(
                    label="0",
                    workdir_remote=workdir_remote,
                    workdir_local=workdir_local,
                )
            ],
        )  # TODO: replace with actual values
        slurm_job_id = self.submit_single_sbatch(
            func,
            parameters=parameters,
            slurm_job=slurm_job,
        )
        slurm_job.slurm_job_id = slurm_job_id
        jobs = {slurm_job_id: slurm_job}

        # Retrieval phase
        while len(jobs) > 0:
            # TODO: Check shutdown condition, and act accordingly
            finished_jobs = get_finished_jobs(job_ids=[slurm_job_id])
            if slurm_job_id in finished_jobs:
                slurm_job = jobs.pop(slurm_job_id)
                self.copy_files_from_remote_to_local(slurm_job)
                result, exception = self.postprocess_single_task(
                    task=slurm_job.tasks[0]
                )
                if not in_compound_task:
                    if exception is None:
                        update_all_images(
                            history_item_id=history_item_id,
                            status=HistoryItemImageStatus.DONE,
                        )
                    else:
                        update_all_images(
                            history_item_id=history_item_id,
                            status=HistoryItemImageStatus.FAILED,
                        )
            time.sleep(self.slurm_poll_interval)
        return result, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_item_id: int,
        workdir_local: Path,
        workdir_remote: Path,
        in_compound_task: bool = False,
        **kwargs,
    ):
        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            in_compound_task=in_compound_task,
        )

        # Create folders
        workdir_local.mkdir(parents=True, exist_ok=True)
        _mkdir_as_user(
            folder=workdir_remote.as_posix(),
            user=self.slurm_user,
        )

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        # TODO Pick a data structure for results and exceptions, or review the
        # interface
        results = []
        exceptions = []
        jobs: dict[str, SlurmJob] = {}

        # TODO: Add batching
        for ind, parameters in enumerate(list_parameters):
            # TODO: replace with actual values
            slurm_job = SlurmJob(
                label=str(ind),  # FIXME
                workdir_local=workdir_local,
                workdir_remote=workdir_remote,
                tasks=[
                    SlurmTask(
                        label=str(ind),  # FIXME
                        workdir_local=workdir_local,
                        workdir_remote=workdir_remote,
                        zarr_url=parameters["zarr_url"],
                    )
                ],
            )
            slurm_job_id = self.submit_single_sbatch(
                func,
                parameters=parameters,
                slurm_job=slurm_job,
            )
            slurm_job.slurm_job_id = slurm_job_id
            jobs[slurm_job_id] = slurm_job

        # Retrieval phase
        while len(jobs) > 0:
            # TODO: Check shutdown condition, and act accordingly
            remaining_jobs = list(jobs.keys())
            finished_jobs = get_finished_jobs(job_ids=remaining_jobs)
            for slurm_job_id in finished_jobs:
                slurm_job = jobs.pop(slurm_job_id)
                self.copy_files_from_remote_to_local(slurm_job)
                for task in slurm_job.tasks:
                    result, exception = self.postprocess_single_task(task=task)
                    if not in_compound_task:
                        if exception is None:
                            update_single_image(
                                zarr_url=task.zarr_url,
                                history_item_id=history_item_id,
                                status=HistoryItemImageStatus.DONE,
                            )
                        else:
                            update_single_image(
                                zarr_url=task.zarr_url,
                                history_item_id=history_item_id,
                                status=HistoryItemImageStatus.FAILED,
                            )
                    # TODO: Now just appending, but this should be done better
                    results.append(result)
                    exceptions.append(exception)
            time.sleep(self.slurm_poll_interval)
        return results, exceptions

    def check_remote_python_interpreter(self):
        """
        Check fractal-server version on the _remote_ Python interpreter.
        """
        settings = Inject(get_settings)
        output = _subprocess_run_or_raise(
            (
                f"{settings.FRACTAL_SLURM_WORKER_PYTHON} "
                "-m fractal_server.app.runner.versions"
            )
        )
        runner_version = json.loads(output.stdout.strip("\n"))[
            "fractal_server"
        ]
        if runner_version != __VERSION__:
            error_msg = (
                "Fractal-server version mismatch.\n"
                "Local interpreter: "
                f"({sys.executable}): {__VERSION__}.\n"
                "Remote interpreter: "
                f"({settings.FRACTAL_SLURM_WORKER_PYTHON}): {runner_version}."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
