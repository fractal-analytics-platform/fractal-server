import json
import math
import shlex
import subprocess  # nosec
import sys
import time
from copy import copy
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional

import cloudpickle
from pydantic import BaseModel
from pydantic import ConfigDict

from ._check_job_status_ssh import get_finished_jobs_ssh
from fractal_server import __VERSION__
from fractal_server.app.db import get_sync_db
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.executors.slurm_common._batching import (
    heuristics,
)
from fractal_server.app.runner.executors.slurm_common._slurm_config import (
    SlurmConfig,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
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
    model_config = ConfigDict(arbitrary_types_allowed=True)
    component: str
    workdir_local: Path
    workdir_remote: Path
    parameters: dict[str, Any]
    zarr_url: Optional[str] = None
    task_files: TaskFiles
    index: int

    @property
    def input_pickle_file_local(self) -> str:
        return (
            self.workdir_local / f"{self.component}-input.pickle"
        ).as_posix()

    @property
    def output_pickle_file_local(self) -> str:
        return (
            self.workdir_local / f"{self.component}-output.pickle"
        ).as_posix()

    @property
    def input_pickle_file_remote(self) -> str:
        return (
            self.workdir_remote / f"{self.component}-input.pickle"
        ).as_posix()

    @property
    def output_pickle_file_remote(self) -> str:
        return (
            self.workdir_remote / f"{self.component}-output.pickle"
        ).as_posix()


class SlurmJob(BaseModel):
    slurm_job_id: Optional[str] = None
    label: str
    workdir_local: Path
    workdir_remote: Path
    tasks: list[SlurmTask]

    @property
    def slurm_log_file_local(self) -> str:
        if self.slurm_job_id:
            return (
                self.workdir_local
                / f"slurm-{self.label}-{self.slurm_job_id}.log"
            ).as_posix()
        else:
            return (
                self.workdir_local / f"slurm-{self.label}-%j.log"
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

    @property
    def slurm_stdout_remote(self) -> str:
        if self.slurm_job_id:
            return (
                self.workdir_remote
                / f"slurm-{self.label}-{self.slurm_job_id}.out"
            ).as_posix()

        else:
            return (
                self.workdir_remote / f"slurm-{self.label}-%j.out"
            ).as_posix()

    @property
    def slurm_stderr_remote(self) -> str:
        if self.slurm_job_id:
            return (
                self.workdir_remote
                / f"slurm-{self.label}-{self.slurm_job_id}.err"
            ).as_posix()

        else:
            return (
                self.workdir_remote / f"slurm-{self.label}-%j.err"
            ).as_posix()

    @property
    def slurm_stdout_local(self) -> str:
        if self.slurm_job_id:
            return (
                self.workdir_local
                / f"slurm-{self.label}-{self.slurm_job_id}.out"
            ).as_posix()

        else:
            return (
                self.workdir_local / f"slurm-{self.label}-%j.out"
            ).as_posix()

    @property
    def slurm_stderr_local(self) -> str:
        if self.slurm_job_id:
            return (
                self.workdir_local
                / f"slurm-{self.label}-{self.slurm_job_id}.err"
            ).as_posix()

        else:
            return (
                self.workdir_local / f"slurm-{self.label}-%j.err"
            ).as_posix()

    @property
    def log_files_local(self) -> list[str]:
        return [task.task_files.log_file_local for task in self.tasks]


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
        raise JobExecutionError(info=error_msg)


class RunnerSlurmSSH(BaseRunner):
    fractal_ssh: FractalSSH

    shutdown_file: Path
    common_script_lines: list[str]
    user_cache_dir: str
    root_dir_local: Path
    root_dir_remote: Path
    poll_interval: int
    python_worker_interpreter: str
    jobs: dict[str, SlurmJob]

    def __init__(
        self,
        *,
        fractal_ssh: FractalSSH,
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: Optional[list[str]] = None,
        user_cache_dir: Optional[str] = None,
        slurm_poll_interval: Optional[int] = None,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """

        self.common_script_lines = common_script_lines or []

        # Check that SLURM account is not set here
        # FIXME: move to little method
        try:
            invalid_line = next(
                line
                for line in self.common_script_lines
                if line.startswith("#SBATCH --account=")
            )
            raise RuntimeError(
                "Invalid line in `RunnerSlurmSSH.common_script_lines`: "
                f"'{invalid_line}'.\n"
                "SLURM account must be set via the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
        except StopIteration:
            pass

        # Check Python versions
        self.fractal_ssh = fractal_ssh
        logger.warning(self.fractal_ssh)

        settings = Inject(get_settings)
        # It is the new handshanke
        if settings.FRACTAL_SLURM_WORKER_PYTHON is not None:
            self.check_remote_python_interpreter()

        # Initialize connection and perform handshake
        self.root_dir_local = root_dir_local
        self.root_dir_remote = root_dir_remote

        # # Create folders
        # original_umask = os.umask(0)
        # self.root_dir_local.mkdir(parents=True, exist_ok=True, mode=0o755)
        # os.umask(original_umask)
        # _mkdir_as_user(
        #     folder=self.root_dir_remote.as_posix(),
        #     user=self.slurm_user,
        # )

        self.user_cache_dir = user_cache_dir

        self.slurm_poll_interval = (
            slurm_poll_interval or settings.FRACTAL_SLURM_POLL_INTERVAL
        )

        self.shutdown_file = self.root_dir_local / SHUTDOWN_FILENAME

        self.python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        self.jobs = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def is_shutdown(self) -> bool:
        return self.shutdown_file.exists()

    def scancel_jobs(self) -> None:
        logger.debug("[scancel_jobs] START")

        if self.jobs:
            scancel_string = " ".join(self.job_ids)
            scancel_cmd = f"scancel {scancel_string}"
            logger.warning(f"Now scancel-ing SLURM jobs {scancel_string}")
            try:
                self.fractal_ssh.run_command(cmd=scancel_cmd)
                # _run_command_as_user(
                #     cmd=scancel_cmd,
                #     user=self.slurm_user,
                #     check=True,
                # )
            except RuntimeError as e:
                logger.warning(
                    "[scancel_jobs] `scancel` command failed. "
                    f"Original error:\n{str(e)}"
                )

        logger.debug("[scancel_jobs] END")

    def _submit_single_sbatch(
        self,
        func,
        slurm_job: SlurmJob,
        slurm_config: SlurmConfig,
    ) -> str:
        # Prepare input pickle(s)
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        for task in slurm_job.tasks:
            _args = []
            _kwargs = dict(parameters=task.parameters)
            funcser = cloudpickle.dumps((versions, func, _args, _kwargs))

            with open(task.input_pickle_file_local, "wb") as f:
                f.write(funcser)

            logger.debug(
                "[_submit_single_sbatch] Written "
                f"{task.input_pickle_file_local=}"
            )
        # Send input pickle
        self.fractal_ssh.send_file(
            local=task.input_pickle_file_local,
            remote=task.input_pickle_file_remote,
        )
        # Prepare commands to be included in SLURM submission script
        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )
        cmdlines = []
        for task in slurm_job.tasks:
            input_pickle_file = task.input_pickle_file_remote
            output_pickle_file = task.output_pickle_file_remote
            cmdlines.append(
                (
                    f"{python_worker_interpreter}"
                    " -m fractal_server.app.runner."
                    "executors.slurm_common.remote "
                    f"--input-file {input_pickle_file} "
                    f"--output-file {output_pickle_file}"
                )
            )

        # ...
        num_tasks_max_running = slurm_config.parallel_tasks_per_job
        mem_per_task_MB = slurm_config.mem_per_task_MB

        # Set ntasks
        ntasks = min(len(cmdlines), num_tasks_max_running)
        slurm_config.parallel_tasks_per_job = ntasks

        # Prepare SLURM preamble based on SlurmConfig object
        script_lines = slurm_config.to_sbatch_preamble(
            remote_export_dir=self.user_cache_dir
        )

        # Extend SLURM preamble with variable which are not in SlurmConfig, and
        # fix their order
        script_lines.extend(
            [
                f"#SBATCH --err={slurm_job.slurm_stderr_remote}",
                f"#SBATCH --out={slurm_job.slurm_stdout_remote}",
                f"#SBATCH -D {slurm_job.workdir_remote}",
            ]
        )
        script_lines = slurm_config.sort_script_lines(script_lines)
        logger.debug(script_lines)

        # Always print output of `uname -n` and `pwd`
        script_lines.append(
            '"Hostname: `uname -n`; current directory: `pwd`"\n'
        )

        # Complete script preamble
        script_lines.append("\n")

        # Include command lines
        tmp_list_commands = copy(cmdlines)
        while tmp_list_commands:
            if tmp_list_commands:
                cmd = tmp_list_commands.pop(0)  # take first element
                script_lines.append(
                    "srun --ntasks=1 --cpus-per-task=$SLURM_CPUS_PER_TASK "
                    f"--mem={mem_per_task_MB}MB "
                    f"{cmd} &"
                )
        script_lines.append("wait\n")

        script = "\n".join(script_lines)

        # Write submission script
        # submission_script_contents = "\n".join(preamble_lines + cmdlines)
        with open(slurm_job.slurm_submission_script_local, "w") as f:
            f.write(script)

        self.fractal_ssh.send_file(
            local=slurm_job.slurm_submission_script_local,
            remote=slurm_job.slurm_submission_script_remote,
        )

        # Run sbatch
        submit_command = (
            f"sbatch --parsable {slurm_job.slurm_submission_script_remote}"
        )
        pre_submission_cmds = slurm_config.pre_submission_commands
        if len(pre_submission_cmds) == 0:
            sbatch_stdout = self.fractal_ssh.run_command(cmd=submit_command)
        else:
            logger.debug(f"Now using {pre_submission_cmds=}")
            script_lines = pre_submission_cmds + [submit_command]
            script_content = "\n".join(script_lines)
            script_content = f"{script_content}\n"
            script_path_remote = (
                f"{slurm_job.slurm_script_remote.as_posix()}_wrapper.sh"
            )
            self.fractal_ssh.write_remote_file(
                path=script_path_remote, content=script_content
            )
            cmd = f"bash {script_path_remote}"
            sbatch_stdout = self.fractal_ssh.run_command(cmd=cmd)

        # Submit SLURM job and retrieve job ID
        stdout = sbatch_stdout.strip("\n")
        submitted_job_id = int(stdout)
        slurm_job.slurm_job_id = str(submitted_job_id)

        # Add job to self.jobs
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.debug(f"Added {slurm_job.slurm_job_id} to self.jobs.")

    @property
    def job_ids(self) -> list[str]:
        return list(self.jobs.keys())

    def _copy_files_from_remote_to_local(self, job: SlurmJob) -> None:
        remote_tar_file = job.workdir_remote / f"{job.label}.tar.gz"
        local_tar_file = job.workdir_local / f"{job.label}.tar.gz"
        source_files = [
            job.slurm_stdout_remote,
            job.slurm_stderr_remote,
        ]

        for task in job.tasks:
            source_files.extend(
                [
                    task.output_pickle_file_remote,
                    task.task_files.log_file_remote,
                    task.task_files.args_file_remote,
                    task.task_files.metadiff_file_remote,
                ]
            )
        source_files_str = " ".join(source_files)
        tar_command = (
            f"tar -czf {remote_tar_file} "
            f"--ignore-failed-read {source_files_str}"
        )
        untar_command = f"tar -xzf {local_tar_file} -C {job.workdir_local}"

        # Now create the tar file
        logger.info(f"START creating the {remote_tar_file}")
        try:
            self.fractal_ssh.run_command(cmd=tar_command)
        except (RuntimeError, FileNotFoundError) as e:
            logger.warning(
                f"SKIP creating {remote_tar_file}. "
                f"Original error: {str(e)}"
            )
        logger.info(f"END creating the {remote_tar_file}")

        # Now copy the tar file
        logger.info(f"START coping the {remote_tar_file}")
        try:
            self.fractal_ssh.fetch_file(
                local=local_tar_file.as_posix(),
                remote=remote_tar_file.as_posix(),
            )
        except (RuntimeError, FileNotFoundError) as e:
            logger.warning(
                f"SKIP coping {remote_tar_file} into {local_tar_file}. "
                f"Original error: {str(e)}"
            )
        logger.info(f"END coping the {remote_tar_file}")

        # Now untar the tar file
        logger.info(f"START untar the {local_tar_file}")
        try:
            _subprocess_run_or_raise(full_command=untar_command)
        except (RuntimeError, FileNotFoundError) as e:
            logger.warning(
                f"SKIP untar {local_tar_file}. " f"Original error: {str(e)}"
            )
        logger.info(f"END untar the {local_tar_file}")

    def _postprocess_single_task(
        self, *, task: SlurmTask
    ) -> tuple[Any, Exception]:
        try:
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
            exception = JobExecutionError(f"ERROR, {str(e)}")
            return None, exception
        finally:
            Path(task.input_pickle_file_local).unlink(missing_ok=True)
            Path(task.output_pickle_file_local).unlink(missing_ok=True)

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_unit_id: int,
        task_files: TaskFiles,
        config: SlurmConfig,
        task_type: Literal[
            "non_parallel",
            "converter_non_parallel",
            "compound",
            "converter_compound",
        ],
    ) -> tuple[Any, Exception]:
        workdir_local = task_files.wftask_subfolder_local
        workdir_remote = task_files.wftask_subfolder_remote

        if self.jobs != {}:
            raise JobExecutionError("Unexpected branch: jobs should be empty.")

        if self.is_shutdown():
            raise JobExecutionError("Cannot continue after shutdown.")

        # Validation phase
        self.validate_submit_parameters(
            parameters=parameters,
            task_type=task_type,
        )

        # Create task subfolder
        workdir_local.mkdir(parents=True)
        self.fractal_ssh.mkdir(
            folder=workdir_remote.as_posix(),
            parents=True,
        )

        # Submission phase
        slurm_job = SlurmJob(
            label="0",
            workdir_local=workdir_local,
            workdir_remote=workdir_remote,
            tasks=[
                SlurmTask(
                    index=0,
                    component=task_files.component,
                    parameters=parameters,
                    workdir_remote=workdir_remote,
                    workdir_local=workdir_local,
                    task_files=task_files,
                )
            ],
        )

        config.parallel_tasks_per_job = 1
        self._submit_single_sbatch(
            func,
            slurm_job=slurm_job,
            slurm_config=config,
        )
        logger.info(f"END submission phase, {self.job_ids=}")

        # TODO: check if this sleep is necessary
        logger.warning("Now sleep 4 (FIXME)")
        time.sleep(4)

        # Retrieval phase
        logger.info("START retrieval phase")
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = get_finished_jobs_ssh(
                job_ids=self.job_ids,
                fractal_ssh=self.fractal_ssh,
            )
            logger.debug(f"{finished_job_ids=}")
            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    logger.debug(f"Now process {slurm_job_id=}")
                    slurm_job = self.jobs.pop(slurm_job_id)
                    self._copy_files_from_remote_to_local(slurm_job)
                    result, exception = self._postprocess_single_task(
                        task=slurm_job.tasks[0]
                    )
                    if exception is not None:
                        update_status_of_history_unit(
                            history_unit_id=history_unit_id,
                            status=HistoryUnitStatus.FAILED,
                            db_sync=db,
                        )
                    else:
                        if task_type not in ["compound", "converter_compound"]:
                            update_status_of_history_unit(
                                history_unit_id=history_unit_id,
                                status=HistoryUnitStatus.DONE,
                                db_sync=db,
                            )

            time.sleep(self.slurm_poll_interval)

        return result, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: Literal["parallel", "compound", "converter_compound"],
        config: SlurmConfig,
    ):
        if len(self.jobs) > 0:
            raise RuntimeError(
                f"Cannot run .multisubmit when {len(self.jobs)=}"
            )

        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            task_type=task_type,
            list_task_files=list_task_files,
        )

        self.validate_multisubmit_history_unit_ids(
            history_unit_ids=history_unit_ids,
            task_type=task_type,
            list_parameters=list_parameters,
        )

        workdir_local = list_task_files[0].wftask_subfolder_local
        workdir_remote = list_task_files[0].wftask_subfolder_remote

        # Create local&remote task subfolders
        if task_type == "parallel":
            workdir_local.mkdir(parents=True)
            self.fractal_ssh.mkdir(
                folder=workdir_remote.as_posix(),
                parents=True,
            )

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        # TODO Pick a data structure for results and exceptions, or review the
        # interface
        results: dict[int, Any] = {}
        exceptions: dict[int, BaseException] = {}

        original_task_files = list_task_files
        tot_tasks = len(list_parameters)

        # Set/validate parameters for task batching
        tasks_per_job, parallel_tasks_per_job = heuristics(
            # Number of parallel components (always known)
            tot_tasks=tot_tasks,
            # Optional WorkflowTask attributes:
            tasks_per_job=config.tasks_per_job,
            parallel_tasks_per_job=config.parallel_tasks_per_job,  # noqa
            # Task requirements (multiple possible sources):
            cpus_per_task=config.cpus_per_task,
            mem_per_task=config.mem_per_task_MB,
            # Fractal configuration variables (soft/hard limits):
            target_cpus_per_job=config.target_cpus_per_job,
            target_mem_per_job=config.target_mem_per_job,
            target_num_jobs=config.target_num_jobs,
            max_cpus_per_job=config.max_cpus_per_job,
            max_mem_per_job=config.max_mem_per_job,
            max_num_jobs=config.max_num_jobs,
        )
        config.parallel_tasks_per_job = parallel_tasks_per_job
        config.tasks_per_job = tasks_per_job

        # Divide arguments in batches of `tasks_per_job` tasks each
        args_batches = []
        batch_size = tasks_per_job
        for ind_chunk in range(0, tot_tasks, batch_size):
            args_batches.append(
                list_parameters[ind_chunk : ind_chunk + batch_size]  # noqa
            )
        if len(args_batches) != math.ceil(tot_tasks / tasks_per_job):
            raise RuntimeError("Something wrong here while batching tasks")

        logger.info(f"START submission phase, {list(self.jobs.keys())=}")
        for ind_batch, chunk in enumerate(args_batches):
            tasks = []
            for ind_chunk, parameters in enumerate(chunk):
                index = (ind_batch * batch_size) + ind_chunk
                tasks.append(
                    SlurmTask(
                        index=index,
                        component=original_task_files[index].component,
                        workdir_local=workdir_local,
                        workdir_remote=workdir_remote,
                        parameters=parameters,
                        zarr_url=parameters["zarr_url"],
                        task_files=original_task_files[index],
                    ),
                )

            slurm_job = SlurmJob(
                label=f"{ind_batch:06d}",
                workdir_local=workdir_local,
                workdir_remote=workdir_remote,
                tasks=tasks,
            )
            self._submit_single_sbatch(
                func,
                slurm_job=slurm_job,
                slurm_config=config,
            )
        logger.info(f"END submission phase, {list(self.jobs.keys())=}")

        # TODO useful?
        # logger.warning("Now sleep 4 (FIXME)")
        # time.sleep(4)

        # Retrieval phase
        logger.info("START retrieval phase")
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = get_finished_jobs_ssh(
                job_ids=self.job_ids,
                fractal_ssh=self.fractal_ssh,
            )
            logger.debug(f"{finished_job_ids=}")
            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    slurm_job = self.jobs.pop(slurm_job_id)
                    self._copy_files_from_remote_to_local(slurm_job)
                    for task in slurm_job.tasks:
                        logger.debug(f"Now processing {task.index=}")
                        result, exception = self._postprocess_single_task(
                            task=task
                        )
                        if exception is not None:
                            logger.debug(
                                f"Task {task.index} has an exception."
                            )  # FIXME  # noqa
                            exceptions[task.index] = exception
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.FAILED,
                                    db_sync=db,
                                )
                        else:
                            logger.debug(
                                f"Task {task.index} has no exception."
                            )  # FIXME  # noqa
                            results[task.index] = result
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.DONE,
                                    db_sync=db,
                                )

            time.sleep(self.slurm_poll_interval)
        return results, exceptions

    def check_remote_python_interpreter(self):
        settings = Inject(get_settings)
        cmd = (
            f"{settings.FRACTAL_SLURM_WORKER_PYTHON} "
            "-m fractal_server.app.runner.versions"
        )
        stdout = self.fractal_ssh.run_command(cmd=cmd)
        remote_version = json.loads(stdout.strip("\n"))["fractal_server"]
        if remote_version != __VERSION__:
            error_msg = (
                "Fractal-server version mismatch.\n"
                "Local interpreter: "
                f"({sys.executable}): {__VERSION__}.\n"
                "Remote interpreter: "
                f"({settings.FRACTAL_SLURM_WORKER_PYTHON}): {remote_version}."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
