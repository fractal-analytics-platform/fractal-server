import json
import math
import sys
import time
from copy import copy
from pathlib import Path
from typing import Any
from typing import Optional

import cloudpickle
from pydantic import BaseModel
from pydantic import ConfigDict

from ._check_job_status_ssh import get_finished_jobs_ssh
from fractal_server import __VERSION__
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
from fractal_server.app.schemas.v2.task import TaskTypeType
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
    def slurm_stdout(self) -> str:
        return (self.workdir_remote / f"slurm-{self.label}.out").as_posix()

    @property
    def slurm_stderr(self) -> str:
        return (self.workdir_remote / f"slurm-{self.label}.err").as_posix()

    @property
    def log_files_local(self) -> list[str]:
        return [task.task_files.log_file_local for task in self.tasks]


# def _subprocess_run_or_raise(
#     full_command: str,
# ) -> Optional[subprocess.CompletedProcess]:
#     try:
#         output = subprocess.run(  # nosec
#             shlex.split(full_command),
#             capture_output=True,
#             check=True,
#             encoding="utf-8",
#         )
#         return output
#     except subprocess.CalledProcessError as e:
#         error_msg = (
#             f"Submit command `{full_command}` failed. "
#             f"Original error:\n{str(e)}\n"
#             f"Original stdout:\n{e.stdout}\n"
#             f"Original stderr:\n{e.stderr}\n"
#         )
#         logging.error(error_msg)
#         raise JobExecutionError(info=error_msg)


class RunnerSlurmSSH(BaseRunner):
    fractal_ssh: FractalSSH

    slurm_user: str
    shutdown_file: Path
    common_script_lines: list[str]
    user_cache_dir: str
    root_dir_local: Path
    root_dir_remote: Path
    slurm_account: Optional[str] = None
    poll_interval: int
    python_worker_interpreter: str
    jobs: dict[str, SlurmJob]

    def __init__(
        self,
        *,
        fractal_ssh: FractalSSH,
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
        settings = Inject(get_settings)
        self.fractal_ssh = fractal_ssh
        logger.warning(self.fractal_ssh)

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
        # Prepare commands to be included in SLURM submission script
        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )
        cmdlines = []
        for task in slurm_job.tasks:
            input_pickle_file = task.input_pickle_file_local
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
                f"#SBATCH --err={slurm_job.slurm_stderr}",
                f"#SBATCH --out={slurm_job.slurm_stdout}",
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
        # FIXME: This should only transfer archives, not single files
        """
        Note: this would differ for SSH
        """
        source_target_list = [
            (job.slurm_log_file_remote, job.slurm_log_file_local)
        ]
        for task in job.tasks:
            source_target_list.extend(
                [
                    (
                        task.output_pickle_file_remote,
                        task.output_pickle_file_local,
                    ),
                    (
                        task.task_files.log_file_remote,
                        task.task_files.log_file_local,
                    ),
                    (
                        task.task_files.args_file_remote,
                        task.task_files.args_file_local,
                    ),
                    (
                        task.task_files.metadiff_file_remote,
                        task.task_files.metadiff_file_local,
                    ),
                ]
            )

        for source, target in source_target_list:
            try:
                self.fractal_ssh.fetch_file(local=target, remote=source)
                # res = _run_command_as_user(
                #     cmd=f"cat {source}",
                #     user=self.slurm_user,
                #     encoding=None,
                #     check=True,
                # )
                # Write local file
                # with open(target, "wb") as f:
                #     f.write(res.stdout)
                # logger.critical(f"Copied {source} into {target}")
            except (RuntimeError, FileNotFoundError) as e:
                logger.warning(
                    f"SKIP copy {target} into {source}. "
                    f"Original error: {str(e)}"
                )

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
        history_item_id: int,
        task_files: TaskFiles,
        slurm_config: SlurmConfig,
        task_type: TaskTypeType,
    ) -> tuple[Any, Exception]:
        workdir_local = task_files.wftask_subfolder_local
        workdir_remote = task_files.wftask_subfolder_remote

        task_files = TaskFiles(
            **task_files.model_dump(
                exclude={"component"},
            ),
            # FIXME _COMPONENT_KEY_ is deprecated
            component="FIXME_INVALID_FAKE_VALUE",
            # component=parameters[_COMPONENT_KEY_],
        )

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
                    component="0",
                    parameters=parameters,
                    workdir_remote=workdir_remote,
                    workdir_local=workdir_local,
                    task_files=task_files,
                )
            ],
        )  # TODO: replace with actual values (BASED ON TASKFILES)

        slurm_config.parallel_tasks_per_job = 1
        self._submit_single_sbatch(
            func,
            slurm_job=slurm_job,
            slurm_config=slurm_config,
        )

        # Retrieval phase
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = get_finished_jobs_ssh(
                job_ids=self.job_ids,
                fractal_ssh=self.fractal_ssh,
            )
            for slurm_job_id in finished_job_ids:
                slurm_job = self.jobs.pop(slurm_job_id)
                self._copy_files_from_remote_to_local(slurm_job)
                result, exception = self._postprocess_single_task(
                    task=slurm_job.tasks[0]
                )
            time.sleep(self.slurm_poll_interval)

        return result, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_item_id: int,
        task_files: TaskFiles,
        slurm_config: SlurmConfig,
        task_type: TaskTypeType,
    ):
        # self.scancel_jobs()

        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            task_type=task_type,
        )

        workdir_local = task_files.wftask_subfolder_local
        workdir_remote = task_files.wftask_subfolder_remote

        # Create local&remote task subfolders
        if task_type not in ["compound", "converter_compound"]:
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

        original_task_files = task_files
        tot_tasks = len(list_parameters)

        # Set/validate parameters for task batching
        tasks_per_job, parallel_tasks_per_job = heuristics(
            # Number of parallel components (always known)
            tot_tasks=tot_tasks,
            # Optional WorkflowTask attributes:
            tasks_per_job=slurm_config.tasks_per_job,
            parallel_tasks_per_job=slurm_config.parallel_tasks_per_job,  # noqa
            # Task requirements (multiple possible sources):
            cpus_per_task=slurm_config.cpus_per_task,
            mem_per_task=slurm_config.mem_per_task_MB,
            # Fractal configuration variables (soft/hard limits):
            target_cpus_per_job=slurm_config.target_cpus_per_job,
            target_mem_per_job=slurm_config.target_mem_per_job,
            target_num_jobs=slurm_config.target_num_jobs,
            max_cpus_per_job=slurm_config.max_cpus_per_job,
            max_mem_per_job=slurm_config.max_mem_per_job,
            max_num_jobs=slurm_config.max_num_jobs,
        )
        slurm_config.parallel_tasks_per_job = parallel_tasks_per_job
        slurm_config.tasks_per_job = tasks_per_job

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
            # TODO: replace with actual values
            tasks = []
            for ind_chunk, parameters in enumerate(chunk):
                # FIXME: _COMPONENT_KEY_ is deprecated
                # component = parameters[_COMPONENT_KEY_]
                component = "INVALID_FAKE_VALUE_FIXME"
                tasks.append(
                    SlurmTask(
                        index=(ind_batch * batch_size) + ind_chunk,
                        component=component,
                        workdir_local=workdir_local,
                        workdir_remote=workdir_remote,
                        parameters=parameters,
                        zarr_url=parameters["zarr_url"],
                        task_files=TaskFiles(
                            **original_task_files.model_dump(
                                exclude={"component"}
                            ),
                            component=component,
                        ),
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
                slurm_config=slurm_config,
            )
        logger.info(f"END submission phase, {list(self.jobs.keys())=}")

        # Retrieval phase
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = get_finished_jobs_ssh(
                job_ids=self.job_ids,
                fractal_ssh=self.fractal_ssh,
            )
            for slurm_job_id in finished_job_ids:
                slurm_job = self.jobs.pop(slurm_job_id)
                self._copy_files_from_remote_to_local(slurm_job)
                for task in slurm_job.tasks:
                    result, exception = self._postprocess_single_task(
                        task=task
                    )
                    if exception is None:
                        results[task.index] = result
                    else:
                        exceptions[task.index] = exception
            time.sleep(self.slurm_poll_interval)
        return results, exceptions

    def check_remote_python_interpreter(self):
        settings = Inject(get_settings)
        cmd = (
            f"{self.python_worker_interpreter} "
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
