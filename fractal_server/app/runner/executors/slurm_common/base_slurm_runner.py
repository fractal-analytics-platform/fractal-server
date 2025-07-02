import json
import math
import sys
import time
from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict

from ..slurm_common._slurm_config import SlurmConfig
from ..slurm_common.slurm_job_task_models import SlurmJob
from ..slurm_common.slurm_job_task_models import SlurmTask
from ._job_states import STATES_FINISHED
from fractal_server import __VERSION__
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import AccountingRecordSlurm
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.executors.base_runner import MultisubmitTaskType
from fractal_server.app.runner.executors.base_runner import SubmitTaskType
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import (
    bulk_update_status_of_history_unit,
)
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import TaskType
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

SHUTDOWN_ERROR_MESSAGE = "Failed due to job-execution shutdown."
SHUTDOWN_EXCEPTION = JobExecutionError(SHUTDOWN_ERROR_MESSAGE)

logger = set_logger(__name__)


class RemoteInputData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    python_version: tuple[int, int, int]
    fractal_server_version: str
    full_command: str

    metadiff_file_remote: str
    log_file_remote: str


def create_accounting_record_slurm(
    *,
    user_id: int,
    slurm_job_ids: list[int],
) -> None:
    with next(get_sync_db()) as db:
        db.add(
            AccountingRecordSlurm(
                user_id=user_id,
                slurm_job_ids=slurm_job_ids,
            )
        )
        db.commit()


class BaseSlurmRunner(BaseRunner):
    shutdown_file: Path
    common_script_lines: list[str]
    user_cache_dir: str
    root_dir_local: Path
    root_dir_remote: Path
    poll_interval: int
    poll_interval_internal: float
    jobs: dict[str, SlurmJob]
    python_worker_interpreter: str
    slurm_runner_type: Literal["ssh", "sudo"]
    slurm_account: str | None = None

    def __init__(
        self,
        root_dir_local: Path,
        root_dir_remote: Path,
        slurm_runner_type: Literal["ssh", "sudo"],
        python_worker_interpreter: str,
        common_script_lines: list[str] | None = None,
        user_cache_dir: str | None = None,
        poll_interval: int | None = None,
        slurm_account: str | None = None,
    ):
        self.slurm_runner_type = slurm_runner_type
        self.root_dir_local = root_dir_local
        self.root_dir_remote = root_dir_remote
        self.common_script_lines = common_script_lines or []
        self._check_slurm_account()
        self.user_cache_dir = user_cache_dir
        self.python_worker_interpreter = python_worker_interpreter
        self.slurm_account = slurm_account

        settings = Inject(get_settings)

        self.poll_interval = (
            poll_interval or settings.FRACTAL_SLURM_POLL_INTERVAL
        )
        self.poll_interval_internal = self.poll_interval / 10.0

        self.check_fractal_server_versions()

        # Create job folders. Note that the local one may or may not exist
        # depending on whether it is a test or an actual run
        try:
            if not self.root_dir_local.is_dir():
                self._mkdir_local_folder(self.root_dir_local.as_posix())
            self._mkdir_remote_folder(self.root_dir_remote.as_posix())
        except Exception as e:
            error_msg = (
                f"Could not mkdir {self.root_dir_local.as_posix()} or "
                f"{self.root_dir_remote.as_posix()}. "
                f"Original error: {str(e)}."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.shutdown_file = self.root_dir_local / SHUTDOWN_FILENAME
        self.jobs = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def _run_remote_cmd(self, cmd: str) -> str:
        raise NotImplementedError("Implement in child class.")

    def run_squeue(self, *, job_ids: list[str], **kwargs) -> str:
        raise NotImplementedError("Implement in child class.")

    def _is_squeue_error_recoverable(self, exception: BaseException) -> True:
        """
        Determine whether a `squeue` error is considered recoverable.

        A _recoverable_ error is one which will disappear after some time,
        without any specific action from the `fractal-server` side.

        Note: if this function returns `True` for an error that does not
        actually recover, this leads to an infinite loop  where
        `fractal-server` keeps polling `squeue` information forever.

        More info at
        https://github.com/fractal-analytics-platform/fractal-server/issues/2682

        Args:
            exception: The exception raised by `self.run_squeue`.
        Returns:
            Whether the error is considered recoverable.
        """
        str_exception = str(exception)
        if (
            "slurm_load_jobs" in str_exception
            and "Socket timed out on send/recv operation" in str_exception
        ):
            return True
        else:
            return False

    def _get_finished_jobs(self, job_ids: list[str]) -> set[str]:
        #  If there is no Slurm job to check, return right away
        if not job_ids:
            return set()

        try:
            stdout = self.run_squeue(job_ids=job_ids)
            slurm_statuses = {
                out.split()[0]: out.split()[1] for out in stdout.splitlines()
            }
        except Exception as e:
            logger.warning(
                "[_get_finished_jobs] `squeue` failed, "
                "retry with individual job IDs. "
                f"Original error: {str(e)}."
            )
            slurm_statuses = dict()
            for job_id in job_ids:
                try:
                    stdout = self.run_squeue(job_ids=[job_id])
                    slurm_statuses.update(
                        {stdout.split()[0]: stdout.split()[1]}
                    )
                except Exception as e:
                    msg = (
                        f"[_get_finished_jobs] `squeue` failed for {job_id=}. "
                        f"Original error: {str(e)}."
                    )
                    logger.warning(msg)
                    if self._is_squeue_error_recoverable(e):
                        logger.warning(
                            "[_get_finished_jobs] Recoverable `squeue` "
                            f"error - mark {job_id=} as FRACTAL_UNDEFINED and"
                            " retry later."
                        )
                        slurm_statuses.update(
                            {str(job_id): "FRACTAL_UNDEFINED"}
                        )
                    else:
                        logger.warning(
                            "[_get_finished_jobs] Non-recoverable `squeue`"
                            f"error - mark {job_id=} as completed."
                        )
                        slurm_statuses.update({str(job_id): "COMPLETED"})

        # If a job is not in `squeue` output, mark it as completed.
        finished_jobs = {
            job_id
            for job_id in job_ids
            if slurm_statuses.get(job_id, "COMPLETED") in STATES_FINISHED
        }
        return finished_jobs

    def _mkdir_local_folder(self, folder: str) -> None:
        raise NotImplementedError("Implement in child class.")

    def _mkdir_remote_folder(self, folder: str) -> None:
        raise NotImplementedError("Implement in child class.")

    def _enrich_slurm_config(
        self,
        slurm_config: SlurmConfig,
    ) -> SlurmConfig:
        """
        Return an enriched `SlurmConfig` object

        Include `self.account` and `self.common_script_lines` into a
        `SlurmConfig` object. Extracting this logic into an independent
        class method is useful to fix issue #2659 (which was due to
        performing this same operation multiple times rather than once).

        Args:
            slurm_config: The original `SlurmConfig` object.

        Returns:
            A new, up-to-date, `SlurmConfig` object.
        """

        new_slurm_config = slurm_config.model_copy()

        # Include SLURM account in `slurm_config`.
        if self.slurm_account is not None:
            new_slurm_config.account = self.slurm_account

        # Include common_script_lines in extra_lines
        if len(self.common_script_lines) > 0:
            logger.debug(
                f"Add {self.common_script_lines} to "
                f"{new_slurm_config.extra_lines=}."
            )
            current_extra_lines = new_slurm_config.extra_lines or []
            new_slurm_config.extra_lines = (
                current_extra_lines + self.common_script_lines
            )

        return new_slurm_config

    def _submit_single_sbatch(
        self,
        *,
        base_command: str,
        slurm_job: SlurmJob,
        slurm_config: SlurmConfig,
    ) -> str:
        logger.debug("[_submit_single_sbatch] START")

        for task in slurm_job.tasks:
            # Write input file
            if self.slurm_runner_type == "ssh":
                args_file_remote = task.task_files.args_file_remote
            else:
                args_file_remote = task.task_files.args_file_local
            metadiff_file_remote = task.task_files.metadiff_file_remote
            full_command = (
                f"{base_command} "
                f"--args-json {args_file_remote} "
                f"--out-json {metadiff_file_remote}"
            )

            input_data = RemoteInputData(
                full_command=full_command,
                python_version=sys.version_info[:3],
                fractal_server_version=__VERSION__,
                metadiff_file_remote=task.task_files.metadiff_file_remote,
                log_file_remote=task.task_files.log_file_remote,
            )

            with open(task.input_file_local, "w") as f:
                json.dump(input_data.model_dump(), f, indent=2)

            with open(task.task_files.args_file_local, "w") as f:
                json.dump(task.parameters, f, indent=2)

            logger.debug(
                "[_submit_single_sbatch] Written " f"{task.input_file_local=}"
            )

            if self.slurm_runner_type == "ssh":
                # Send input file (only relevant for SSH)
                self.fractal_ssh.send_file(
                    local=task.input_file_local,
                    remote=task.input_file_remote,
                )
                self.fractal_ssh.send_file(
                    local=task.task_files.args_file_local,
                    remote=task.task_files.args_file_remote,
                )
                logger.debug(
                    "[_submit_single_sbatch] Transferred "
                    f"{task.input_file_local=}"
                )

        # Prepare commands to be included in SLURM submission script
        cmdlines = []
        for task in slurm_job.tasks:
            if self.slurm_runner_type == "ssh":
                input_file = task.input_file_remote
            else:
                input_file = task.input_file_local
            output_file = task.output_file_remote
            cmdlines.append(
                f"{self.python_worker_interpreter}"
                " -m fractal_server.app.runner."
                "executors.slurm_common.remote "
                f"--input-file {input_file} "
                f"--output-file {output_file}"
            )

        # Set ntasks
        num_tasks_max_running = slurm_config.parallel_tasks_per_job
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
        script_lines.append('\necho "Hostname: $(uname -n)"')
        script_lines.append('echo "Current directory: $(pwd)"')
        script_lines.append(
            'echo "Start time: $(date +"%Y-%m-%dT%H:%M:%S%z")"'
        )

        # Complete script preamble
        script_lines.append("\n")

        # Include command lines
        mem_per_task_MB = slurm_config.mem_per_task_MB
        for cmd in cmdlines:
            script_lines.append(
                "srun --ntasks=1 --cpus-per-task=$SLURM_CPUS_PER_TASK "
                f"--mem={mem_per_task_MB}MB "
                f"{cmd} &"
            )
        script_lines.append("wait\n")
        script = "\n".join(script_lines)
        script_lines.append(
            'echo "End time:   $(date +"%Y-%m-%dT%H:%M:%S%z")"'
        )

        # Write submission script
        with open(slurm_job.slurm_submission_script_local, "w") as f:
            f.write(script)
        logger.debug(
            "[_submit_single_sbatch] Written "
            f"{slurm_job.slurm_submission_script_local=}"
        )

        if self.slurm_runner_type == "ssh":
            self.fractal_ssh.send_file(
                local=slurm_job.slurm_submission_script_local,
                remote=slurm_job.slurm_submission_script_remote,
            )
            submit_command = (
                "sbatch --parsable "
                f"{slurm_job.slurm_submission_script_remote}"
            )
        else:
            submit_command = (
                "sbatch --parsable "
                f"{slurm_job.slurm_submission_script_local}"
            )
        # Run sbatch
        pre_submission_cmds = slurm_config.pre_submission_commands
        if len(pre_submission_cmds) == 0:
            logger.debug(f"Now run {submit_command=}")
            sbatch_stdout = self._run_remote_cmd(submit_command)
        else:
            logger.debug(f"Now using {pre_submission_cmds=}")
            script_lines = pre_submission_cmds + [submit_command]
            wrapper_script_contents = "\n".join(script_lines)
            wrapper_script_contents = f"{wrapper_script_contents}\n"
            if self.slurm_runner_type == "ssh":
                wrapper_script = (
                    f"{slurm_job.slurm_submission_script_remote}_wrapper.sh"
                )
                self.fractal_ssh.write_remote_file(
                    path=wrapper_script, content=wrapper_script_contents
                )
            else:
                wrapper_script = (
                    f"{slurm_job.slurm_submission_script_local}_wrapper.sh"
                )
                with open(wrapper_script, "w") as f:
                    f.write(wrapper_script_contents)
            logger.debug(f"Now run {wrapper_script=}")
            sbatch_stdout = self._run_remote_cmd(f"bash {wrapper_script}")

        # Submit SLURM job and retrieve job ID
        logger.info(f"[_submit_single_sbatch] {sbatch_stdout=}")
        stdout = sbatch_stdout.strip("\n")
        submitted_job_id = int(stdout)
        slurm_job.slurm_job_id = str(submitted_job_id)

        # Add job to self.jobs
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.debug(
            "[_submit_single_sbatch] Added "
            f"{slurm_job.slurm_job_id} to self.jobs."
        )
        logger.debug("[_submit_single_sbatch] END")

    def _fetch_artifacts(
        self,
        finished_slurm_jobs: list[SlurmJob],
    ) -> None:
        raise NotImplementedError("Implement in child class.")

    def _check_slurm_account(self) -> None:
        """
        Check that SLURM account is not set here in `common_script_lines`.
        """
        try:
            invalid_line = next(
                line
                for line in self.common_script_lines
                if line.startswith("#SBATCH --account=")
            )
            raise RuntimeError(
                "Invalid line in `common_script_lines`: "
                f"'{invalid_line}'.\n"
                "SLURM account must be set via the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
        except StopIteration:
            pass

    def _postprocess_single_task(
        self,
        *,
        task: SlurmTask,
        was_job_scancelled: bool = False,
    ) -> tuple[Any, Exception]:
        try:
            with open(task.output_file_local) as f:
                output = json.load(f)
            success = output[0]
            if success:
                # Task succeeded
                result = output[1]
                return (result, None)
            else:
                # Task failed in a controlled way, and produced an `output`
                # object which is a dictionary with required keys
                # `exc_type_name` and `traceback_string` and with optional
                # keys `workflow_task_order`, `workflow_task_id` and
                # `task_name`.
                exc_proxy = output[1]
                exc_type_name = exc_proxy.get("exc_type_name")
                logger.debug(
                    f"Output file contains a '{exc_type_name}' exception."
                )
                traceback_string = output[1].get("traceback_string")
                exception = TaskExecutionError(
                    traceback_string,
                    workflow_task_id=task.workflow_task_id,
                    workflow_task_order=task.workflow_task_order,
                    task_name=task.task_name,
                )
                return (None, exception)

        except Exception as e:
            exception = JobExecutionError(f"ERROR, {str(e)}")
            # If job was scancelled and task failed, replace
            # exception with a shutdown-related one.
            if was_job_scancelled:
                logger.debug(
                    "Replacing exception with a shutdown-related one, "
                    f"for {task.index=}."
                )
                exception = SHUTDOWN_EXCEPTION
            return (None, exception)
        finally:
            Path(task.input_file_local).unlink(missing_ok=True)
            Path(task.output_file_local).unlink(missing_ok=True)

    def is_shutdown(self) -> bool:
        return self.shutdown_file.exists()

    @property
    def job_ids(self) -> list[str]:
        return list(self.jobs.keys())

    def wait_and_check_shutdown(self) -> list[str]:
        """
        Wait at most `self.poll_interval`, while also checking for shutdown.
        """
        # Sleep for `self.poll_interval`, but keep checking for shutdowns
        start_time = time.perf_counter()
        # Always wait at least 0.2 (note: this is for cases where
        # `poll_interval=0`).
        waiting_time = max(self.poll_interval, 0.2)
        max_time = start_time + waiting_time
        logger.debug(
            "[wait_and_check_shutdown] "
            f"I will wait at most {self.poll_interval} s, "
            f"in blocks of {self.poll_interval_internal} s."
        )

        while time.perf_counter() < max_time:
            if self.is_shutdown():
                logger.info("[wait_and_check_shutdown] Shutdown file detected")
                scancelled_job_ids = self.scancel_jobs()
                logger.info(f"[wait_and_check_shutdown] {scancelled_job_ids=}")
                return scancelled_job_ids
            time.sleep(self.poll_interval_internal)

        logger.debug("[wait_and_check_shutdown] No shutdown file detected")
        return []

    def _check_no_active_jobs(self):
        if self.jobs != {}:
            raise JobExecutionError(
                "Unexpected branch: jobs must be empty before new "
                "submissions."
            )

    def submit(
        self,
        base_command: str,
        workflow_task_order: int,
        workflow_task_id: int,
        task_name: str,
        parameters: dict[str, Any],
        history_unit_id: int,
        task_files: TaskFiles,
        config: SlurmConfig,
        task_type: SubmitTaskType,
        user_id: int,
    ) -> tuple[Any, Exception]:
        logger.debug("[submit] START")

        config = self._enrich_slurm_config(config)

        try:
            workdir_local = task_files.wftask_subfolder_local
            workdir_remote = task_files.wftask_subfolder_remote

            if self.is_shutdown():
                with next(get_sync_db()) as db:
                    update_status_of_history_unit(
                        history_unit_id=history_unit_id,
                        status=HistoryUnitStatus.FAILED,
                        db_sync=db,
                    )

                return None, SHUTDOWN_EXCEPTION

            self._check_no_active_jobs()

            # Validation phase
            self.validate_submit_parameters(
                parameters=parameters,
                task_type=task_type,
            )

            # Create task subfolder
            logger.debug("[submit] Create local/remote folders - START")
            self._mkdir_local_folder(folder=workdir_local.as_posix())
            self._mkdir_remote_folder(folder=workdir_remote.as_posix())
            logger.debug("[submit] Create local/remote folders - END")

            # Submission phase
            slurm_job = SlurmJob(
                prefix=task_files.prefix,
                workdir_local=workdir_local,
                workdir_remote=workdir_remote,
                tasks=[
                    SlurmTask(
                        prefix=task_files.prefix,
                        index=0,
                        component=task_files.component,
                        parameters=parameters,
                        workdir_remote=workdir_remote,
                        workdir_local=workdir_local,
                        task_files=task_files,
                        workflow_task_order=workflow_task_order,
                        workflow_task_id=workflow_task_id,
                        task_name=task_name,
                    )
                ],
            )

            config.parallel_tasks_per_job = 1
            self._submit_single_sbatch(
                base_command=base_command,
                slurm_job=slurm_job,
                slurm_config=config,
            )
            logger.debug(f"[submit] END submission phase, {self.job_ids=}")

            create_accounting_record_slurm(
                user_id=user_id,
                slurm_job_ids=self.job_ids,
            )

            # Retrieval phase
            logger.debug("[submit] START retrieval phase")
            scancelled_job_ids = []
            while len(self.jobs) > 0:
                # Look for finished jobs
                finished_job_ids = self._get_finished_jobs(
                    job_ids=self.job_ids
                )
                logger.debug(f"[submit] {finished_job_ids=}")
                finished_jobs = [
                    self.jobs[_slurm_job_id]
                    for _slurm_job_id in finished_job_ids
                ]
                self._fetch_artifacts(finished_jobs)
                with next(get_sync_db()) as db:
                    for slurm_job_id in finished_job_ids:
                        logger.debug(f"[submit] Now process {slurm_job_id=}")
                        slurm_job = self.jobs.pop(slurm_job_id)
                        was_job_scancelled = slurm_job_id in scancelled_job_ids
                        result, exception = self._postprocess_single_task(
                            task=slurm_job.tasks[0],
                            was_job_scancelled=was_job_scancelled,
                        )

                        if exception is not None:
                            update_status_of_history_unit(
                                history_unit_id=history_unit_id,
                                status=HistoryUnitStatus.FAILED,
                                db_sync=db,
                            )
                        else:
                            if task_type not in [
                                TaskType.COMPOUND,
                                TaskType.CONVERTER_COMPOUND,
                            ]:
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_id,
                                    status=HistoryUnitStatus.DONE,
                                    db_sync=db,
                                )

                if len(self.jobs) > 0:
                    scancelled_job_ids = self.wait_and_check_shutdown()

            logger.debug("[submit] END")
            return result, exception

        except Exception as e:
            logger.error(
                f"[submit] Unexpected exception. Original error: {str(e)}"
            )
            with next(get_sync_db()) as db:
                update_status_of_history_unit(
                    history_unit_id=history_unit_id,
                    status=HistoryUnitStatus.FAILED,
                    db_sync=db,
                )
            self.scancel_jobs()
            return None, e

    def multisubmit(
        self,
        base_command: str,
        workflow_task_order: int,
        workflow_task_id: int,
        task_name: str,
        list_parameters: list[dict],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: MultisubmitTaskType,
        config: SlurmConfig,
        user_id: int,
    ) -> tuple[dict[int, Any], dict[int, BaseException]]:
        """
        Note: `list_parameters`, `list_task_files` and `history_unit_ids`
        have the same size. For parallel tasks, this is also the number of
        input images, while for compound tasks these can differ.
        """

        config = self._enrich_slurm_config(config)

        logger.debug(f"[multisubmit] START, {len(list_parameters)=}")
        try:
            if self.is_shutdown():
                if task_type == TaskType.PARALLEL:
                    with next(get_sync_db()) as db:
                        bulk_update_status_of_history_unit(
                            history_unit_ids=history_unit_ids,
                            status=HistoryUnitStatus.FAILED,
                            db_sync=db,
                        )
                results = {}
                exceptions = {
                    ind: SHUTDOWN_EXCEPTION
                    for ind in range(len(list_parameters))
                }
                return results, exceptions

            self._check_no_active_jobs()
            self.validate_multisubmit_parameters(
                list_parameters=list_parameters,
                task_type=task_type,
                list_task_files=list_task_files,
                history_unit_ids=history_unit_ids,
            )

            workdir_local = list_task_files[0].wftask_subfolder_local
            workdir_remote = list_task_files[0].wftask_subfolder_remote

            # Create local&remote task subfolders
            if task_type == TaskType.PARALLEL:
                self._mkdir_local_folder(workdir_local.as_posix())
                self._mkdir_remote_folder(folder=workdir_remote.as_posix())

            results: dict[int, Any] = {}
            exceptions: dict[int, BaseException] = {}

            # NOTE: chunking has already taken place in `get_slurm_config`,
            # so that `config.tasks_per_job` is now set.

            # Divide arguments in batches of `tasks_per_job` tasks each
            tot_tasks = len(list_parameters)
            args_batches = []
            batch_size = config.tasks_per_job
            for ind_chunk in range(0, tot_tasks, batch_size):
                args_batches.append(
                    list_parameters[ind_chunk : ind_chunk + batch_size]  # noqa
                )
            if len(args_batches) != math.ceil(
                tot_tasks / config.tasks_per_job
            ):
                raise RuntimeError("Something wrong here while batching tasks")

            # Part 1/3: Iterate over chunks, prepare SlurmJob objects
            logger.debug("[multisubmit] Prepare `SlurmJob`s.")
            jobs_to_submit = []
            for ind_batch, chunk in enumerate(args_batches):
                # Read prefix based on the first task of this batch
                prefix = list_task_files[ind_batch * batch_size].prefix
                tasks = []
                for ind_chunk, parameters in enumerate(chunk):
                    index = (ind_batch * batch_size) + ind_chunk
                    tasks.append(
                        SlurmTask(
                            prefix=prefix,
                            index=index,
                            component=list_task_files[index].component,
                            workdir_local=workdir_local,
                            workdir_remote=workdir_remote,
                            parameters=parameters,
                            zarr_url=parameters["zarr_url"],
                            task_files=list_task_files[index],
                            workflow_task_order=workflow_task_order,
                            workflow_task_id=workflow_task_id,
                            task_name=task_name,
                        ),
                    )
                jobs_to_submit.append(
                    SlurmJob(
                        prefix=prefix,
                        workdir_local=workdir_local,
                        workdir_remote=workdir_remote,
                        tasks=tasks,
                    )
                )

            # NOTE: see issue 2431
            logger.debug("[multisubmit] Transfer files and submit jobs.")
            for slurm_job in jobs_to_submit:
                self._submit_single_sbatch(
                    base_command=base_command,
                    slurm_job=slurm_job,
                    slurm_config=config,
                )

            logger.info(f"[multisubmit] END submission phase, {self.job_ids=}")

            create_accounting_record_slurm(
                user_id=user_id,
                slurm_job_ids=self.job_ids,
            )

        except Exception as e:
            logger.error(
                "[multisubmit] Unexpected exception during submission."
                f" Original error {str(e)}"
            )
            self.scancel_jobs()
            if task_type == TaskType.PARALLEL:
                with next(get_sync_db()) as db:
                    bulk_update_status_of_history_unit(
                        history_unit_ids=history_unit_ids,
                        status=HistoryUnitStatus.FAILED,
                        db_sync=db,
                    )
            results = {}
            exceptions = {ind: e for ind in range(len(list_parameters))}
            return results, exceptions

        # Retrieval phase
        logger.debug("[multisubmit] START retrieval phase")
        scancelled_job_ids = []
        while len(self.jobs) > 0:
            # Look for finished jobs
            finished_job_ids = self._get_finished_jobs(job_ids=self.job_ids)
            logger.debug(f"[multisubmit] {finished_job_ids=}")
            finished_jobs = [
                self.jobs[_slurm_job_id] for _slurm_job_id in finished_job_ids
            ]
            fetch_artifacts_exception = None
            try:
                self._fetch_artifacts(finished_jobs)
            except Exception as e:
                logger.error(
                    "[multisubmit] Unexpected exception in "
                    "`_fetch_artifacts`. "
                    f"Original error: {str(e)}"
                )
                fetch_artifacts_exception = e

            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    logger.debug(f"[multisubmit] Now process {slurm_job_id=}")
                    slurm_job = self.jobs.pop(slurm_job_id)
                    for task in slurm_job.tasks:
                        logger.debug(
                            f"[multisubmit] Now process {task.index=}"
                        )
                        was_job_scancelled = slurm_job_id in scancelled_job_ids
                        if fetch_artifacts_exception is not None:
                            result = None
                            exception = fetch_artifacts_exception
                        else:
                            try:
                                (
                                    result,
                                    exception,
                                ) = self._postprocess_single_task(
                                    task=task,
                                    was_job_scancelled=was_job_scancelled,
                                )
                            except Exception as e:
                                logger.error(
                                    "[multisubmit] Unexpected exception in "
                                    "`_postprocess_single_task`. "
                                    f"Original error: {str(e)}"
                                )
                                result = None
                                exception = e
                        # Note: the relevant done/failed check is based on
                        # whether `exception is None`. The fact that
                        # `result is None` is not relevant for this purpose.
                        if exception is not None:
                            exceptions[task.index] = exception
                            if task_type == TaskType.PARALLEL:
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.FAILED,
                                    db_sync=db,
                                )
                        else:
                            results[task.index] = result
                            if task_type == TaskType.PARALLEL:
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.DONE,
                                    db_sync=db,
                                )

            if len(self.jobs) > 0:
                scancelled_job_ids = self.wait_and_check_shutdown()

        logger.debug("[multisubmit] END")
        return results, exceptions

    def check_fractal_server_versions(self) -> None:
        """
        Compare fractal-server versions of local/remote Python interpreters.
        """

        # Skip check when the local and remote interpreters are the same
        # (notably for some sudo-slurm deployments)
        if self.python_worker_interpreter == sys.executable:
            return

        # Fetch remote fractal-server version
        cmd = (
            f"{self.python_worker_interpreter} "
            "-m fractal_server.app.runner.versions"
        )
        stdout = self._run_remote_cmd(cmd)
        remote_version = json.loads(stdout.strip("\n"))["fractal_server"]

        # Verify local/remote version match
        if remote_version != __VERSION__:
            error_msg = (
                "Fractal-server version mismatch.\n"
                "Local interpreter: "
                f"({sys.executable}): {__VERSION__}.\n"
                "Remote interpreter: "
                f"({self.python_worker_interpreter}): {remote_version}."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def scancel_jobs(self) -> list[str]:
        logger.info("[scancel_jobs] START")
        scancelled_job_ids = self.job_ids
        if self.jobs:
            scancel_string = " ".join(scancelled_job_ids)
            scancel_cmd = f"scancel {scancel_string}"
            logger.warning(f"[scancel_jobs] {scancel_string}")
            try:
                self._run_remote_cmd(scancel_cmd)
            except Exception as e:
                logger.error(
                    "[scancel_jobs] `scancel` command failed. "
                    f"Original error:\n{str(e)}"
                )
        logger.info("[scancel_jobs] END")
        return scancelled_job_ids

    def validate_slurm_jobs_workdirs(
        self,
        slurm_jobs: list[SlurmJob],
    ) -> None:
        """
        Check that a list of `SlurmJob`s have homogeneous working folders.
        """
        set_workdir_local = {_job.workdir_local for _job in slurm_jobs}
        set_workdir_remote = {_job.workdir_remote for _job in slurm_jobs}
        if len(set_workdir_local) > 1:
            raise ValueError(f"Non-unique values in {set_workdir_local=}.")
        if len(set_workdir_remote) > 1:
            raise ValueError(f"Non-unique values in {set_workdir_remote=}.")
