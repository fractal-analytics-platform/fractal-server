import json
import math
import sys
import time
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional

import cloudpickle

from ..slurm_common._slurm_config import SlurmConfig
from ..slurm_common.slurm_job_task_models import SlurmJob
from ..slurm_common.slurm_job_task_models import SlurmTask
from ._batching import heuristics
from ._handle_exception_proxy import _handle_exception_proxy
from ._job_states import STATES_FINISHED
from fractal_server import __VERSION__
from fractal_server.app.db import get_sync_db
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.task_files import MULTISUBMIT_PREFIX
from fractal_server.app.runner.task_files import SUBMIT_PREFIX
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)

# FIXME: Transform several logger.info into logger.debug.


class BaseSlurmRunner(BaseRunner):
    shutdown_file: Path
    common_script_lines: list[str]
    user_cache_dir: str
    root_dir_local: Path
    root_dir_remote: Path
    poll_interval: int
    jobs: dict[str, SlurmJob]
    python_worker_interpreter: str
    slurm_runner_type: Literal["ssh", "sudo"]

    def __init__(
        self,
        root_dir_local: Path,
        root_dir_remote: Path,
        slurm_runner_type: Literal["ssh", "sudo"],
        common_script_lines: Optional[list[str]] = None,
        user_cache_dir: Optional[str] = None,
        poll_interval: Optional[int] = None,
    ):
        self.slurm_runner_type = slurm_runner_type
        self.root_dir_local = root_dir_local
        self.root_dir_remote = root_dir_remote
        self.common_script_lines = common_script_lines or []
        self._check_slurm_account()
        self.user_cache_dir = user_cache_dir

        settings = Inject(get_settings)

        self.poll_interval = (
            poll_interval or settings.FRACTAL_SLURM_POLL_INTERVAL
        )
        self.check_fractal_server_versions()

        # Create job folders. Note that the local one may or may not exist
        # depending on whether it is a test or an actual run
        if not self.root_dir_local.is_dir():
            self._mkdir_local_folder(self.root_dir_local.as_posix())
        self._mkdir_remote_folder(self.root_dir_remote.as_posix())

        self.shutdown_file = self.root_dir_local / SHUTDOWN_FILENAME
        self.jobs = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def _run_local_cmd(self, cmd: str) -> str:
        raise NotImplementedError("Implement in child class.")

    def _run_remote_cmd(self, cmd: str) -> str:
        raise NotImplementedError("Implement in child class.")

    def run_squeue(self, job_ids: list[str]) -> tuple[bool, str]:
        # FIXME: review different cases (exception vs no job found)
        job_id_single_str = ",".join([str(j) for j in job_ids])
        cmd = (
            f"squeue --noheader --format='%i %T' --jobs {job_id_single_str}"
            " --states=all"
        )

        try:
            if self.slurm_runner_type == "sudo":
                stdout = self._run_local_cmd(cmd)
            else:
                stdout = self._run_remote_cmd(cmd)
            return True, stdout
        except Exception as e:
            logger.info(f"{cmd=} failed with {str(e)}")
            return False, ""

    def _get_finished_jobs(self, job_ids: list[str]) -> set[str]:
        #  If there is no Slurm job to check, return right away

        if not job_ids:
            return set()
        id_to_state = dict()

        success, stdout = self.run_squeue(job_ids)
        if success:
            id_to_state = {
                out.split()[0]: out.split()[1] for out in stdout.splitlines()
            }
        else:
            id_to_state = dict()
            for j in job_ids:
                success, res = self.run_squeue([j])
                if not success:
                    logger.info(f"Job {j} not found. Marked it as completed")
                    id_to_state.update({str(j): "COMPLETED"})
                else:
                    id_to_state.update(
                        {res.stdout.split()[0]: res.stdout.split()[1]}
                    )

        # Finished jobs only stay in squeue for a few mins (configurable). If
        # a job ID isn't there, we'll assume it's finished.
        return {
            j
            for j in job_ids
            if id_to_state.get(j, "COMPLETED") in STATES_FINISHED
        }

    def _mkdir_local_folder(self, folder: str) -> None:
        raise NotImplementedError("Implement in child class.")

    def _mkdir_remote_folder(self, folder: str) -> None:
        raise NotImplementedError("Implement in child class.")

    def _submit_single_sbatch(
        self,
        func,
        slurm_job: SlurmJob,
        slurm_config: SlurmConfig,
    ) -> str:
        logger.info("[_submit_single_sbatch] START")
        # Prepare input pickle(s)
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        for task in slurm_job.tasks:
            # Wrinte input pickle
            _args = []
            _kwargs = dict(
                parameters=task.parameters,
                remote_files=task.task_files.remote_files_dict,
            )
            funcser = cloudpickle.dumps((versions, func, _args, _kwargs))
            with open(task.input_pickle_file_local, "wb") as f:
                f.write(funcser)
            logger.info(
                "[_submit_single_sbatch] Written "
                f"{task.input_pickle_file_local=}"
            )

            if self.slurm_runner_type == "ssh":
                # Send input pickle (only relevant for SSH)
                self.fractal_ssh.send_file(
                    local=task.input_pickle_file_local,
                    remote=task.input_pickle_file_remote,
                )
                logger.info(
                    "[_submit_single_sbatch] Transferred "
                    f"{task.input_pickle_file_local=}"
                )

        # Prepare commands to be included in SLURM submission script
        cmdlines = []
        for task in slurm_job.tasks:
            if self.slurm_runner_type == "ssh":
                input_pickle_file = task.input_pickle_file_remote
            else:
                input_pickle_file = task.input_pickle_file_local
            output_pickle_file = task.output_pickle_file_remote
            cmdlines.append(
                (
                    f"{self.python_worker_interpreter}"
                    " -m fractal_server.app.runner."
                    "executors.slurm_common.remote "
                    f"--input-file {input_pickle_file} "
                    f"--output-file {output_pickle_file}"
                )
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
        logger.info(script_lines)

        # Always print output of `uname -n` and `pwd`
        script_lines.append(
            '"Hostname: `uname -n`; current directory: `pwd`"\n'
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

        # Write submission script
        with open(slurm_job.slurm_submission_script_local, "w") as f:
            f.write(script)
        logger.info(
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
            logger.info(f"Now run {submit_command=}")
            sbatch_stdout = self._run_remote_cmd(submit_command)
        else:
            logger.info(f"Now using {pre_submission_cmds=}")
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
            logger.info(f"Now run {wrapper_script=}")
            sbatch_stdout = self._run_remote_cmd(f"bash {wrapper_script}")

        # Submit SLURM job and retrieve job ID
        logger.info(f"[_submit_single_sbatc] {sbatch_stdout=}")
        stdout = sbatch_stdout.strip("\n")
        submitted_job_id = int(stdout)
        slurm_job.slurm_job_id = str(submitted_job_id)

        # Add job to self.jobs
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.info(
            "[_submit_single_sbatch] Added "
            f"{slurm_job.slurm_job_id} to self.jobs."
        )
        logger.info("[_submit_single_sbatch] END")

    def _copy_files_from_remote_to_local(
        self,
        slurm_job: SlurmJob,
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
            pass
            # FIXME: Re-include unlinks of pickle files
            # Path(task.input_pickle_file_local).unlink(missing_ok=True)
            # Path(task.output_pickle_file_local).unlink(missing_ok=True)

    def is_shutdown(self) -> bool:
        # FIXME: shutdown is not implemented
        return self.shutdown_file.exists()

    @property
    def job_ids(self) -> list[str]:
        return list(self.jobs.keys())

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

        logger.info("[submit] START")

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
        logger.info("[submit] Create local/remote folders - START")
        self._mkdir_local_folder(folder=workdir_local.as_posix())
        self._mkdir_remote_folder(folder=workdir_remote.as_posix())
        logger.info("[submit] Create local/remote folders - END")

        # Add prefix to task_files object
        task_files.prefix = SUBMIT_PREFIX

        # Submission phase
        slurm_job = SlurmJob(
            prefix=SUBMIT_PREFIX,
            workdir_local=workdir_local,
            workdir_remote=workdir_remote,
            tasks=[
                SlurmTask(
                    prefix=SUBMIT_PREFIX,
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
        logger.info(f"[submit] END submission phase, {self.job_ids=}")

        # FIXME: replace this sleep a more precise check
        settings = Inject(get_settings)
        sleep_time = settings.FRACTAL_SLURM_INTERVAL_BEFORE_RETRIEVAL
        logger.warning(f"[submit] Now sleep {sleep_time} (FIXME)")
        time.sleep(sleep_time)

        # Retrieval phase
        logger.info("[submit] START retrieval phase")
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = self._get_finished_jobs(job_ids=self.job_ids)
            logger.info(f"{finished_job_ids=}")
            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    logger.info(f"Now process {slurm_job_id=}")
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

            time.sleep(self.poll_interval)

        logger.info("[submit] END")
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

        logger.info(f"[multisubmit] START, {len(list_parameters)=}")

        workdir_local = list_task_files[0].wftask_subfolder_local
        workdir_remote = list_task_files[0].wftask_subfolder_remote

        # Create local&remote task subfolders
        if task_type == "parallel":
            self._mkdir_local_folder(workdir_local.as_posix())
            self._mkdir_remote_folder(folder=workdir_remote.as_posix())

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        # TODO Pick a data structure for results and exceptions, or review the
        # interface
        results: dict[int, Any] = {}
        exceptions: dict[int, BaseException] = {}

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
            prefix = f"{MULTISUBMIT_PREFIX}-{ind_batch:06d}"
            tasks = []
            for ind_chunk, parameters in enumerate(chunk):
                index = (ind_batch * batch_size) + ind_chunk
                current_task_files = list_task_files[index]
                current_task_files.prefix = prefix
                tasks.append(
                    SlurmTask(
                        prefix=prefix,
                        index=index,
                        component=current_task_files.component,
                        workdir_local=workdir_local,
                        workdir_remote=workdir_remote,
                        parameters=parameters,
                        zarr_url=parameters["zarr_url"],
                        task_files=current_task_files,
                    ),
                )

            slurm_job = SlurmJob(
                prefix=prefix,
                workdir_local=workdir_local,
                workdir_remote=workdir_remote,
                tasks=tasks,
            )
            self._submit_single_sbatch(
                func,
                slurm_job=slurm_job,
                slurm_config=config,
            )
        logger.info(f"END submission phase, {self.job_ids=}")

        # FIXME: replace this sleep a more precise check
        settings = Inject(get_settings)
        sleep_time = settings.FRACTAL_SLURM_INTERVAL_BEFORE_RETRIEVAL
        logger.warning(f"[submit] Now sleep {sleep_time} (FIXME)")
        time.sleep(sleep_time)

        # Retrieval phase
        logger.info("START retrieval phase")
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = self._get_finished_jobs(job_ids=self.job_ids)
            logger.info(f"{finished_job_ids=}")
            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    logger.info(f"Now processing {slurm_job_id=}")
                    slurm_job = self.jobs.pop(slurm_job_id)
                    self._copy_files_from_remote_to_local(slurm_job)
                    for task in slurm_job.tasks:
                        logger.info(f"Now processing {task.index=}")
                        result, exception = self._postprocess_single_task(
                            task=task
                        )

                        # Note: the relevant done/failed check is based on
                        # whether `exception is None`. The fact that
                        # `result is None` is not relevant for this purpose.
                        if exception is not None:
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
                            results[task.index] = result
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.DONE,
                                    db_sync=db,
                                )

            time.sleep(self.poll_interval)
        return results, exceptions

    def check_fractal_server_versions(self):
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

    def scancel_jobs(self) -> None:
        logger.info("[scancel_jobs] START")

        if self.jobs:
            scancel_string = " ".join(self.job_ids)
            scancel_cmd = f"scancel {scancel_string}"
            logger.warning(f"Now scancel-ing SLURM jobs {scancel_string}")
            try:
                self._run_remote_cmd(scancel_cmd)
            except Exception as e:
                logger.warning(
                    "[scancel_jobs] `scancel` command failed. "
                    f"Original error:\n{str(e)}"
                )

        logger.info("[scancel_jobs] END")
