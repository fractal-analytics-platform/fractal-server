# This adapts clusterfutures <https://github.com/sampsyo/clusterfutures>
# Original Copyright
# Copyright 2021 Adrian Sampson <asampson@cs.washington.edu>
# License: MIT
#
# Modified by:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
import math
import sys
import tarfile
import time
from concurrent.futures import Future
from concurrent.futures import InvalidStateError
from copy import copy
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Optional
from typing import Sequence

import cloudpickle
from cfut import SlurmExecutor
from devtools import debug  # noqa : F401
from fabric.connection import Connection
from invoke.exceptions import UnexpectedExit

from .....config import get_settings
from .....logger import set_logger
from .....syringe import Inject
from ...exceptions import JobExecutionError
from ...exceptions import TaskExecutionError
from ...filenames import SHUTDOWN_FILENAME
from ...task_files import get_task_file_paths
from ...task_files import TaskFiles
from ._batching import heuristics
from ._executor_wait_thread import FractalSlurmWaitThread
from ._slurm_config import get_default_slurm_config
from ._slurm_config import SlurmConfig
from fractal_server import __VERSION__
from fractal_server.app.runner.components import _COMPONENT_KEY_
from fractal_server.app.runner.executors.slurm_ssh._slurm_job import SlurmJob

logger = set_logger(__name__)


def _run_command_over_ssh(
    *,
    cmd: str,
    connection: Connection,
):
    logger.info(f"START running '{cmd}' over SSH.")
    try:
        res = connection.run(cmd)
    except UnexpectedExit as e:
        error_msg = (
            f"Running command `{cmd}` over SSH failed. "
            f"Original error: {str(e)}"
        )
        logger.error(error_msg)
        raise JobExecutionError(info=error_msg)

    logger.info(f"END   running '{cmd}' over SSH.")
    logger.info(f"STDOUT: {res.stdout}")
    logger.info(f"STDERR: {res.stderr}")
    return res.stdout


class FractalSlurmSSHExecutor(SlurmExecutor):
    """
    FractalSlurmSSHExecutor (inherits from cfut.SlurmExecutor)

    Attributes:
        slurm_user:
            Shell username that runs the `sbatch` command.
        common_script_lines:
            Arbitrary script lines that will always be included in the
            sbatch script
        workflow_dir_local:
            Directory for both the cfut/SLURM and fractal-server files and logs
        workflow_dir_remote:
            Directory for both the cfut/SLURM and fractal-server files and logs
        map_jobid_to_slurm_files:
            Dictionary with paths of slurm-related files for active jobs
    """

    wait_thread_cls = FractalSlurmWaitThread
    slurm_user: str
    shutdown_file: str
    common_script_lines: list[str]
    user_cache_dir: str
    workflow_dir_local: Path
    workflow_dir_remote: Path
    map_jobid_to_slurm_files_remote: dict[str, tuple[str, str, str]]
    keep_pickle_files: bool
    slurm_account: Optional[str]
    jobs: dict[str, tuple[Future, SlurmJob]]
    ssh_host: str
    ssh_user: str
    ssh_password: str

    def __init__(
        self,
        ssh_host: str,
        slurm_user: str,
        workflow_dir_local: Path,
        workflow_dir_remote: Path,
        shutdown_file: Optional[str] = None,
        user_cache_dir: Optional[str] = None,
        common_script_lines: Optional[list[str]] = None,
        slurm_poll_interval: Optional[int] = None,
        keep_pickle_files: bool = False,
        slurm_account: Optional[str] = None,
        ssh_user: str = "test01",
        ssh_password: str = "test01",
        *args,
        **kwargs,
    ):
        """
        Init method for FractalSlurmSSHExecutor
        """

        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password

        if not slurm_user:
            raise RuntimeError(
                "Missing attribute FractalSlurmSSHExecutor.slurm_user"
            )

        super().__init__(*args, **kwargs)

        self.keep_pickle_files = keep_pickle_files
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
            raise RuntimeError(
                "Invalid line in `FractalSlurmSSHExecutor.common_script_lines`: "
                f"'{invalid_line}'.\n"
                "SLURM account must be set via the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
        except StopIteration:
            pass

        self.workflow_dir_local = workflow_dir_local
        self.user_cache_dir = user_cache_dir

        self.workflow_dir_remote = workflow_dir_remote
        self.map_jobid_to_slurm_files_remote = {}

        # Set the attribute slurm_poll_interval for self.wait_thread (see
        # cfut.SlurmWaitThread)
        if not slurm_poll_interval:
            settings = Inject(get_settings)
            slurm_poll_interval = settings.FRACTAL_SLURM_POLL_INTERVAL
        self.wait_thread.slurm_poll_interval = slurm_poll_interval
        self.wait_thread.slurm_user = self.slurm_user

        self.wait_thread.shutdown_file = (
            shutdown_file
            or (self.workflow_dir_local / SHUTDOWN_FILENAME).as_posix()
        )
        self.wait_thread.shutdown_callback = self.shutdown
        self.wait_thread.jobs_finished_callback = self._jobs_finished

    def _cleanup(self, jobid: str) -> None:
        """
        Given a job ID as returned by _start, perform any necessary
        cleanup after the job has finished.
        """
        with self.jobs_lock:
            self.map_jobid_to_slurm_files_remote.pop(jobid)

    def get_input_pickle_file_path_local(
        self, *, arg: str, subfolder_name: str, prefix: Optional[str] = None
    ) -> Path:

        prefix = prefix or "cfut"
        output = (
            self.workflow_dir_local
            / subfolder_name
            / f"{prefix}_in_{arg}.pickle"
        )
        return output

    def get_input_pickle_file_path_remote(
        self, *, arg: str, subfolder_name: str, prefix: Optional[str] = None
    ) -> Path:

        prefix = prefix or "cfut"
        output = (
            self.workflow_dir_remote
            / subfolder_name
            / f"{prefix}_in_{arg}.pickle"
        )
        return output

    def get_output_pickle_file_path_local(
        self, *, arg: str, subfolder_name: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "cfut"
        return (
            self.workflow_dir_local
            / subfolder_name
            / f"{prefix}_out_{arg}.pickle"
        )

    def get_output_pickle_file_path_remote(
        self, *, arg: str, subfolder_name: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "cfut"
        return (
            self.workflow_dir_remote
            / subfolder_name
            / f"{prefix}_out_{arg}.pickle"
        )

    def get_slurm_script_file_path_local(
        self, *, subfolder_name: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "_temp"
        return (
            self.workflow_dir_local
            / subfolder_name
            / f"{prefix}_slurm_submit.sbatch"
        )

    def get_slurm_script_file_path_remote(
        self, *, subfolder_name: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "_temp"
        return (
            self.workflow_dir_remote
            / subfolder_name
            / f"{prefix}_slurm_submit.sbatch"
        )

    def get_slurm_stdout_file_path_local(
        self,
        *,
        subfolder_name: str,
        arg: str = "%j",
        prefix: Optional[str] = None,
    ) -> Path:
        prefix = prefix or "slurmpy.stdout"
        return (
            self.workflow_dir_local
            / subfolder_name
            / f"{prefix}_slurm_{arg}.out"
        )

    def get_slurm_stdout_file_path_remote(
        self,
        *,
        subfolder_name: str,
        arg: str = "%j",
        prefix: Optional[str] = None,
    ) -> Path:
        prefix = prefix or "slurmpy.stdout"
        return (
            self.workflow_dir_remote
            / subfolder_name
            / f"{prefix}_slurm_{arg}.out"
        )

    def get_slurm_stderr_file_path_local(
        self,
        *,
        subfolder_name: str,
        arg: str = "%j",
        prefix: Optional[str] = None,
    ) -> Path:
        prefix = prefix or "slurmpy.stderr"
        return (
            self.workflow_dir_local
            / subfolder_name
            / f"{prefix}_slurm_{arg}.err"
        )

    def get_slurm_stderr_file_path_remote(
        self,
        *,
        subfolder_name: str,
        arg: str = "%j",
        prefix: Optional[str] = None,
    ) -> Path:
        prefix = prefix or "slurmpy.stderr"
        return (
            self.workflow_dir_remote
            / subfolder_name
            / f"{prefix}_slurm_{arg}.err"
        )

    def submit(
        self,
        fun: Callable[..., Any],
        *fun_args: Sequence[Any],
        slurm_config: Optional[SlurmConfig] = None,
        task_files: Optional[TaskFiles] = None,
        **fun_kwargs: dict,
    ) -> Future:
        """
        Submit a function for execution on `FractalSlurmSSHExecutor`

        Arguments:
            fun: The function to be executed
            fun_args: Function positional arguments
            fun_kwargs: Function keyword arguments
            slurm_config:
                A `SlurmConfig` object; if `None`, use
                `get_default_slurm_config()`.
            task_files:
                A `TaskFiles` object; if `None`, use
                `self.get_default_task_files()`.

        Returns:
            Future representing the execution of the current SLURM job.
        """

        # Set defaults, if needed
        if slurm_config is None:
            slurm_config = get_default_slurm_config()
        if task_files is None:
            task_files = self.get_default_task_files()

        # Set slurm_file_prefix
        slurm_file_prefix = task_files.file_prefix

        # Include common_script_lines in extra_lines
        logger.debug(
            f"Adding {self.common_script_lines=} to "
            f"{slurm_config.extra_lines=}, from submit method."
        )
        current_extra_lines = slurm_config.extra_lines or []
        slurm_config.extra_lines = (
            current_extra_lines + self.common_script_lines
        )

        # Adapt slurm_config to the fact that this is a single-task SlurmJob
        # instance
        slurm_config.tasks_per_job = 1
        slurm_config.parallel_tasks_per_job = 1

        fut = self._submit_job(
            fun,
            slurm_config=slurm_config,
            slurm_file_prefix=slurm_file_prefix,
            task_files=task_files,
            single_task_submission=True,
            args=fun_args,
            kwargs=fun_kwargs,
        )
        return fut

    def map(
        self,
        fn: Callable[..., Any],
        iterable: list[Sequence[Any]],
        *,
        slurm_config: Optional[SlurmConfig] = None,
        task_files: Optional[TaskFiles] = None,
    ):
        """
        Return an iterator with the results of several execution of a function

        This function is based on `concurrent.futures.Executor.map` from Python
        Standard Library 3.11.
        Original Copyright 2009 Brian Quinlan. All Rights Reserved. Licensed to
        PSF under a Contributor Agreement.

        Main modifications from the PSF function:

        1. Only `fn` and `iterable` can be assigned as positional arguments;
        2. `*iterables` argument replaced with a single `iterable`;
        3. `timeout` and `chunksize` arguments are not supported.

        Arguments:
            fn:
                The function to be executed
            iterable:
                An iterable such that each element is the list of arguments to
                be passed to `fn`, as in `fn(*args)`.
            slurm_config:
                A `SlurmConfig` object; if `None`, use
                `get_default_slurm_config()`.
            task_files:
                A `TaskFiles` object; if `None`, use
                `self.get_default_task_files()`.

        """

        def _result_or_cancel(fut):
            """
            This function is based on the Python Standard Library 3.11.
            Original Copyright 2009 Brian Quinlan. All Rights Reserved.
            Licensed to PSF under a Contributor Agreement.
            """
            try:
                try:
                    return fut.result()
                finally:
                    fut.cancel()
            finally:
                # Break a reference cycle with the exception in
                # self._exception
                del fut

        # Set defaults, if needed
        if not slurm_config:
            slurm_config = get_default_slurm_config()
        if task_files is None:
            task_files = self.get_default_task_files()

        # Include common_script_lines in extra_lines
        logger.debug(
            f"Adding {self.common_script_lines=} to "
            f"{slurm_config.extra_lines=}, from map method."
        )
        current_extra_lines = slurm_config.extra_lines or []
        slurm_config.extra_lines = (
            current_extra_lines + self.common_script_lines
        )

        # Set file prefixes
        general_slurm_file_prefix = str(task_files.task_order)

        # Transform iterable into a list and count its elements
        list_args = list(iterable)
        tot_tasks = len(list_args)

        # Set/validate parameters for task batching
        tasks_per_job, parallel_tasks_per_job = heuristics(
            # Number of parallel components (always known)
            tot_tasks=len(list_args),
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

        # Divide arguments in batches of `n_tasks_per_script` tasks each
        args_batches = []
        batch_size = tasks_per_job
        for ind_chunk in range(0, tot_tasks, batch_size):
            args_batches.append(
                list_args[ind_chunk : ind_chunk + batch_size]  # noqa
            )
        if len(args_batches) != math.ceil(tot_tasks / tasks_per_job):
            raise RuntimeError("Something wrong here while batching tasks")

        # Fetch configuration variable
        settings = Inject(get_settings)
        FRACTAL_SLURM_SBATCH_SLEEP = settings.FRACTAL_SLURM_SBATCH_SLEEP

        # Construct list of futures (one per SLURM job, i.e. one per batch)
        fs = []
        current_component_index = 0
        for ind_batch, batch in enumerate(args_batches):
            batch_size = len(batch)
            this_slurm_file_prefix = (
                f"{general_slurm_file_prefix}_batch_{ind_batch:06d}"
            )
            fs.append(
                self._submit_job(
                    fn,
                    slurm_config=slurm_config,
                    slurm_file_prefix=this_slurm_file_prefix,
                    task_files=task_files,
                    single_task_submission=False,
                    components=batch,
                )
            )
            current_component_index += batch_size
            time.sleep(FRACTAL_SLURM_SBATCH_SLEEP)

        # Yield must be hidden in closure so that the futures are submitted
        # before the first iterator value is required.
        # NOTE: In this custom map() method, _result_or_cancel(fs.pop()) is an
        # iterable of results (if successful), and we should yield its elements
        # rather than the whole iterable.
        def result_iterator():
            """
            This function is based on the Python Standard Library 3.11.
            Original Copyright 2009 Brian Quinlan. All Rights Reserved.
            Licensed to PSF under a Contributor Agreement.
            """
            try:
                # reverse to keep finishing order
                fs.reverse()
                while fs:
                    # Careful not to keep a reference to the popped future
                    results = _result_or_cancel(fs.pop())
                    for res in results:
                        yield res
            finally:
                for future in fs:
                    future.cancel()

        return result_iterator()

    def _submit_job(
        self,
        fun: Callable[..., Any],
        slurm_file_prefix: str,
        task_files: TaskFiles,
        slurm_config: SlurmConfig,
        single_task_submission: bool = False,
        args: Optional[Sequence[Any]] = None,
        kwargs: Optional[dict] = None,
        components: Optional[list[Any]] = None,
    ) -> Future:
        """
        Submit a multi-task job to the pool, where each task is handled via the
        pickle/remote logic

        NOTE: this method has different behaviors when it is called from the
        `self.submit` or `self.map` methods (which is also encoded in
        `single_task_submission`):

        * When called from `self.submit`, it supports general `args` and
          `kwargs` arguments;
        * When called from `self.map`, there cannot be any `args` or `kwargs`
          argument, but there must be a `components` argument.

        Arguments:
            fun:
            slurm_file_prefix:
            task_files:
            slurm_config:
            single_task_submission:
            args:
            kwargs:
            components:

        Returns:
            Future representing the execution of the current SLURM job.
        """
        fut: Future = Future()

        # Inject SLURM account (if set) into slurm_config
        if self.slurm_account:
            slurm_config.account = self.slurm_account

        # Define slurm-job-related files
        if single_task_submission:
            if components is not None:
                raise ValueError(
                    f"{single_task_submission=} but components is not None"
                )
            job = SlurmJob(
                slurm_file_prefix=slurm_file_prefix,
                num_tasks_tot=1,
                slurm_config=slurm_config,
            )
            if job.num_tasks_tot > 1:
                raise ValueError(
                    "{single_task_submission=} but {job.num_tasks_tot=}"
                )
            job.single_task_submission = True
            job.wftask_file_prefixes = (task_files.file_prefix,)
            job.wftask_subfolder_name = task_files.subfolder_name

        else:
            if not components or len(components) < 1:
                raise ValueError(
                    "In FractalSlurmSSHExecutor._submit_job, given "
                    f"{components=}."
                )
            num_tasks_tot = len(components)
            job = SlurmJob(
                slurm_file_prefix=slurm_file_prefix,
                num_tasks_tot=num_tasks_tot,
                slurm_config=slurm_config,
            )

            _prefixes = []
            _subfolder_names = []
            for component in components:
                if isinstance(component, dict):
                    # This is needed for V2
                    actual_component = component.get(_COMPONENT_KEY_, None)
                else:
                    actual_component = component
                _task_file_paths = get_task_file_paths(
                    workflow_dir_local=task_files.workflow_dir_local,
                    workflow_dir_remote=task_files.workflow_dir_remote,
                    task_name=task_files.task_name,
                    task_order=task_files.task_order,
                    component=actual_component,
                )
                _prefixes.append(_task_file_paths.file_prefix)
                _subfolder_names.append(_task_file_paths.subfolder_name)
            job.wftask_file_prefixes = tuple(_prefixes)

            num_subfolders = len(set(_subfolder_names))
            if num_subfolders != 1:
                error_msg_short = (
                    f"[_submit_job] Subfolder list has {num_subfolders} "
                    "different values, but it must have only one (since "
                    "workflow tasks are executed one by one)."
                )
                error_msg_detail = (
                    "[_submit_job] Current unique subfolder names: "
                    f"{set(_subfolder_names)}"
                )
                logger.error(error_msg_short)
                logger.error(error_msg_detail)
                raise ValueError(error_msg_short)
            job.wftask_subfolder_name = _subfolder_names[0]

        # Check that server-side subfolder exists
        subfolder_path = self.workflow_dir_local / job.wftask_subfolder_name
        if not subfolder_path.exists():
            raise FileNotFoundError(
                f"Missing folder {subfolder_path.as_posix()}."
            )

        # Define I/O pickle file names/paths
        job.input_pickle_files_local = tuple(
            self.get_input_pickle_file_path_local(
                arg=job.workerids[ind],
                subfolder_name=job.wftask_subfolder_name,
                prefix=job.wftask_file_prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )
        job.input_pickle_files_remote = tuple(
            self.get_input_pickle_file_path_remote(
                arg=job.workerids[ind],
                subfolder_name=job.wftask_subfolder_name,
                prefix=job.wftask_file_prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )
        job.output_pickle_files_local = tuple(
            self.get_output_pickle_file_path_local(
                arg=job.workerids[ind],
                subfolder_name=job.wftask_subfolder_name,
                prefix=job.wftask_file_prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )
        job.output_pickle_files_remote = tuple(
            self.get_output_pickle_file_path_remote(
                arg=job.workerids[ind],
                subfolder_name=job.wftask_subfolder_name,
                prefix=job.wftask_file_prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )

        # Define SLURM-job file names/paths
        job.slurm_script_local = self.get_slurm_script_file_path_local(
            subfolder_name=job.wftask_subfolder_name,
            prefix=job.slurm_file_prefix,
        )
        job.slurm_script_remote = self.get_slurm_script_file_path_remote(
            subfolder_name=job.wftask_subfolder_name,
            prefix=job.slurm_file_prefix,
        )
        job.slurm_stdout_local = self.get_slurm_stdout_file_path_local(
            subfolder_name=job.wftask_subfolder_name,
            prefix=job.slurm_file_prefix,
        )
        job.slurm_stdout_remote = self.get_slurm_stdout_file_path_remote(
            subfolder_name=job.wftask_subfolder_name,
            prefix=job.slurm_file_prefix,
        )
        job.slurm_stderr_local = self.get_slurm_stderr_file_path_local(
            subfolder_name=job.wftask_subfolder_name,
            prefix=job.slurm_file_prefix,
        )
        job.slurm_stderr_remote = self.get_slurm_stderr_file_path_remote(
            subfolder_name=job.wftask_subfolder_name,
            prefix=job.slurm_file_prefix,
        )

        # Dump serialized versions+function+args+kwargs to pickle file
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        if job.single_task_submission:
            _args = args or []
            _kwargs = kwargs or {}
            funcser = cloudpickle.dumps((versions, fun, _args, _kwargs))
            with open(job.input_pickle_files_local[0], "wb") as f:
                f.write(funcser)
        else:
            for ind_component, component in enumerate(components):
                _args = [component]
                _kwargs = {}
                funcser = cloudpickle.dumps((versions, fun, _args, _kwargs))
                with open(
                    job.input_pickle_files_local[ind_component], "wb"
                ) as f:
                    f.write(funcser)

        # Submit job to SLURM, and get jobid
        jobid, job = self._start(job)

        # Add the SLURM script/out/err paths to map_jobid_to_slurm_files (this
        # must be after self._start(job), so that "%j" has already been
        # replaced with the job ID)
        with self.jobs_lock:
            self.map_jobid_to_slurm_files_remote[jobid] = (
                job.slurm_script_remote.as_posix(),
                job.slurm_stdout_remote.as_posix(),
                job.slurm_stderr_remote.as_posix(),
            )

        # Thread will wait for it to finish.
        self.wait_thread.wait(
            filenames=job.get_clean_output_pickle_files(),
            jobid=jobid,
        )

        with self.jobs_lock:
            self.jobs[jobid] = (fut, job)
        return fut

    def _prepare_JobExecutionError(
        self, jobid: str, info: str
    ) -> JobExecutionError:
        """
        Prepare the `JobExecutionError` for a given job

        This method creates a `JobExecutionError` object and sets its attribute
        to the appropriate SLURM-related file names. Note that the method
        should always be called after values in `self.map_jobid_to_slurm_files`
        have been updated, so that they point to `self.workflow_dir_local`
        files which are readable from `fractal-server`.

        Arguments:
            jobid:
                ID of the SLURM job.
            info:
        """
        # Extract SLURM file paths
        with self.jobs_lock:
            (
                slurm_script_file,
                slurm_stdout_file,
                slurm_stderr_file,
            ) = self.map_jobid_to_slurm_files_remote[jobid]
        # Construct JobExecutionError exception
        job_exc = JobExecutionError(
            cmd_file=slurm_script_file,
            stdout_file=slurm_stdout_file,
            stderr_file=slurm_stderr_file,
            info=info,
        )
        return job_exc

    def _completion(self, jobid: str) -> None:
        """
        Callback function to be executed whenever a job finishes.

        This function is executed by self.wait_thread (triggered by either
        finding an existing output pickle file `out_path` or finding that the
        SLURM job is over). Since this takes place on a different thread,
        failures may not be captured by the main thread; we use a broad
        try/except block, so that those exceptions are reported to the main
        thread via `fut.set_exception(...)`.

        Arguments:
            jobid: ID of the SLURM job
        """
        # Handle all uncaught exceptions in this broad try/except block
        try:

            # Retrieve job
            with self.jobs_lock:
                try:
                    fut, job = self.jobs.pop(jobid)
                except KeyError:
                    return
                if not self.jobs:
                    self.jobs_empty_cond.notify_all()

            # Copy all relevant files from self.workflow_dir_remote to
            # self.workflow_dir_local

            self._copy_files_from_remote_to_local(job)

            in_paths = job.input_pickle_files_local
            out_paths = tuple(
                (self.workflow_dir_local / job.wftask_subfolder_name / f.name)
                for f in job.output_pickle_files_local
            )

            outputs = []
            for ind_out_path, out_path in enumerate(out_paths):
                in_path = in_paths[ind_out_path]

                # The output pickle file may be missing because of some slow
                # filesystem operation; wait some time before considering it as
                # missing
                if not out_path.exists():
                    settings = Inject(get_settings)
                    time.sleep(settings.FRACTAL_SLURM_ERROR_HANDLING_INTERVAL)
                if not out_path.exists():
                    # Output pickle file is missing
                    info = (
                        "Output pickle file of the FractalSlurmSSHExecutor job "
                        "not found.\n"
                        f"Expected file path: {str(out_path)}.\n"
                        "Here are some possible reasons:\n"
                        "1. The SLURM job was scancel-ed, either by the user "
                        "or due to an error (e.g. an out-of-memory or timeout "
                        "error). Note that if the scancel took place before "
                        "the job started running, the SLURM out/err files "
                        "will be empty.\n"
                        "2. Some error occurred upon writing the file to disk "
                        "(e.g. because there is not enough space on disk, or "
                        "due to an overloaded NFS filesystem). "
                        "Note that the server configuration has "
                        "FRACTAL_SLURM_ERROR_HANDLING_INTERVAL="
                        f"{settings.FRACTAL_SLURM_ERROR_HANDLING_INTERVAL} "
                        "seconds.\n"
                    )
                    time.sleep(100000)
                    raise ValueError(info)
                    job_exc = self._prepare_JobExecutionError(jobid, info=info)
                    try:
                        fut.set_exception(job_exc)
                        raise job_exc
                        return
                    except InvalidStateError:
                        logger.warning(
                            f"Future {fut} (SLURM job ID: {jobid}) was already"
                            " cancelled, exit from"
                            " FractalSlurmSSHExecutor._completion."
                        )
                        if not self.keep_pickle_files:
                            in_path.unlink()
                        self._cleanup(jobid)
                        return

                # Read the task output (note: we now know that out_path exists)
                with out_path.open("rb") as f:
                    outdata = f.read()
                # Note: output can be either the task result (typically a
                # dictionary) or an ExceptionProxy object; in the latter
                # case, the ExceptionProxy definition is also part of the
                # pickle file (thanks to cloudpickle.dumps).
                success, output = cloudpickle.loads(outdata)
                try:
                    if success:
                        outputs.append(output)
                    else:
                        proxy = output
                        if proxy.exc_type_name == "JobExecutionError":
                            job_exc = self._prepare_JobExecutionError(
                                jobid, info=proxy.kwargs.get("info", None)
                            )
                            fut.set_exception(job_exc)
                            return
                        else:
                            # This branch catches both TaskExecutionError's
                            # (coming from the typical fractal-server
                            # execution of tasks, and with additional
                            # fractal-specific kwargs) or arbitrary
                            # exceptions (coming from a direct use of
                            # FractalSlurmSSHExecutor, possibly outside
                            # fractal-server)
                            kwargs = {}
                            for key in [
                                "workflow_task_id",
                                "workflow_task_order",
                                "task_name",
                            ]:
                                if key in proxy.kwargs.keys():
                                    kwargs[key] = proxy.kwargs[key]
                            exc = TaskExecutionError(proxy.tb, **kwargs)
                            fut.set_exception(exc)
                            return
                    if not self.keep_pickle_files:
                        out_path.unlink()
                except InvalidStateError:
                    logger.warning(
                        f"Future {fut} (SLURM job ID: {jobid}) was already"
                        " cancelled, exit from"
                        " FractalSlurmSSHExecutor._completion."
                    )
                    if not self.keep_pickle_files:
                        out_path.unlink()
                        in_path.unlink()

                    self._cleanup(jobid)
                    return

                # Clean up input pickle file
                if not self.keep_pickle_files:
                    in_path.unlink()
            self._cleanup(jobid)
            if job.single_task_submission:
                fut.set_result(outputs[0])
            else:
                fut.set_result(outputs)
            return

        except Exception as e:
            try:
                fut.set_exception(e)
                return
            except InvalidStateError:
                logger.warning(
                    f"Future {fut} (SLURM job ID: {jobid}) was already"
                    " cancelled, exit from"
                    " FractalSlurmSSHExecutor._completion."
                )

    def _copy_files_from_remote_to_local(
        self,
        job: SlurmJob,
    ):
        """
        Impersonate the user and copy task-related files

        For all files in `self.workflow_dir_remote` that start with
        `job.file_prefix`, read them (with `sudo -u` impersonation) and write
        them to `self.workflow_dir_local`.

        Files to copy:
        * Job-related files (SLURM stderr/stdout files); with prefix
          `job.slurm_file_prefix`;
        * Task-related files (stderr/stdout, args.json, metadiff.json, output
          pickle), with prefixes `job.wftask_file_prefixes`.

        Arguments:
            job:
                `SlurmJob` object (needed for its prefixes-related attributes).

        Raises:
            JobExecutionError: If a `cat` command fails.
        """
        logger.debug("[_copy_files_from_remote_to_local] Start")

        debug(vars(job))

        subfolder_name = job.wftask_subfolder_name
        tarfile_path_local = (
            self.workflow_dir_local / f"{subfolder_name}.tar.gz"
        ).as_posix()
        tarfile_path_remote = (
            self.workflow_dir_remote / f"{subfolder_name}.tar.gz"
        ).as_posix()

        # Remove local tarfile
        Path(tarfile_path_local).unlink()
        logger.warning(f"In principle I just removed {tarfile_path_local}")
        logger.warning(f"{Path(tarfile_path_local).exists()=}")

        with Connection(
            host=self.ssh_host,
            user=self.ssh_user,
            connect_kwargs={"password": self.ssh_password},
        ) as conn:

            # Remove remote tarfile
            rm_command = f"rm {tarfile_path_remote}"
            _run_command_over_ssh(cmd=rm_command, connection=conn)

            # Create remote tarfile
            tar_command = (
                "tar --verbose "
                f"--directory {self.workflow_dir_remote.as_posix()} "
                "--create "
                f"--file {tarfile_path_remote} "
                f"{subfolder_name}"
            )
            _run_command_over_ssh(cmd=tar_command, connection=conn)

            # DEBUG
            ls_command = f"ls {self.workflow_dir_remote.as_posix()}"
            _run_command_over_ssh(cmd=ls_command, connection=conn)

            # Fetch tarfile
            res = conn.get(
                remote=tarfile_path_remote,
                local=tarfile_path_local,
            )
            logger.info(
                f"Subfolder archive transferred back to {tarfile_path_local}"
            )
            logger.info(f"{res=}")

            globs = self.workflow_dir_local.glob("*")
            logger.warning(f"{globs=}")

        # Extract tarfile locally
        tar_command = (
            "tar --verbose --extract "
            f"--file {tarfile_path_local} "
            f"--directory {self.workflow_dir_local.as_posix()}"
        )

        # FIXME: replace subprocess with tarfile library
        import subprocess, shlex

        res = subprocess.run(
            shlex.split(tar_command),
            capture_output=True, encoding="utf-8", check=True
        )

        logger.debug("[_copy_files_from_remote_to_local] End")

    def _start(
        self,
        job: SlurmJob,
    ) -> tuple[str, SlurmJob]:
        """
        Submit function for execution on a SLURM cluster
        """

        # Prepare commands to be included in SLURM submission script
        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        cmdlines = []
        for ind_task in range(job.num_tasks_tot):
            input_pickle_file = job.input_pickle_files_remote[ind_task]
            output_pickle_file = job.output_pickle_files_remote[ind_task]
            cmdlines.append(
                (
                    f"{python_worker_interpreter}"
                    " -m fractal_server.app.runner.executors.slurm.remote "
                    f"--input-file {input_pickle_file} "
                    f"--output-file {output_pickle_file}"
                )
            )

        # Prepare SLURM submission script
        sbatch_script_content = self._prepare_sbatch_script(
            slurm_config=job.slurm_config,
            list_commands=cmdlines,
            slurm_out_path=str(job.slurm_stdout_remote),
            slurm_err_path=str(job.slurm_stderr_remote),
        )
        with job.slurm_script_local.open("w") as f:
            f.write(sbatch_script_content)

        # Create compressed subfolder archive
        subfolder_name = job.wftask_subfolder_name
        local_subfolder = self.workflow_dir_local / subfolder_name
        remote_subfolder = self.workflow_dir_remote / subfolder_name
        tarfile_path_local = (
            self.workflow_dir_local / f"{subfolder_name}.tar.gz"
        ).as_posix()
        tarfile_path_remote = (
            self.workflow_dir_remote / f"{subfolder_name}.tar.gz"
        ).as_posix()
        with tarfile.open(tarfile_path_local, "w:gz") as tar:
            tar.add(local_subfolder, arcname=subfolder_name, recursive=True)
        logger.info(f"Subfolder archive created at {tarfile_path_local}")

        with Connection(
            host=self.ssh_host,
            user=self.ssh_user,
            connect_kwargs={"password": self.ssh_password},
        ) as conn:

            # FIXME: There is a logical issue here, we are sending a tar for
            # each sbatch script, but we should only send one (or otherwise
            # the archive should only include some specific files).

            # Transfer archive
            res = conn.put(
                local=tarfile_path_local,
                remote=tarfile_path_remote,
            )
            logger.info(
                f"Subfolder archive transferred to {tarfile_path_remote}"
            )
            logger.info(f"{res=}")

            # DEBUG: see that the archive is where it should
            ls_command = f"ls {Path(tarfile_path_remote).parent.as_posix()}"
            stdout = _run_command_over_ssh(cmd=ls_command, connection=conn)

            # Uncompress archive
            tar_command = (
                "tar --verbose --extract "
                f"--file {tarfile_path_remote} "
                f"--directory {self.workflow_dir_remote.as_posix()}"
            )
            stdout = _run_command_over_ssh(cmd=tar_command, connection=conn)

            # DEBUG: Check that the archive was uncompressed
            remote_subfolder = (
                self.workflow_dir_remote / subfolder_name
            ).as_posix()
            ls_command = f"ls {remote_subfolder}"
            stdout = _run_command_over_ssh(cmd=ls_command, connection=conn)

            # Run sbatch
            sbatch_command = f"sbatch --parsable {job.slurm_script_remote}"
            sbatch_stdout = _run_command_over_ssh(
                cmd=sbatch_command, connection=conn
            )

        # Extract SLURM job ID from stdout
        try:
            stdout = sbatch_stdout.strip("\n")
            jobid = int(stdout)
        except ValueError as e:
            error_msg = (
                f"Submit command `{sbatch_command}` returned "
                f"`{stdout=}` which cannot be cast to an integer "
                f"SLURM-job ID. Original error:\n{str(e)}"
            )
            logger.error(error_msg)
            raise JobExecutionError(info=error_msg)
        jobid_str = str(jobid)

        # Plug SLURM job id in stdout/stderr SLURM file paths (local and remote)
        def _replace_slurm_job_id(_old_path: Path) -> Path:
            return Path(_old_path.as_posix().replace("%j", jobid_str))

        job.slurm_stdout_local = _replace_slurm_job_id(job.slurm_stdout_local)
        job.slurm_stdout_remote = _replace_slurm_job_id(
            job.slurm_stdout_remote
        )
        job.slurm_stderr_local = _replace_slurm_job_id(job.slurm_stderr_local)
        job.slurm_stderr_remote = _replace_slurm_job_id(
            job.slurm_stderr_remote
        )

        return jobid_str, job

    def _prepare_sbatch_script(
        self,
        *,
        list_commands: list[str],
        slurm_out_path: str,
        slurm_err_path: str,
        slurm_config: SlurmConfig,
    ):

        num_tasks_max_running = slurm_config.parallel_tasks_per_job
        mem_per_task_MB = slurm_config.mem_per_task_MB

        # Set ntasks
        ntasks = min(len(list_commands), num_tasks_max_running)
        if len(list_commands) < num_tasks_max_running:
            ntasks = len(list_commands)
            slurm_config.parallel_tasks_per_job = ntasks
            logger.debug(
                f"{len(list_commands)=} is smaller than "
                f"{num_tasks_max_running=}. Setting {ntasks=}."
            )

        # Prepare SLURM preamble based on SlurmConfig object
        script_lines = slurm_config.to_sbatch_preamble(
            user_cache_dir=self.user_cache_dir
        )

        # Extend SLURM preamble with variable which are not in SlurmConfig, and
        # fix their order
        script_lines.extend(
            [
                f"#SBATCH --err={slurm_err_path}",
                f"#SBATCH --out={slurm_out_path}",
                f"#SBATCH -D {self.workflow_dir_remote}",
            ]
        )
        script_lines = slurm_config.sort_script_lines(script_lines)
        logger.debug(script_lines)

        # Always print output of `pwd`
        script_lines.append('echo "Working directory (pwd): `pwd`"\n')

        # Complete script preamble
        script_lines.append("\n")

        # Include command lines
        tmp_list_commands = copy(list_commands)
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
        return script

    def get_default_task_files(self) -> TaskFiles:
        """
        This will be called when self.submit or self.map are called from
        outside fractal-server, and then lack some optional arguments.
        """
        task_files = TaskFiles(
            workflow_dir_local=self.workflow_dir_local,
            workflow_dir_remote=self.workflow_dir_remote,
            task_order=None,
            task_name="name",
        )
        return task_files

    def shutdown(self, wait=True, *, cancel_futures=False):
        """
        Clean up all executor variables. Note that this function is executed on
        the self.wait_thread thread, see _completion.
        """

        logger.debug("Executor shutdown: start")

        # Handle all job futures
        slurm_jobs_to_scancel = []
        with self.jobs_lock:
            while self.jobs:
                jobid, fut_and_job = self.jobs.popitem()
                slurm_jobs_to_scancel.append(jobid)
                fut = fut_and_job[0]
                self.map_jobid_to_slurm_files_remote.pop(jobid)
                if not fut.cancelled():
                    fut.set_exception(
                        JobExecutionError(
                            "Job cancelled due to executor shutdown."
                        )
                    )
                    fut.cancel()

        # Cancel SLURM jobs
        if slurm_jobs_to_scancel:
            scancel_string = " ".join(slurm_jobs_to_scancel)
            logger.warning(f"Now scancel-ing SLURM jobs {scancel_string}")
            scancel_command = f"scancel {scancel_string}"
            with Connection(
                host=self.ssh_host,
                user=self.ssh_user,
                connect_kwargs={"password": self.ssh_password},
            ) as conn:
                _run_command_over_ssh(
                    cmd=scancel_command,
                    connection=conn,
                )
        logger.debug("Executor shutdown: end")

    def __exit__(self, *args, **kwargs):
        """
        See
        https://github.com/fractal-analytics-platform/fractal-server/issues/1508
        """
        logger.debug(
            "[FractalSlurmSSHExecutor.__exit__] Stop and join `wait_thread`"
        )
        self.wait_thread.stop()
        self.wait_thread.join()
        logger.debug("[FractalSlurmSSHExecutor.__exit__] End")

    def run_squeue(self, job_ids):
        squeue_command = (
            "squeue "
            "--noheader "
            "--format='%i %T' "
            "--jobs __JOBS__ "
            "--states=all"
        )
        job_ids = ",".join([str(j) for j in job_ids])
        squeue_command = squeue_command.replace("__JOBS__", job_ids)
        with Connection(
            host=self.ssh_host,
            user=self.ssh_user,
            connect_kwargs={"password": self.ssh_password},
        ) as conn:
            stdout = _run_command_over_ssh(
                cmd=squeue_command,
                connection=conn,
            )
        return stdout

    def _jobs_finished(self, job_ids) -> set[str]:
        """
        Check which ones of the given Slurm jobs already finished

        The function is based on the `_jobs_finished` function from
        clusterfutures (version 0.5).
        Original Copyright: 2022 Adrian Sampson
        (released under the MIT licence)
        """

        from cfut.slurm import STATES_FINISHED

        # If there is no Slurm job to check, return right away
        if not job_ids:
            return set()
        id_to_state = dict()

        try:
            stdout = self.run_squeue(job_ids)
            id_to_state = {
                out.split()[0]: out.split()[1] for out in stdout.splitlines()
            }
        except Exception:
            raise NotImplementedError
            id_to_state = dict()
            for j in job_ids:
                res = self.run_squeue([j])
                if res.returncode != 0:
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
