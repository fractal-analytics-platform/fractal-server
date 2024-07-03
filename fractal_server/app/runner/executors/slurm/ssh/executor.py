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
import json
import math
import sys
import tarfile
import threading
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
from fabric.connection import Connection
from paramiko.ssh_exception import NoValidConnectionsError

from ....filenames import SHUTDOWN_FILENAME
from ....task_files import get_task_file_paths
from ....task_files import TaskFiles
from ....versions import get_versions
from ...slurm._slurm_config import get_default_slurm_config
from ...slurm._slurm_config import SlurmConfig
from .._batching import heuristics
from ._executor_wait_thread import FractalSlurmWaitThread
from fractal_server.app.runner.components import _COMPONENT_KEY_
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm.ssh._slurm_job import SlurmJob
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import check_connection
from fractal_server.ssh._fabric import run_command_over_ssh
from fractal_server.syringe import Inject

logger = set_logger(__name__)


class FractalSlurmSSHExecutor(SlurmExecutor):
    """
    FractalSlurmSSHExecutor (inherits from cfut.SlurmExecutor)

    FIXME: docstring

    Attributes:
        connection: SSH connection
        shutdown_file:
        python_remote: Equal to `settings.FRACTAL_SLURM_WORKER_PYTHON`
        wait_thread_cls: Class for waiting thread
        keep_pickle_files:
        workflow_dir_local:
            Directory for both the cfut/SLURM and fractal-server files and logs
        workflow_dir_remote:
            Directory for both the cfut/SLURM and fractal-server files and logs
        common_script_lines:
            Arbitrary script lines that will always be included in the
            sbatch script
        slurm_account:
        jobs:
        map_jobid_to_slurm_files:
            Dictionary with paths of slurm-related files for active jobs
    """

    connection: Connection

    workflow_dir_local: Path
    workflow_dir_remote: Path
    shutdown_file: str
    python_remote: str

    wait_thread_cls = FractalSlurmWaitThread
    keep_pickle_files: bool

    common_script_lines: list[str]
    slurm_account: Optional[str]

    jobs: dict[str, tuple[Future, SlurmJob]]
    map_jobid_to_slurm_files_local: dict[str, tuple[str, str, str]]

    def __init__(
        self,
        *,
        # SSH connection
        connection: Connection,
        # Folders and files
        workflow_dir_local: Path,
        workflow_dir_remote: Path,
        # Runner options
        keep_pickle_files: bool = False,
        # Monitoring options
        slurm_poll_interval: Optional[int] = None,
        # SLURM submission script options
        common_script_lines: Optional[list[str]] = None,
        slurm_account: Optional[str] = None,
        # Other kwargs are ignored
        **kwargs,
    ):
        """
        Init method for FractalSlurmSSHExecutor

        Note: since we are not using `super().__init__`, we duplicate some
        relevant bits of `cfut.ClusterExecutor.__init__`.

        Args:
            connection:
            workflow_dir_local:
            workflow_dir_remote:
            keep_pickle_files:
            slurm_poll_interval:
            common_script_lines:
            slurm_account:
        """

        if kwargs != {}:
            raise ValueError(
                f"FractalSlurmSSHExecutor received unexpected {kwargs=}"
            )

        self.workflow_dir_local = workflow_dir_local
        self.workflow_dir_remote = workflow_dir_remote

        # Relevant bits of cfut.ClusterExecutor.__init__ are copied here,
        # postponing the .start() call to when the callbacks are defined
        self.jobs = {}
        self.job_outfiles = {}
        self.jobs_lock = threading.Lock()
        self.jobs_empty_cond = threading.Condition(self.jobs_lock)
        self.wait_thread = self.wait_thread_cls(self._completion)

        # Set up attributes and methods for self.wait_thread
        # cfut.SlurmWaitThread)
        self.wait_thread.shutdown_callback = self.shutdown
        self.wait_thread.jobs_finished_callback = self._jobs_finished
        if slurm_poll_interval is None:
            settings = Inject(get_settings)
            slurm_poll_interval = settings.FRACTAL_SLURM_POLL_INTERVAL
        elif slurm_poll_interval <= 0:
            raise ValueError(f"Invalid attribute {slurm_poll_interval=}")
        self.wait_thread.slurm_poll_interval = slurm_poll_interval
        self.wait_thread.shutdown_file = (
            self.workflow_dir_local / SHUTDOWN_FILENAME
        ).as_posix()

        # Now start self.wait_thread (note: this must be *after* its callback
        # methods have been defined)
        self.wait_thread.start()

        # Define remote Python interpreter
        settings = Inject(get_settings)
        self.python_remote = settings.FRACTAL_SLURM_WORKER_PYTHON
        if self.python_remote is None:
            raise ValueError("FRACTAL_SLURM_WORKER_PYTHON is not set. Exit.")

        # Initialize connection and perform handshake
        self.connection = connection
        logger.warning(self.connection)
        self.handshake()

        # Set/validate parameters for SLURM submission scripts
        self.slurm_account = slurm_account
        self.common_script_lines = common_script_lines or []
        self._validate_common_script_lines()

        # Set/initialize some more options
        self.keep_pickle_files = keep_pickle_files
        self.map_jobid_to_slurm_files_local = {}

    def _validate_common_script_lines(self):
        """
        Check that SLURM account is not set in `self.common_script_lines`.
        """
        try:
            invalid_line = next(
                line
                for line in self.common_script_lines
                if line.startswith("#SBATCH --account=")
            )
            raise RuntimeError(
                "Invalid line in `FractalSlurmSSHExecutor."
                "common_script_lines`: "
                f"'{invalid_line}'.\n"
                "SLURM account must be set via the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
        except StopIteration:
            pass

    def _cleanup(self, jobid: str) -> None:
        """
        Given a job ID, perform any necessary cleanup after the job has
        finished.
        """
        with self.jobs_lock:
            self.map_jobid_to_slurm_files_local.pop(jobid)

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

        job = self._prepare_job(
            fun,
            slurm_config=slurm_config,
            slurm_file_prefix=slurm_file_prefix,
            task_files=task_files,
            single_task_submission=True,
            args=fun_args,
            kwargs=fun_kwargs,
        )
        try:
            self._put_subfolder_sftp(jobs=[job])
        except NoValidConnectionsError as e:
            logger.error("NoValidConnectionError")
            logger.error(f"{str(e)=}")
            logger.error(f"{e.errors=}")
            for err in e.errors:
                logger.error(f"{str(err)}")
            raise e
        future, job_id_str = self._submit_job(job)
        self.wait_thread.wait(job_id=job_id_str)
        return future

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

        logger.debug("[map] Job preparation - START")
        current_component_index = 0
        jobs_to_submit = []
        for ind_batch, batch in enumerate(args_batches):
            batch_size = len(batch)
            this_slurm_file_prefix = (
                f"{general_slurm_file_prefix}_batch_{ind_batch:06d}"
            )
            new_job_to_submit = self._prepare_job(
                fn,
                slurm_config=slurm_config,
                slurm_file_prefix=this_slurm_file_prefix,
                task_files=task_files,
                single_task_submission=False,
                components=batch,
            )
            jobs_to_submit.append(new_job_to_submit)
            current_component_index += batch_size
        logger.debug("[map] Job preparation - END")

        try:
            self._put_subfolder_sftp(jobs=jobs_to_submit)
        except NoValidConnectionsError as e:
            logger.error("NoValidConnectionError")
            logger.error(f"{str(e)=}")
            logger.error(f"{e.errors=}")
            for err in e.errors:
                logger.error(f"{str(err)}")

            raise e

        # Construct list of futures (one per SLURM job, i.e. one per batch)
        # FIXME SSH: we may create a single `_submit_many_jobs` method to
        # reduce the number of commands run over SSH
        logger.debug("[map] Job submission - START")
        fs = []
        job_ids = []
        for job in jobs_to_submit:
            future, job_id = self._submit_job(job)
            job_ids.append(job_id)
            fs.append(future)
            time.sleep(FRACTAL_SLURM_SBATCH_SLEEP)
        for job_id in job_ids:
            self.wait_thread.wait(job_id=job_id)
        logger.debug("[map] Job submission - END")

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

    def _prepare_job(
        self,
        fun: Callable[..., Any],
        slurm_file_prefix: str,
        task_files: TaskFiles,
        slurm_config: SlurmConfig,
        single_task_submission: bool = False,
        args: Optional[Sequence[Any]] = None,
        kwargs: Optional[dict] = None,
        components: Optional[list[Any]] = None,
    ) -> SlurmJob:
        """
        Prepare a SLURM job locally, without submitting it

        This function prepares and writes the local submission script, but it
        does not transfer it to the SLURM cluster.

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
            SlurmJob object
        """

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

            # Check that all components share the same subfolder
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

        # Define I/O pickle file local/remote paths
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

        # Define SLURM-job file local/remote paths
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

        # Dump serialized versions+function+args+kwargs to pickle file(s)
        versions = get_versions()
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

        # Prepare commands to be included in SLURM submission script
        cmdlines = []
        for ind_task in range(job.num_tasks_tot):
            input_pickle_file = job.input_pickle_files_remote[ind_task]
            output_pickle_file = job.output_pickle_files_remote[ind_task]
            cmdlines.append(
                (
                    f"{self.python_remote}"
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

        return job

    def _put_subfolder_sftp(self, jobs: list[SlurmJob]) -> None:
        """
        Transfer the jobs subfolder to the remote host.

        Arguments:
            jobs: The list of `SlurmJob` objects associated to a given
                subfolder.
        """

        # Check that the subfolder is unique
        subfolder_names = [job.wftask_subfolder_name for job in jobs]
        if len(set(subfolder_names)) > 1:
            raise ValueError(
                "[_put_subfolder] Invalid list of jobs, "
                f"{set(subfolder_names)=}."
            )
        subfolder_name = subfolder_names[0]

        # Create compressed subfolder archive (locally)
        local_subfolder = self.workflow_dir_local / subfolder_name
        tarfile_name = f"{subfolder_name}.tar.gz"
        tarfile_path_local = (
            self.workflow_dir_local / tarfile_name
        ).as_posix()
        tarfile_path_remote = (
            self.workflow_dir_remote / tarfile_name
        ).as_posix()
        with tarfile.open(tarfile_path_local, "w:gz") as tar:
            for this_file in local_subfolder.glob("*"):
                tar.add(this_file, arcname=this_file.name)
        logger.info(f"Subfolder archive created at {tarfile_path_local}")

        # Transfer archive
        t_0_put = time.perf_counter()
        self.connection.put(
            local=tarfile_path_local,
            remote=tarfile_path_remote,
        )
        t_1_put = time.perf_counter()
        logger.info(
            f"Subfolder archive transferred to {tarfile_path_remote}"
            f" - elapsed: {t_1_put - t_0_put:.3f} s"
        )
        # Uncompress archive (remotely)
        tar_command = (
            f"{self.python_remote} -m "
            "fractal_server.app.runner.extract_archive "
            f"{tarfile_path_remote}"
        )
        run_command_over_ssh(cmd=tar_command, connection=self.connection)

        # Remove local version
        t_0_rm = time.perf_counter()
        Path(tarfile_path_local).unlink()
        t_1_rm = time.perf_counter()
        logger.info(
            f"Local archive removed - elapsed: {t_1_rm - t_0_rm:.3f} s"
        )

    def _submit_job(self, job: SlurmJob) -> tuple[Future, str]:
        """
        Submit a job to SLURM via SSH.

        This method must always be called after `self._put_subfolder`.

        Arguments:
            job: The `SlurmJob` object to submit.
        """

        # Submit job to SLURM, and get jobid
        sbatch_command = f"sbatch --parsable {job.slurm_script_remote}"
        sbatch_stdout = run_command_over_ssh(
            cmd=sbatch_command,
            connection=self.connection,
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
        job_id_str = str(jobid)

        # Plug job id in stdout/stderr SLURM file paths (local and remote)
        def _replace_job_id(_old_path: Path) -> Path:
            return Path(_old_path.as_posix().replace("%j", job_id_str))

        job.slurm_stdout_local = _replace_job_id(job.slurm_stdout_local)
        job.slurm_stdout_remote = _replace_job_id(job.slurm_stdout_remote)
        job.slurm_stderr_local = _replace_job_id(job.slurm_stderr_local)
        job.slurm_stderr_remote = _replace_job_id(job.slurm_stderr_remote)

        # Add the SLURM script/out/err paths to map_jobid_to_slurm_files (this
        # must be after the `sbatch` call, so that "%j" has already been
        # replaced with the job ID)
        with self.jobs_lock:
            self.map_jobid_to_slurm_files_local[job_id_str] = (
                job.slurm_script_local.as_posix(),
                job.slurm_stdout_local.as_posix(),
                job.slurm_stderr_local.as_posix(),
            )

        # Create future
        future = Future()
        with self.jobs_lock:
            self.jobs[job_id_str] = (future, job)
        return future, job_id_str

    def _prepare_JobExecutionError(
        self, jobid: str, info: str
    ) -> JobExecutionError:
        """
        Prepare the `JobExecutionError` for a given job

        This method creates a `JobExecutionError` object and sets its attribute
        to the appropriate SLURM-related file names. Note that the SLURM files
        are the local ones (i.e. the ones in `self.workflow_dir_local`).

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
            ) = self.map_jobid_to_slurm_files_local[jobid]
        # Construct JobExecutionError exception
        job_exc = JobExecutionError(
            cmd_file=slurm_script_file,
            stdout_file=slurm_stdout_file,
            stderr_file=slurm_stderr_file,
            info=info,
        )
        return job_exc

    def _missing_pickle_error_msg(self, out_path: Path) -> str:
        settings = Inject(get_settings)
        info = (
            "Output pickle file of the FractalSlurmSSHExecutor "
            "job not found.\n"
            f"Expected file path: {out_path.as_posix()}n"
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
        return info

    def _handle_remaining_jobs(
        self,
        remaining_futures: list[Future],
        remaining_job_ids: list[str],
        remaining_jobs: list[SlurmJob],
    ) -> None:
        """
        Helper function used within _completion, when looping over a list of
        several jobs/futures.
        """
        for future in remaining_futures:
            try:
                future.cancel()
            except InvalidStateError:
                pass
        for job_id in remaining_job_ids:
            self._cleanup(job_id)
        if not self.keep_pickle_files:
            for job in remaining_jobs:
                for path in job.output_pickle_files_local:
                    path.unlink()
                for path in job.input_pickle_files_local:
                    path.unlink()

    def _completion(self, job_ids: list[str]) -> None:
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

        # Loop over all job_ids, and fetch future and job objects
        futures: list[Future] = []
        jobs: list[SlurmJob] = []
        with self.jobs_lock:
            for job_id in job_ids:
                future, job = self.jobs.pop(job_id)
                futures.append(future)
                jobs.append(job)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()

        # Fetch subfolder from remote host
        try:
            self._get_subfolder_sftp(jobs=jobs)
        except NoValidConnectionsError as e:
            logger.error("NoValidConnectionError")
            logger.error(f"{str(e)=}")
            logger.error(f"{e.errors=}")
            for err in e.errors:
                logger.error(f"{str(err)}")

            raise e

        # First round of checking whether all output files exist
        missing_out_paths = []
        for job in jobs:
            for ind_out_path, out_path in enumerate(
                job.output_pickle_files_local
            ):
                if not out_path.exists():
                    missing_out_paths.append(out_path)
        num_missing = len(missing_out_paths)
        if num_missing > 0:
            # Output pickle files may be missing e.g. because of some slow
            # filesystem operation; wait some time before re-trying
            settings = Inject(get_settings)
            sleep_time = settings.FRACTAL_SLURM_ERROR_HANDLING_INTERVAL
            logger.info(
                f"{num_missing} output pickle files are missing; "
                f"sleep {sleep_time} seconds."
            )
            for missing_file in missing_out_paths:
                logger.debug(f"Missing output pickle file: {missing_file}")
            time.sleep(sleep_time)

        # Handle all jobs
        for ind_job, job_id in enumerate(job_ids):
            try:
                # Retrieve job and future objects
                job = jobs[ind_job]
                future = futures[ind_job]
                remaining_job_ids = job_ids[ind_job + 1 :]  # noqa: E203
                remaining_futures = futures[ind_job + 1 :]  # noqa: E203

                outputs = []

                for ind_out_path, out_path in enumerate(
                    job.output_pickle_files_local
                ):
                    in_path = job.input_pickle_files_local[ind_out_path]
                    if not out_path.exists():
                        # Output pickle file is still missing
                        info = self._missing_pickle_error_msg(out_path)
                        job_exc = self._prepare_JobExecutionError(
                            job_id, info=info
                        )
                        try:
                            future.set_exception(job_exc)
                            self._handle_remaining_jobs(
                                remaining_futures=remaining_futures,
                                remaining_job_ids=remaining_job_ids,
                            )
                            return
                        except InvalidStateError:
                            logger.warning(
                                f"Future {future} (SLURM job ID: {job_id}) "
                                "was already cancelled."
                            )
                            if not self.keep_pickle_files:
                                in_path.unlink()
                            self._cleanup(job_id)
                            self._handle_remaining_jobs(
                                remaining_futures=remaining_futures,
                                remaining_job_ids=remaining_job_ids,
                            )
                            return

                    # Read the task output
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
                                    job_id, info=proxy.kwargs.get("info", None)
                                )
                                future.set_exception(job_exc)
                                self._handle_remaining_jobs(
                                    remaining_futures=remaining_futures,
                                    remaining_job_ids=remaining_job_ids,
                                )
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
                                future.set_exception(exc)
                                self._handle_remaining_jobs(
                                    remaining_futures=remaining_futures,
                                    remaining_job_ids=remaining_job_ids,
                                )
                                return
                        if not self.keep_pickle_files:
                            out_path.unlink()
                    except InvalidStateError:
                        logger.warning(
                            f"Future {future} (SLURM job ID: {job_id}) was "
                            "already cancelled, exit from "
                            "FractalSlurmSSHExecutor._completion."
                        )
                        if not self.keep_pickle_files:
                            out_path.unlink()
                            in_path.unlink()

                        self._cleanup(job_id)
                        self._handle_remaining_jobs(
                            remaining_futures=remaining_futures,
                            remaining_job_ids=remaining_job_ids,
                        )
                        return

                    # Clean up input pickle file
                    if not self.keep_pickle_files:
                        in_path.unlink()
                self._cleanup(job_id)
                if job.single_task_submission:
                    future.set_result(outputs[0])
                else:
                    future.set_result(outputs)

            except Exception as e:
                try:
                    future.set_exception(e)
                    return
                except InvalidStateError:
                    logger.warning(
                        f"Future {future} (SLURM job ID: {job_id}) was already"
                        " cancelled, exit from"
                        " FractalSlurmSSHExecutor._completion."
                    )

    def _get_subfolder_sftp(self, jobs: list[SlurmJob]) -> None:
        """
        Fetch a remote folder via tar+sftp+tar

        Arguments:
            job:
                `SlurmJob` object (needed for its prefixes-related attributes).
        """

        # Check that the subfolder is unique
        subfolder_names = [job.wftask_subfolder_name for job in jobs]
        if len(set(subfolder_names)) > 1:
            raise ValueError(
                "[_put_subfolder] Invalid list of jobs, "
                f"{set(subfolder_names)=}."
            )
        subfolder_name = subfolder_names[0]

        t_0 = time.perf_counter()
        logger.debug("[_get_subfolder_sftp] Start")
        tarfile_path_local = (
            self.workflow_dir_local / f"{subfolder_name}.tar.gz"
        ).as_posix()
        tarfile_path_remote = (
            self.workflow_dir_remote / f"{subfolder_name}.tar.gz"
        ).as_posix()

        # Remove local tarfile - FIXME SSH: is this needed?
        logger.warning(f"In principle I just removed {tarfile_path_local}")
        logger.warning(f"{Path(tarfile_path_local).exists()=}")

        # Remove remote tarfile - FIXME SSH: is this needed?
        # rm_command = f"rm {tarfile_path_remote}"
        # _run_command_over_ssh(cmd=rm_command, connection=self.connection)
        logger.warning(f"Unlink {tarfile_path_remote=} - START")
        self.connection.sftp().unlink(tarfile_path_remote)
        logger.warning(f"Unlink {tarfile_path_remote=} - STOP")

        # Create remote tarfile
        tar_command = (
            f"{self.python_remote} "
            "-m fractal_server.app.runner.compress_folder "
            f"{(self.workflow_dir_remote / subfolder_name).as_posix()}"
        )
        stdout = run_command_over_ssh(
            cmd=tar_command, connection=self.connection
        )
        print(stdout)

        # Fetch tarfile
        t_0_get = time.perf_counter()
        self.connection.get(
            remote=tarfile_path_remote,
            local=tarfile_path_local,
        )
        t_1_get = time.perf_counter()
        logger.info(
            f"Subfolder archive transferred back to {tarfile_path_local}"
            f" - elapsed: {t_1_get - t_0_get:.3f} s"
        )

        # Extract tarfile locally
        with tarfile.open(tarfile_path_local) as tar:
            tar.extractall(path=(self.workflow_dir_local / subfolder_name))

        t_1 = time.perf_counter()
        logger.info("[_get_subfolder_sftp] End - " f"elapsed: {t_1-t_0:.3f} s")

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
            remote_export_dir=self.workflow_dir_remote.as_posix()
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
        # self.connection.close()

        # Handle all job futures
        slurm_jobs_to_scancel = []
        with self.jobs_lock:
            while self.jobs:
                jobid, fut_and_job = self.jobs.popitem()
                slurm_jobs_to_scancel.append(jobid)
                fut = fut_and_job[0]
                self.map_jobid_to_slurm_files_local.pop(jobid)
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
            run_command_over_ssh(
                cmd=scancel_command, connection=self.connection
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
        stdout = run_command_over_ssh(
            cmd=squeue_command,
            connection=self.connection,
        )
        return stdout

    def _jobs_finished(self, job_ids: list[str]) -> set[str]:
        """
        Check which ones of the given Slurm jobs already finished

        The function is based on the `_jobs_finished` function from
        clusterfutures (version 0.5).
        Original Copyright: 2022 Adrian Sampson
        (released under the MIT licence)
        """

        from cfut.slurm import STATES_FINISHED

        logger.debug(
            f"[FractalSlurmSSHExecutor._jobs_finished] START ({job_ids=})"
        )

        # If there is no Slurm job to check, return right away
        if not job_ids:
            logger.debug(
                "[FractalSlurmSSHExecutor._jobs_finished] "
                "No jobs provided, return."
            )
            return set()

        try:
            stdout = self.run_squeue(job_ids)
            id_to_state = {
                out.split()[0]: out.split()[1] for out in stdout.splitlines()
            }
            # Finished jobs only stay in squeue for a few mins (configurable).
            # If a job ID isn't there, we'll assume it's finished.
            output = {
                _id
                for _id in job_ids
                if id_to_state.get(_id, "COMPLETED") in STATES_FINISHED
            }
            logger.debug(
                f"[FractalSlurmSSHExecutor._jobs_finished] END - {output=}"
            )
            return output
        except Exception as e:
            # If something goes wrong, proceed anyway
            logger.error(
                f"Something wrong in _jobs_finished. Original error: {str(e)}"
            )
            output = set()
            logger.debug(
                f"[FractalSlurmSSHExecutor._jobs_finished] END - {output=}"
            )
            return output

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

    def handshake(self) -> dict:
        """
        Healthcheck for SSH connection and for versions match.

        FIXME SSH: We should add a timeout here
        FIXME SSH: We could include checks on the existence of folders
        FIXME SSH: We could include further checks on version matches
        """

        check_connection(self.connection)

        t_start_handshake = time.perf_counter()

        logger.info("[FractalSlurmSSHExecutor.ssh_handshake] START")
        cmd = f"{self.python_remote} -m fractal_server.app.runner.versions"
        stdout = run_command_over_ssh(cmd=cmd, connection=self.connection)
        remote_versions = json.loads(stdout.strip("\n"))

        # Check compatibility with local versions
        local_versions = get_versions()
        remote_fractal_server = remote_versions["fractal_server"]
        local_fractal_server = local_versions["fractal_server"]
        if remote_fractal_server != local_fractal_server:
            error_msg = (
                "Fractal-server version mismatch.\n"
                "Local interpreter: "
                f"({sys.executable}): {local_versions}.\n"
                "Remote interpreter: "
                f"({self.python_remote}): {remote_versions}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        t_end_handshake = time.perf_counter()
        logger.info(
            "[FractalSlurmSSHExecutor.ssh_handshake] END"
            f" - elapsed: {t_end_handshake-t_start_handshake:.3f} s"
        )
        return remote_versions
