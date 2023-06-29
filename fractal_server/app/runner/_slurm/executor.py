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
import shlex
import subprocess  # nosec
import sys
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
from cfut.util import random_string

from ....config import get_settings
from ....logger import set_logger
from ....syringe import Inject
from .._common import get_task_file_paths
from .._common import SHUTDOWN_FILENAME
from .._common import TaskFiles
from ..common import JobExecutionError
from ..common import TaskExecutionError
from ._batching import heuristics
from ._executor_wait_thread import FractalSlurmWaitThread
from ._slurm_config import get_default_slurm_config
from ._slurm_config import SlurmConfig
from ._subprocess_run_as_user import _glob_as_user
from ._subprocess_run_as_user import _path_exists_as_user
from ._subprocess_run_as_user import _run_command_as_user
from fractal_server import __VERSION__


logger = set_logger(__name__)


class SlurmJob:
    """
    Collect information related to a FractalSlurmExecutor job

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
            `_copy_files_from_user_to_server` method.
        wftask_file_prefixes:
            Prefix for files that are created as part of the functions
            submitted for execution on the `FractalSlurmExecutor`; this is
            needed in the `_copy_files_from_user_to_server` method, and also to
            construct the names of per-task input/output pickle files.
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
    slurm_script: Path
    slurm_stdout: Path
    slurm_stderr: Path
    # Per-task attributes
    workerids: tuple[str, ...]
    wftask_file_prefixes: tuple[str, ...]
    input_pickle_files: tuple[Path, ...]
    output_pickle_files: tuple[Path, ...]
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
        return tuple(str(f.as_posix()) for f in self.output_pickle_files)


class FractalSlurmExecutor(SlurmExecutor):
    """
    FractalSlurmExecutor (inherits from cfut.SlurmExecutor)

    Attributes:
        slurm_user:
            Shell username that runs the `sbatch` command.
        common_script_lines:
            Arbitrary script lines that will always be included in the
            sbatch script
        working_dir:
            Directory for both the cfut/SLURM and fractal-server files and logs
        working_dir_user:
            Directory for both the cfut/SLURM and fractal-server files and logs
        map_jobid_to_slurm_files:
            Dictionary with paths of slurm-related files for active jobs
    """

    wait_thread_cls = FractalSlurmWaitThread
    slurm_user: str
    shutdown_file: str
    common_script_lines: list[str]
    user_cache_dir: str
    working_dir: Path
    working_dir_user: Path
    map_jobid_to_slurm_files: dict[str, tuple[str, str, str]]
    keep_pickle_files: bool

    def __init__(
        self,
        slurm_user: str,
        working_dir: Path,
        working_dir_user: Path,
        shutdown_file: Optional[str] = None,
        user_cache_dir: Optional[str] = None,
        common_script_lines: Optional[list[str]] = None,
        slurm_poll_interval: Optional[int] = None,
        keep_pickle_files: bool = False,
        *args,
        **kwargs,
    ):
        """
        Init method for FractalSlurmExecutor
        """

        if not slurm_user:
            raise RuntimeError(
                "Missing attribute FractalSlurmExecutor.slurm_user"
            )

        super().__init__(*args, **kwargs)

        self.keep_pickle_files = keep_pickle_files
        self.slurm_user = slurm_user

        self.common_script_lines = common_script_lines or []
        self.working_dir = working_dir
        if not _path_exists_as_user(
            path=str(working_dir_user), user=self.slurm_user
        ):
            logger.info(f"Missing folder {working_dir_user=}")
        self.user_cache_dir = user_cache_dir

        self.working_dir_user = working_dir_user
        self.map_jobid_to_slurm_files = {}

        # Set the attribute slurm_poll_interval for self.wait_thread (see
        # cfut.SlurmWaitThread)
        if not slurm_poll_interval:
            settings = Inject(get_settings)
            slurm_poll_interval = settings.FRACTAL_SLURM_POLL_INTERVAL
        self.wait_thread.slurm_poll_interval = slurm_poll_interval
        self.wait_thread.slurm_user = self.slurm_user

        self.wait_thread.shutdown_file = (
            shutdown_file or (self.working_dir / SHUTDOWN_FILENAME).as_posix()
        )
        self.wait_thread.shutdown_callback = self.shutdown

    def _cleanup(self, jobid: str) -> None:
        """
        Given a job ID as returned by _start, perform any necessary
        cleanup after the job has finished.
        """
        with self.jobs_lock:
            self.map_jobid_to_slurm_files.pop(jobid)

    def get_input_pickle_file_path(
        self, arg: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "cfut"
        return self.working_dir / f"{prefix}_in_{arg}.pickle"

    def get_output_pickle_file_path(
        self, arg: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "cfut"
        return self.working_dir_user / f"{prefix}_out_{arg}.pickle"

    def get_slurm_script_file_path(self, prefix: Optional[str] = None) -> Path:
        prefix = prefix or "_temp"
        return self.working_dir / f"{prefix}_slurm_submit.sbatch"

    def get_slurm_stdout_file_path(
        self, arg: str = "%j", prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "slurmpy.stdout"
        return self.working_dir_user / f"{prefix}_slurm_{arg}.out"

    def get_slurm_stderr_file_path(
        self, arg: str = "%j", prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "slurmpy.stderr"
        return self.working_dir_user / f"{prefix}_slurm_{arg}.err"

    def submit(
        self,
        fun: Callable[..., Any],
        *fun_args: Sequence[Any],
        slurm_config: Optional[SlurmConfig] = None,
        task_files: Optional[TaskFiles] = None,
        **fun_kwargs: dict,
    ) -> Future:
        """
        Submit a function for execution on `FractalSlurmExecutor`

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

        Returns:
            An iterator of results.
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
        components: list[Any] = None,
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

        # Define slurm-job-related files
        if single_task_submission:
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
        else:
            if not components or len(components) < 1:
                raise ValueError(
                    "In FractalSlurmExecutor._submit_job, given "
                    f"{components=}."
                )
            num_tasks_tot = len(components)
            job = SlurmJob(
                slurm_file_prefix=slurm_file_prefix,
                num_tasks_tot=num_tasks_tot,
                slurm_config=slurm_config,
            )
            job.wftask_file_prefixes = tuple(
                get_task_file_paths(
                    workflow_dir=task_files.workflow_dir,
                    workflow_dir_user=task_files.workflow_dir_user,
                    task_order=task_files.task_order,
                    component=component,
                ).file_prefix
                for component in components
            )

        # Define I/O pickle file names/paths
        job.input_pickle_files = tuple(
            self.get_input_pickle_file_path(
                job.workerids[ind],
                prefix=job.wftask_file_prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )
        job.output_pickle_files = tuple(
            self.get_output_pickle_file_path(
                job.workerids[ind],
                prefix=job.wftask_file_prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )

        # Define SLURM-job file names/paths
        job.slurm_script = self.get_slurm_script_file_path(
            prefix=job.slurm_file_prefix
        )
        job.slurm_stdout = self.get_slurm_stdout_file_path(
            prefix=job.slurm_file_prefix
        )
        job.slurm_stderr = self.get_slurm_stderr_file_path(
            prefix=job.slurm_file_prefix
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
            with open(job.input_pickle_files[0], "wb") as f:
                f.write(funcser)
        else:
            for ind_component, component in enumerate(components):
                _args = [component]
                _kwargs = {}
                funcser = cloudpickle.dumps((versions, fun, _args, _kwargs))
                with open(job.input_pickle_files[ind_component], "wb") as f:
                    f.write(funcser)

        # Submit job to SLURM, and get jobid
        jobid, job = self._start(job)

        # Add the SLURM script/out/err paths to map_jobid_to_slurm_files (this
        # must be after self._start(job), so that "%j" has already been
        # replaced with the job ID)
        with self.jobs_lock:
            self.map_jobid_to_slurm_files[jobid] = (
                job.slurm_script.as_posix(),
                job.slurm_stdout.as_posix(),
                job.slurm_stderr.as_posix(),
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
        Prepare the JobExecutionError for a given job

            1. Wait for `FRACTAL_SLURM_KILLWAIT_INTERVAL` seconds, so that
               SLURM has time to complete the job cancellation.
            2. Assign the SLURM-related file names as attributes of the
               JobExecutionError instance.

        Note: this function should be called after values in
        `self.map_jobid_to_slurm_files` have been updated, so that they point
        to `self.working_dir` files which are readable for the user running
        fractal-server.  by the server

        Arguments:
            jobid:
                ID of the SLURM job.
        """
        # Wait FRACTAL_SLURM_KILLWAIT_INTERVAL seconds
        settings = Inject(get_settings)
        settings.FRACTAL_SLURM_KILLWAIT_INTERVAL
        time.sleep(settings.FRACTAL_SLURM_KILLWAIT_INTERVAL)
        # Extract SLURM file paths
        with self.jobs_lock:
            (
                slurm_script_file,
                slurm_stdout_file,
                slurm_stderr_file,
            ) = self.map_jobid_to_slurm_files[jobid]
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

            # Copy all relevant files from self.working_dir_user to
            # self.working_dir

            self._copy_files_from_user_to_server(job)

            # Update the paths to use the files in self.working_dir (rather
            # than the user's ones in self.working_dir_user)
            with self.jobs_lock:
                self.map_jobid_to_slurm_files[jobid]
                (
                    slurm_script_file,
                    slurm_stdout_file,
                    slurm_stderr_file,
                ) = self.map_jobid_to_slurm_files[jobid]
            new_slurm_stdout_file = str(
                self.working_dir / Path(slurm_stdout_file).name
            )
            new_slurm_stderr_file = str(
                self.working_dir / Path(slurm_stderr_file).name
            )
            with self.jobs_lock:
                self.map_jobid_to_slurm_files[jobid] = (
                    slurm_script_file,
                    new_slurm_stdout_file,
                    new_slurm_stderr_file,
                )

            in_paths = job.input_pickle_files
            out_paths = tuple(
                self.working_dir / f.name for f in job.output_pickle_files
            )

            outputs = []
            for ind_out_path, out_path in enumerate(out_paths):
                in_path = in_paths[ind_out_path]

                # The output pickle file may be missing because of some slow
                # filesystem operation; wait some time before considering it as
                # missing
                if not out_path.exists():
                    settings = Inject(get_settings)
                    time.sleep(settings.FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME)
                if not out_path.exists():
                    # Output pickle file is missing
                    info = (
                        "Output pickle file of the FractalSlurmExecutor job "
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
                        "FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME="
                        f"{settings.FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME} "
                        "seconds.\n"
                    )
                    job_exc = self._prepare_JobExecutionError(jobid, info=info)
                    try:
                        fut.set_exception(job_exc)
                        return
                    except InvalidStateError:
                        logger.warning(
                            f"Future {fut} (SLURM job ID: {jobid}) was already"
                            " cancelled, exit from"
                            " FractalSlurmExecutor._completion."
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
                            # FractalSlurmExecutor, possibly outside
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
                        " FractalSlurmExecutor._completion."
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
                    " FractalSlurmExecutor._completion."
                )

    def _copy_files_from_user_to_server(
        self,
        job: SlurmJob,
    ):
        """
        Impersonate the user and copy task-related files

        For all files in `self.working_dir_user` that start with
        `job.file_prefix`, read them (with `sudo -u` impersonation) and write
        them to `self.working_dir`.

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
        logger.debug("Enter _copy_files_from_user_to_server")
        if self.working_dir_user == self.working_dir:
            return

        prefixes = set(
            [job.slurm_file_prefix] + list(job.wftask_file_prefixes)
        )

        logger.debug(f"[_copy_files_from_user_to_server] {prefixes=}")
        logger.debug(
            f"[_copy_files_from_user_to_server] {str(self.working_dir_user)=}"
        )

        for prefix in prefixes:

            files_to_copy = _glob_as_user(
                folder=str(self.working_dir_user),
                user=self.slurm_user,
                startswith=prefix,
            )
            logger.debug(
                "[_copy_files_from_user_to_server] "
                f"{prefix=}, {len(files_to_copy)=}"
            )

            for source_file_name in files_to_copy:
                if " " in source_file_name:
                    raise ValueError(
                        f'source_file_name="{source_file_name}" '
                        "contains whitespaces"
                    )
                source_file_path = str(
                    self.working_dir_user / source_file_name
                )
                dest_file_path = str(self.working_dir / source_file_name)

                # Read source_file_path (requires sudo)
                # NOTE: By setting encoding=None, we read/write bytes instead
                # of strings; this is needed to also handle pickle files.
                cmd = f"cat {source_file_path}"
                res = _run_command_as_user(
                    cmd=cmd, user=self.slurm_user, encoding=None
                )
                if res.returncode != 0:
                    info = (
                        f'Running cmd="{cmd}" as {self.slurm_user=} failed\n\n'
                        f"{res.returncode=}\n\n"
                        f"{res.stdout=}\n\n{res.stderr=}\n"
                    )
                    logger.error(info)
                    raise JobExecutionError(info)
                # Write to dest_file_path (including empty files)
                dest_file_path = str(self.working_dir / source_file_name)
                with open(dest_file_path, "wb") as f:
                    f.write(res.stdout)
        logger.debug("[_copy_files_from_user_to_server] End")

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
            input_pickle_file = job.input_pickle_files[ind_task]
            output_pickle_file = job.output_pickle_files[ind_task]
            cmdlines.append(
                (
                    f"{python_worker_interpreter}"
                    " -m fractal_server.app.runner._slurm.remote "
                    f"--input-file {input_pickle_file} "
                    f"--output-file {output_pickle_file}"
                )
            )

        # ...
        sbatch_script = self._prepare_sbatch_script(
            slurm_config=job.slurm_config,
            list_commands=cmdlines,
            slurm_out_path=str(job.slurm_stdout),
            slurm_err_path=str(job.slurm_stderr),
        )

        # Submit job via sbatch, and retrieve jobid

        # Write script content to a job.slurm_script
        with job.slurm_script.open("w") as f:
            f.write(sbatch_script)

        # Prepare submission command
        pre_command = f"sudo --non-interactive -u {self.slurm_user}"
        submit_command = f"sbatch --parsable {job.slurm_script}"
        full_command = f"{pre_command} {submit_command}"

        # Submit SLURM job and retrieve job ID
        try:
            output = subprocess.run(  # nosec
                shlex.split(full_command),
                capture_output=True,
                check=True,
                encoding="utf-8",
            )
        except subprocess.CalledProcessError as e:
            error_msg = (
                f"Submit command `{full_command}` failed. "
                f"Original error:\n{str(e)}"
            )
            logger.error(error_msg)
            raise JobExecutionError(info=error_msg)
        try:
            jobid = int(output.stdout)
        except ValueError as e:
            error_msg = (
                f"Submit command `{full_command}` returned "
                f"`{output.stdout=}` which cannot be cast to an integer "
                f"SLURM-job ID. Original error:\n{str(e)}"
            )
            logger.error(error_msg)
            raise JobExecutionError(info=error_msg)
        jobid_str = str(jobid)

        # Plug SLURM job id in stdout/stderr file paths
        job.slurm_stdout = Path(
            job.slurm_stdout.as_posix().replace("%j", jobid_str)
        )
        job.slurm_stderr = Path(
            job.slurm_stderr.as_posix().replace("%j", jobid_str)
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
            ]
        )
        script_lines = slurm_config.sort_script_lines(script_lines)
        logger.debug(script_lines)

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
        import random

        task_files = TaskFiles(
            workflow_dir=self.working_dir,
            workflow_dir_user=self.working_dir_user,
            task_order=random.randint(10000, 99999),  # nosec
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
                fut, job = fut_and_job[:]
                self.map_jobid_to_slurm_files.pop(jobid)
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
            pre_command = f"sudo --non-interactive -u {self.slurm_user}"
            submit_command = f"scancel {scancel_string}"
            full_command = f"{pre_command} {submit_command}"
            logger.debug(f"Now execute `{full_command}`")
            try:
                subprocess.run(  # nosec
                    shlex.split(full_command),
                    capture_output=True,
                    check=True,
                    encoding="utf-8",
                )
            except subprocess.CalledProcessError as e:
                error_msg = (
                    f"Cancel command `{full_command}` failed. "
                    f"Original error:\n{str(e)}"
                )
                logger.error(error_msg)
                raise JobExecutionError(info=error_msg)

        logger.debug("Executor shutdown: end")
