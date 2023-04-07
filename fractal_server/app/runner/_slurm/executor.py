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
import logging
import math
import shlex
import subprocess  # nosec
import sys
import time
from concurrent import futures
from copy import copy
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Optional

import cloudpickle
from cfut import SlurmExecutor
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject
from .._common import get_task_file_paths
from .._common import TaskFiles
from ..common import JobExecutionError
from ..common import TaskExecutionError
from ._batching import heuristics
from ._executor_wait_thread import FractalSlurmWaitThread
from ._slurm_config import get_default_slurm_config
from ._submit_setup import SlurmConfig
from ._subprocess_run_as_user import _glob_as_user
from ._subprocess_run_as_user import _path_exists_as_user
from ._subprocess_run_as_user import _run_command_as_user
from fractal_server import __VERSION__


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
    common_script_lines: list[str]
    working_dir: Path
    working_dir_user: Path
    map_jobid_to_slurm_files: dict[str, tuple[str, str, str]]
    keep_pickle_files: bool

    def __init__(
        self,
        slurm_user: str,
        working_dir: Optional[Path] = None,
        working_dir_user: Optional[Path] = None,
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
        if not working_dir:
            settings = Inject(get_settings)
            working_dir = settings.FRACTAL_RUNNER_WORKING_BASE_DIR
        self.working_dir = working_dir
        if not working_dir_user:
            if self.slurm_user:
                raise RuntimeError(f"{self.slurm_user=}, {working_dir_user=}")
            else:
                working_dir_user = working_dir
        if not _path_exists_as_user(
            path=str(working_dir_user), user=self.slurm_user
        ):
            logging.info(f"Missing folder {working_dir_user=}")

        self.working_dir_user = working_dir_user
        self.map_jobid_to_slurm_files = {}

        # Set the attribute slurm_poll_interval for self.wait_thread (see
        # cfut.SlurmWaitThread)
        if not slurm_poll_interval:
            settings = Inject(get_settings)
            slurm_poll_interval = settings.FRACTAL_SLURM_POLL_INTERVAL
        self.wait_thread.slurm_poll_interval = slurm_poll_interval
        self.wait_thread.slurm_user = self.slurm_user

    def _cleanup(self, jobid: str) -> None:
        """
        Given a job ID as returned by _start, perform any necessary
        cleanup after the job has finished.
        """
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
        *fun_args,
        slurm_config: Optional[SlurmConfig] = None,
        task_files: Optional[TaskFiles] = None,
        **fun_kwargs,
    ) -> futures.Future:
        """
        Submit a job to the pool.
        """

        # Set defaults, if needed
        if slurm_config is None:
            slurm_config = get_default_slurm_config()
        if task_files is None:
            task_files = self.get_default_task_files()

        slurm_file_prefix = task_files.file_prefix

        # Include common_script_lines in extra_lines
        logging.debug(
            f"Adding {self.common_script_lines=} to "
            f"{slurm_config.extra_lines=}, from submit method."
        )
        current_extra_lines = slurm_config.extra_lines or []
        slurm_config.extra_lines = (
            current_extra_lines + self.common_script_lines
        )

        # Adapt slurm_config to the fact that this is a single-task SlurmJob
        # instance
        slurm_config.n_ftasks_per_script = 1
        slurm_config.n_parallel_ftasks_per_script = 1

        fut = self.submit_multitask(
            fun,
            list_list_args=[fun_args],
            list_list_kwargs=[fun_kwargs],
            slurm_config=slurm_config,
            slurm_file_prefix=slurm_file_prefix,
            task_files=task_files,
            component_indices=None,
            single_task_submission=True,
        )
        return fut

    def map(
        self,
        fn: Callable[..., Any],
        iterable: Iterable[list[Any]],
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
        1. Only `fn` and `iterable` can be assigned as positional arguments.
        2. `*iterables` argument eplaced with a single `iterable`
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
            An iterator of results, with the same number of elements as
            `iterable`.
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
        logging.debug(
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
        n_ftasks_tot = len(list_args)

        # Set/validate parameters for task batching
        n_ftasks_per_script, n_parallel_ftasks_per_script = heuristics(
            # Number of parallel componens (always known)
            n_ftasks_tot=len(list_args),
            # Optional WorkflowTask attributes:
            n_ftasks_per_script=slurm_config.n_ftasks_per_script,
            n_parallel_ftasks_per_script=slurm_config.n_parallel_ftasks_per_script,  # noqa
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
        slurm_config.n_parallel_ftasks_per_script = (
            n_parallel_ftasks_per_script
        )
        slurm_config.n_ftasks_per_script = n_ftasks_per_script

        # Divide arguments in batches of `n_tasks_per_script` tasks each
        args_batches = []
        batch_size = n_ftasks_per_script
        for ind_chunk in range(0, n_ftasks_tot, batch_size):
            args_batches.append(
                list_args[ind_chunk : ind_chunk + batch_size]  # noqa
            )
        if len(args_batches) != math.ceil(n_ftasks_tot / n_ftasks_per_script):
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
                self.submit_multitask(
                    fn,
                    list_list_args=[
                        [x] for x in batch
                    ],  # FIXME: clarify structure  # noqa
                    list_list_kwargs=[{} for x in batch],  # FIXME
                    slurm_config=slurm_config,
                    slurm_file_prefix=this_slurm_file_prefix,
                    task_files=task_files,
                    component_indices=[
                        current_component_index + _ind
                        for _ind in range(batch_size)
                    ],
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

    def submit_multitask(
        self,
        fun: Callable[..., Any],
        list_list_args: Iterable[Iterable],
        list_list_kwargs: Iterable[dict],
        slurm_file_prefix: str,
        task_files: TaskFiles,
        slurm_config: SlurmConfig,
        component_indices: Optional[list[int]] = None,
        single_task_submission: bool = False,
    ) -> futures.Future:
        """
        Submit a multi-task job to the pool, where each task is handled via the
        pickle/remote logic
        """
        fut: futures.Future = futures.Future()

        # Define slurm-job-related files
        num_tasks_tot = len(list_list_args)
        job = SlurmJob(
            slurm_file_prefix=slurm_file_prefix,
            num_tasks_tot=num_tasks_tot,
            slurm_config=slurm_config,
        )
        if single_task_submission:
            if job.num_tasks_tot > 1:
                raise ValueError(
                    "{single_task_submission=} but {job.num_tasks_tot=}"
                )
            job.single_task_submission = 1
            job.wftask_file_prefixes = (task_files.file_prefix,)
        else:
            logging.critical(f"{list_list_args=}")
            job.wftask_file_prefixes = tuple(
                get_task_file_paths(
                    workflow_dir=task_files.workflow_dir,
                    workflow_dir_user=task_files.workflow_dir_user,
                    task_order=task_files.task_order,
                    component=list_args[0],  # FIXME
                ).file_prefix
                for list_args in list_list_args
            )

        logging.critical(f"{job.wftask_file_prefixes=}")

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
        for ind_task, args_list in enumerate(list_list_args):
            kwargs_dict = list_list_kwargs[ind_task]
            funcser = cloudpickle.dumps(
                (versions, fun, args_list, kwargs_dict)
            )
            with open(job.input_pickle_files[ind_task], "wb") as f:
                f.write(funcser)

        # Submit job to SLURM, and get jobid
        jobid, job = self._start_multitask(job)

        # Add the SLURM script/out/err paths to map_jobid_to_slurm_files (this
        # must be after self._start(job), so that "%j" has already been
        # replaced with the job ID)
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

        with self.jobs_lock:
            fut, job = self.jobs.pop(jobid)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()

        # Handle all uncaught exceptions in this broad try/except block
        try:

            # Copy all relevant files from self.working_dir_user to
            # self.working_dir

            self._copy_files_from_user_to_server(job)

            # Update the paths to use the files in self.working_dir (rather
            # than the user's ones in self.working_dir_user)
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
                        "(e.g. due to an overloaded NFS filesystem). "
                        "Note that the server configuration has "
                        "FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME="
                        f"{settings.FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME} "
                        "seconds.\n"
                    )
                    job_exc = self._prepare_JobExecutionError(jobid, info=info)
                    try:
                        fut.set_exception(job_exc)
                        return
                    except futures.InvalidStateError:
                        logging.warning(
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
                except futures.InvalidStateError:
                    logging.warning(
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
            except futures.InvalidStateError:
                logging.warning(
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
        logging.debug("Enter _copy_files_from_user_to_server")
        if self.working_dir_user == self.working_dir:
            return

        prefixes = set(
            [job.slurm_file_prefix] + list(job.wftask_file_prefixes)
        )

        logging.debug(f"[_copy_files_from_user_to_server] {prefixes=}")
        logging.debug(
            f"[_copy_files_from_user_to_server] {str(self.working_dir_user)=}"
        )

        for prefix in prefixes:

            files_to_copy = _glob_as_user(
                folder=str(self.working_dir_user),
                user=self.slurm_user,
                startswith=prefix,
            )
            logging.debug(
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
                    logging.error(info)
                    raise JobExecutionError(info)
                # Write to dest_file_path (including empty files)
                dest_file_path = str(self.working_dir / source_file_name)
                with open(dest_file_path, "wb") as f:
                    f.write(res.stdout)
        logging.debug("[_copy_files_from_user_to_server] End")

    def _start_multitask(
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
        sbatch_script = self.compose_sbatch_script_multitask(
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
        full_cmd = shlex.split(f"{pre_command} {submit_command}")

        # Submit SLURM job and retrieve job ID
        try:
            output = subprocess.run(  # nosec
                full_cmd, capture_output=True, check=True
            )
        except subprocess.CalledProcessError as e:
            # FIXME: turn this error into a JobExecutionError
            logging.error(e.stderr.decode("utf-8"))
            raise e
        try:
            jobid = int(output.stdout)
        except ValueError as e:
            # FIXME: turn this error into a JobExecutionError
            logging.error(
                f"Submit command `{submit_command}` returned "
                f"`{output.stdout.decode('utf-8')}`, which cannot be cast "
                "to an integer job ID."
            )
            raise e
        jobid = str(jobid)

        # Plug SLURM job id in stdout/stderr file paths
        job.slurm_stdout = Path(
            job.slurm_stdout.as_posix().replace("%j", jobid)
        )
        job.slurm_stderr = Path(
            job.slurm_stderr.as_posix().replace("%j", jobid)
        )

        return jobid, job

    def compose_sbatch_script_multitask(  # FIXME: rename
        self,
        *,
        list_commands: list[str],
        slurm_out_path: str,
        slurm_err_path: str,
        slurm_config: SlurmConfig,
    ):

        num_tasks_max_running = slurm_config.n_parallel_ftasks_per_script
        mem_per_task_MB = slurm_config.mem_per_task_MB

        # Set ntasks
        ntasks = min(len(list_commands), num_tasks_max_running)
        if len(list_commands) < num_tasks_max_running:
            ntasks = len(list_commands)
            slurm_config.n_parallel_ftasks_per_script = ntasks
            logging.warning(
                f"{len(list_commands)=} is smaller than "
                f"{num_tasks_max_running=}. Setting {ntasks=}."
            )

        # Prepare SLURM preamble based on SlurmConfig object
        script_lines = slurm_config.to_sbatch_preamble()

        # Extend SLURM preamble with variable which are not in SlurmConfig, and
        # fix their order
        script_lines.extend(
            [
                f"#SBATCH --err={slurm_err_path}",
                f"#SBATCH --out={slurm_out_path}",
            ]
        )
        script_lines = slurm_config.sort_script_lines(script_lines)
        logging.debug(script_lines)

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
