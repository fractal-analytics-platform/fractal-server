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
from ..common import JobExecutionError
from ..common import TaskExecutionError
from ._batching_heuristics import heuristics
from ._slurm_config import get_default_slurm_config
from ._slurm_config import SlurmConfig
from ._subprocess_run_as_user import _glob_as_user
from ._subprocess_run_as_user import _path_exists_as_user
from ._subprocess_run_as_user import _run_command_as_user
from ._timer import tic
from .wait_thread import FractalSlurmWaitThread
from fractal_server import __VERSION__


class SlurmJob:
    """
    Collect information related to a FractalSlurmExecutor job

    # FIXME: review and clean up SlurmJob class

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
            TBD
        single_task_submission:
            This must be `True` for jobs submitted as part of the `submit`
            method, and `False` for jobs coming from the `map` method.
        slurm_file_prefix:
            Prefix for SLURM-job related files (submission script and SLURM
            stdout/stderr); this is needed because such files are created by
            `FractalSlurmExecutor`.
        wftask_file_prefix:
            Prefix for files that are created as part of the functions
            submitted for execution on the `FractalSlurmExecutor`; this
            attribute is needed as part of the
            `_copy_files_from_user_to_server` method, and also to construct the
            names of per-task input/output pickle files.
        slurm_script:
            Path of SLURM submission script.
        slurm_stdout:
            Path of SLURM stdout file; if this includes `"%j"`, then this
            string will be replaced by the SLURM job ID upon `sbatch`
            submission.
        slurm_stderr:
            Path of SLURM stderr file; see `slurm_stdout` concerning `"%j"`.
        workerids:
            IDs that enter in the per-task input/output pickle files.
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
    wftask_file_prefix: str
    slurm_script: Path
    slurm_stdout: Path
    slurm_stderr: Path
    # Per-task attributes
    workerids: tuple[str]
    input_pickle_files: tuple[Path]
    output_pickle_files: tuple[Path]
    # Slurm configuration
    slurm_config: SlurmConfig

    def __init__(
        self,
        num_tasks_tot: int,
        slurm_config: SlurmConfig,
        workflow_task_file_prefix: Optional[str] = None,
        slurm_file_prefix: Optional[str] = None,
        wftask_file_prefix: Optional[str] = None,
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
        self.wftask_file_prefix = wftask_file_prefix or "default_wftask_prefix"
        self.workerids = tuple(
            random_string() for i in range(self.num_tasks_tot)
        )
        self.slurm_config = slurm_config

    def get_clean_output_pickle_files(self) -> tuple[str]:
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

    def __init__(
        self,
        slurm_user: str,
        working_dir: Optional[Path] = None,
        working_dir_user: Optional[Path] = None,
        common_script_lines: Optional[list[str]] = None,
        slurm_poll_interval: Optional[int] = None,
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

    def submit_sbatch(
        self,
        *,
        sbatch_script: str,
        script_path: Path,
        submit_pre_command: str = "",
    ) -> str:
        """
        Submit a Slurm job script

        Write the batch script in a temporary file and submit it with `sbatch`.

        Args:
            sbatch_script:
                the string representing the full job
            submit_pre_command:
                command that is prefixed to `sbatch`

        Returns:
            jobid:
                integer job id as returned by `sbatch` submission
        """
        logging.warning(sbatch_script)

        # Write script content to a file and prepare submission command
        with open(script_path, "w") as f:
            f.write(sbatch_script)
        submit_command = f"sbatch --parsable {script_path}"
        full_cmd = shlex.split(submit_pre_command) + shlex.split(
            submit_command
        )

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
        return str(jobid)

    def map(
        self,
        fn: Callable[..., Any],
        iterable: Iterable[list[Any]],
        *,
        slurm_config: Optional[SlurmConfig] = None,
        wftask_order: Optional[str] = None,
        wftask_file_prefix: Optional[str] = None,
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
                A `SlurmConfig` object; if `None`, `get_default_slurm_config()`
                will be used.
            wftask_order:
                FIXME
            wftask_file_prefix:
                FIXME

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

        # If slurm_config was not provided (e.g. when FractalSlurmExecutor is
        # used as a standalone executor, that is, outside fractal-server), use
        # a default one
        if not slurm_config:
            slurm_config = get_default_slurm_config()

        # Include common_script_lines in extra_lines
        logging.warning(
            f"Adding {self.common_script_lines=} to "
            f"{slurm_config.extra_lines=}, from map method."
        )
        current_extra_lines = slurm_config.extra_lines or []
        slurm_config.extra_lines = (
            current_extra_lines + self.common_script_lines
        )

        # Set file prefixes
        if wftask_file_prefix is None:
            wftask_file_prefix = f"_wftask_{random_string()}"
        if wftask_order is not None:
            general_slurm_file_prefix = str(wftask_order)
        else:
            general_slurm_file_prefix = f"_{random_string()}"

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
        logging.warning(n_ftasks_per_script)
        logging.warning(n_parallel_ftasks_per_script)

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
                f"{general_slurm_file_prefix}_" f"batch_{ind_batch}"
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
                    wftask_file_prefix=wftask_file_prefix,
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
        wftask_file_prefix: str,
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
            wftask_file_prefix=wftask_file_prefix,
            num_tasks_tot=num_tasks_tot,
            slurm_config=slurm_config,
        )
        if single_task_submission:
            if job.num_tasks_tot > 1:
                raise ValueError(
                    "{single_task_submission=} but {job.num_tasks_tot=}"
                )
            job.single_task_submission = 1

        # If available, set a more granular prefix for each parallel component
        if component_indices is not None:
            prefixes = [
                f"{job.wftask_file_prefix}_{component_indices[i]}"
                for i in range(num_tasks_tot)
            ]
        else:
            prefixes = [f"{job.wftask_file_prefix}"] * num_tasks_tot

        # Define I/O pickle file names/paths
        job.input_pickle_files = tuple(
            self.get_input_pickle_file_path(
                job.workerids[ind],
                prefix=prefixes[ind],
            )
            for ind in range(job.num_tasks_tot)
        )
        job.output_pickle_files = tuple(
            self.get_output_pickle_file_path(
                job.workerids[ind],
                prefix=prefixes[ind],
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
        logging.warning(list_list_args)
        for ind_task, args_list in enumerate(list_list_args):
            logging.warning(args_list)
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
        self.wait_thread.wait(job.get_clean_output_pickle_files(), jobid)

        with self.jobs_lock:
            self.jobs[jobid] = (fut, job)
        return fut

    def submit(
        self,
        fun: Callable[..., Any],
        *fun_args,
        slurm_config: Optional[SlurmConfig] = None,
        wftask_file_prefix: Optional[str] = None,
        wftask_order: Optional[str] = None,
        **fun_kwargs,
    ) -> futures.Future:
        """
        Submit a job to the pool.

        If additional_setup_lines is passed, it overrides the lines given
        when creating the executor. FIXME: this is now possible via
        slurm_config, is it?
        """

        if wftask_file_prefix is None:
            wftask_file_prefix = f"_wftask_{random_string()}"
        if wftask_order is not None:
            slurm_file_prefix = str(wftask_order)
        else:
            slurm_file_prefix = f"_{random_string()}"

        if not slurm_config:
            slurm_config = get_default_slurm_config()

        # Include common_script_lines in extra_lines
        logging.warning(
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
            wftask_file_prefix=wftask_file_prefix,
            component_indices=None,
            single_task_submission=True,
        )
        logging.warning(fut)
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
        toc = tic("_prepare_JobExecutionError")
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
        toc()
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

        logging.warning(job)

        # Handle all uncaught exceptions in this broad try/except block
        try:

            # Copy all relevant files from self.working_dir_user to
            # self.working_dir

            toc = tic(f"_completion/_copy_files_from_user_to_server({job})")
            self._copy_files_from_user_to_server(job)
            toc()

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

                logging.warning(out_path)

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
                logging.warning(cloudpickle.loads(outdata))
                toc = tic("cloudpickle.loads")
                success, output = cloudpickle.loads(outdata)
                toc()
                try:
                    if success:
                        outputs.append(output)
                    else:
                        proxy = output
                        logging.warning(proxy)
                        logging.warning(vars(proxy))
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
                    toc = tic("out_path.unlink")
                    out_path.unlink()
                    toc()
                except futures.InvalidStateError:
                    logging.warning(
                        f"Future {fut} (SLURM job ID: {jobid}) was already"
                        " cancelled, exit from"
                        " FractalSlurmExecutor._completion."
                    )
                    out_path.unlink()
                    in_path.unlink()
                    self._cleanup(jobid)
                    return

                # Clean up input pickle file
                toc = tic("in_path.unlink")
                in_path.unlink()
                toc()
            toc = tic(f"_cleanup({jobid})")
            self._cleanup(jobid)
            toc()
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

        Arguments:
            job: `SlurmJob` object (needed for its
                 `file_prefix` attribute)

        Raises:
            JobExecutionError: If a `cat` command fails.
        """
        logging.debug("Enter _copy_files_from_user_to_server")
        if self.working_dir_user == self.working_dir:
            return

        logging.warning(
            f"[_copy_files_from_user_to_server] {job.slurm_file_prefix=}"
        )
        logging.warning(
            f"[_copy_files_from_user_to_server] {job.wftask_file_prefix=}"
        )
        logging.warning(
            "[_copy_files_from_user_to_server] "
            f"{str(self.working_dir_user)=}"
        )

        toc = tic(
            "_glob_as_user("
            f"folder={str(self.working_dir_user)}, "
            f"user={self.slurm_user}, "
            f"startswith={job.wftask_file_prefix})"
        )
        wftask_files_to_copy = _glob_as_user(
            folder=str(self.working_dir_user),
            user=self.slurm_user,
            startswith=job.wftask_file_prefix,
        )
        toc()
        logging.warning(
            f"[_copy_files_from_user_to_server] {len(wftask_files_to_copy)=}"
        )
        toc = tic(
            "_glob_as_user("
            f"folder={str(self.working_dir_user)}, "
            f"user={self.slurm_user}, "
            f"startswith={job.slurm_file_prefix})"
        )
        slurm_files_to_copy = _glob_as_user(
            folder=str(self.working_dir_user),
            user=self.slurm_user,
            startswith=job.slurm_file_prefix,
        )
        toc()
        logging.warning(
            f"[_copy_files_from_user_to_server] {len(slurm_files_to_copy)=}"
        )
        files_to_copy = set(slurm_files_to_copy + wftask_files_to_copy)

        logging.warning(
            f"[_copy_files_from_user_to_server] XXX {len(files_to_copy)=}"
        )

        # NOTE: By setting encoding=None, we read/write bytes instead of
        # strings. This is needed to also handle pickle files
        for source_file_name in files_to_copy:
            source_file_path = str(self.working_dir_user / source_file_name)

            if not _path_exists_as_user(
                path=source_file_path, user=self.slurm_user
            ):
                raise RuntimeError(
                    f"Trying to `cat` missing path {source_file_path}"
                )

            # Read source_file_path (requires sudo)
            cmd = f"cat {source_file_path}"
            toc = tic(f"_run_command_as_user(cmd={cmd}")
            res = _run_command_as_user(
                cmd=cmd, user=self.slurm_user, encoding=None
            )
            toc()
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
            toc = tic(f"Write to {dest_file_path}")
            with open(dest_file_path, "wb") as f:
                f.write(res.stdout)
            toc()
        logging.debug("Exit _copy_files_from_user_to_server")

    def _start_multitask(
        self,
        job: SlurmJob,
    ) -> tuple[str, SlurmJob]:
        """
        Submit function for execution on a SLURM cluster
        """

        logging.warning(job)

        # Prepare commands to be included in SLURM submission script
        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        cmdlines = []
        logging.warning(vars(job))
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
        toc = tic("compose_sbatch_script_multitask")
        sbatch_script = self.compose_sbatch_script_multitask(
            slurm_config=job.slurm_config,
            list_commands=cmdlines,
            slurm_out_path=str(job.slurm_stdout),
            slurm_err_path=str(job.slurm_stderr),
        )
        toc()

        # Submit job via sbatch, and retrieve jobid
        pre_cmd = f"sudo --non-interactive -u {self.slurm_user}"
        toc = tic("submit_sbatch")
        jobid = self.submit_sbatch(
            script_path=job.slurm_script,
            sbatch_script=sbatch_script,
            submit_pre_command=pre_cmd,
        )
        toc()

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
        logging.warning(script_lines)

        # Complete script preamble
        script_lines.append("\n")

        # Include command lines
        tmp_list_commands = copy(list_commands)
        while tmp_list_commands:
            if tmp_list_commands:
                cmd = tmp_list_commands.pop(0)  # take first element
                logging.warning(cmd)
                script_lines.append(
                    "srun --ntasks=1 --cpus-per-task=$SLURM_CPUS_PER_TASK "
                    f"--mem={mem_per_task_MB}MB "
                    f"{cmd} &"
                )
        script_lines.append("wait\n")

        script = "\n".join(script_lines)
        logging.warning(f"\n{script}\n")
        return script
