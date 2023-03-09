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
import shlex
import subprocess  # nosec
import sys
import time
from concurrent import futures
from pathlib import Path
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple

import cloudpickle
from cfut import SlurmExecutor
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject
from ....utils import close_logger
from ....utils import set_logger
from ..common import JobExecutionError
from ..common import TaskExecutionError
from ._subprocess_run_as_user import _glob_as_user
from ._subprocess_run_as_user import _path_exists_as_user
from ._subprocess_run_as_user import _run_command_as_user
from .wait_thread import FractalSlurmWaitThread
from fractal_server import __VERSION__


class SlurmJob:
    """
    Collect a few relevant information related to a FractalSlurmExecutor job

    Attributes:
        file_prefix: Prefix for all files handled by FractalSlurmExecutor.
        workerid: Random string that enters the input/output pickle-file names.
        input_pickle_file: Input pickle file.
        output_pickle_file: Output pickle file.
        slurm_script: Script to be submitted via `sbatch` command.
        stdout: SLURM stdout file.
        stderr: SLURM stderr file.
    """

    workerid: str
    file_prefix: str
    input_pickle_file: Path
    output_pickle_file: Path
    slurm_script: Path
    stdout: Path
    stderr: Path

    def __init__(self, file_prefix: Optional[str] = None):
        self.workerid = random_string()
        self.file_prefix = file_prefix or "default_prefix"


class FractalSlurmExecutor(SlurmExecutor):
    """
    FractalSlurmExecutor (inherits from cfut.SlurmExecutor)

    Attributes:
        slurm_user:
            shell username that runs the `sbatch` command
        common_script_lines:
            arbitrary script lines that will always be included in the
            sbatch script
        working_dir:
            directory for both the cfut/SLURM and fractal-server files and logs
        working_dir_user:
            directory for both the cfut/SLURM and fractal-server files and logs
        map_jobid_to_slurm_files:
            dictionary with paths of slurm-related files for active jobs
    """

    wait_thread_cls = FractalSlurmWaitThread

    def __init__(
        self,
        slurm_user: str,
        working_dir: Optional[Path] = None,
        working_dir_user: Optional[Path] = None,
        common_script_lines: Optional[List[str]] = None,
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
        self.working_dir: Path = working_dir  # type: ignore
        if not working_dir_user:
            if self.slurm_user:
                raise RuntimeError(f"{self.slurm_user=}, {working_dir_user=}")
            else:
                working_dir_user = working_dir
        if not _path_exists_as_user(
            path=str(working_dir_user), user=self.slurm_user
        ):
            logging.info(f"Missing folder {working_dir_user=}")

        self.working_dir_user: Path = working_dir_user  # type: ignore
        self.map_jobid_to_slurm_files: dict = {}

        # Set the attribute slurm_poll_interval for self.wait_thread (see
        # cfut.SlurmWaitThread)
        if not slurm_poll_interval:
            settings = Inject(get_settings)
            slurm_poll_interval = settings.FRACTAL_SLURM_POLL_INTERVAL
        self.wait_thread.slurm_poll_interval = slurm_poll_interval
        self.wait_thread.slurm_user = slurm_user

    def _cleanup(self, jobid: str):
        """
        Given a job ID as returned by _start, perform any necessary
        cleanup after the job has finished.
        """
        self.map_jobid_to_slurm_files.pop(jobid)

    def get_input_pickle_file_path(
        self, arg: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "cfut"
        return self.working_dir / f"{prefix}.in.{arg}.pickle"

    def get_output_pickle_file_path(
        self, arg: str, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "cfut"
        return self.working_dir_user / f"{prefix}.out.{arg}.pickle"

    def get_slurm_script_file_path(
        self, arg: Optional[str] = None, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "_temp"
        arg = arg or "submit"
        return self.working_dir / f"{prefix}.slurm.{arg}.sbatch"

    def get_slurm_stdout_file_path(
        self, arg: str = "%j", prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "slurmpy.stdout"
        return self.working_dir_user / f"{prefix}.slurm.{arg}.out"

    def get_slurm_stderr_file_path(
        self, arg: str = "%j", prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "slurmpy.stderr"
        return self.working_dir_user / f"{prefix}.slurm.{arg}.err"

    def write_batch_script(self, sbatch_script: str, dest: Path) -> Path:
        """
        Write batch script

        Returns:
            sbatch_script:
                The content of the batch script
            dest:
                The path to the batch script
        """
        with open(dest, "w") as f:
            f.write(sbatch_script)
        return dest

    def submit_sbatch(
        self,
        sbatch_script: str,
        submit_pre_command: str = "",
        script_path: Optional[Path] = None,
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
        script_path = script_path or self.get_slurm_script_file_path(
            random_string()
        )
        self.write_batch_script(sbatch_script=sbatch_script, dest=script_path)
        submit_command = f"sbatch --parsable {script_path}"
        full_cmd = shlex.split(submit_pre_command) + shlex.split(
            submit_command
        )
        try:
            output = subprocess.run(  # nosec
                full_cmd, capture_output=True, check=True
            )
        except subprocess.CalledProcessError as e:
            logger = set_logger(logger_name="slurm_runner")
            logger.error(e.stderr.decode("utf-8"))
            close_logger(logger)
            raise e
        try:
            jobid = int(output.stdout)
        except ValueError as e:
            logger = set_logger(logger_name="slurm_runner")
            logger.error(
                f"Submit command `{submit_command}` returned "
                f"`{output.stdout.decode('utf-8')}`, which cannot be cast "
                "to an integer job ID."
            )
            close_logger(logger)
            raise e
        return str(jobid)

    def compose_sbatch_script(
        self,
        cmdline: List[str],
        # NOTE: In SLURM, `%j` is the placeholder for the job ID.
        outpath: Optional[Path] = None,
        errpath: Optional[Path] = None,
        additional_setup_lines=None,
    ) -> str:
        additional_setup_lines = additional_setup_lines or []
        slurm_stdout_file = outpath or self.get_slurm_stdout_file_path()
        slurm_stderr_file = errpath or self.get_slurm_stderr_file_path()

        sbatch_lines = [
            f"#SBATCH --output={slurm_stdout_file}",
            f"#SBATCH --error={slurm_stderr_file}",
        ] + [
            ln
            for ln in additional_setup_lines + self.common_script_lines
            if ln.startswith("#SBATCH")
        ]

        non_sbatch_lines = [
            ln
            for ln in additional_setup_lines + self.common_script_lines
            if not ln.startswith("#SBATCH")
        ]

        cmd = [shlex.join(["srun", *cmdline])]

        script_lines = ["#!/bin/sh"] + sbatch_lines + non_sbatch_lines + cmd
        return "\n".join(script_lines) + "\n"

    def map(
        self,
        fn: Callable[..., Any],
        *iterables,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        additional_setup_lines: Optional[List[str]] = None,
        job_file_prefix: Optional[str] = None,
    ):
        """
        Returns an iterator equivalent to map(fn, iter), passing
        parameters to submit

        Overrides the PSL's `concurrent.futures.Executor.map` so that extra
        parameters can be passed to `Executor.submit`.

        This function is copied from PSL==3.11

        Original Copyright 2009 Brian Quinlan. All Rights Reserved.
        Licensed to PSF under a Contributor Agreement.
        """
        import time

        def _result_or_cancel(fut, timeout=None):
            """
            This function is copied from PSL ==3.11

            Copyright 2009 Brian Quinlan. All Rights Reserved.
            Licensed to PSF under a Contributor Agreement.
            """
            try:
                try:
                    return fut.result(timeout)
                finally:
                    fut.cancel()
            finally:
                # Break a reference cycle with the exception in
                # self._exception
                del fut

        if timeout is not None:
            end_time = timeout + time.monotonic()

        def sanitize_string(s):
            return s.replace(" ", "_").replace("/", "_").replace(".", "_")

        if job_file_prefix:
            job_file_fmt = job_file_prefix + "_par_{args}"
        else:
            job_file_fmt = f"_temp_{random_string()}"

        fs = [
            self.submit(
                fn,
                *args,
                additional_setup_lines=additional_setup_lines,
                job_file_prefix=job_file_fmt.format(
                    args=sanitize_string(str(args[0])),
                ),
            )
            for args in zip(*iterables)
        ]

        # Yield must be hidden in closure so that the futures are submitted
        # before the first iterator value is required.
        def result_iterator():
            try:
                # reverse to keep finishing order
                fs.reverse()
                while fs:
                    # Careful not to keep a reference to the popped future
                    if timeout is None:
                        yield _result_or_cancel(fs.pop())
                    else:
                        yield _result_or_cancel(
                            fs.pop(), end_time - time.monotonic()
                        )
            finally:
                for future in fs:
                    future.cancel()

        return result_iterator()

    def submit(
        self,
        fun: Callable[..., Any],
        *args,
        additional_setup_lines: Optional[List[str]] = None,
        job_file_prefix: Optional[str] = None,
        **kwargs,
    ) -> futures.Future:
        """
        Submit a job to the pool.

        If additional_setup_lines is passed, it overrides the lines given
        when creating the executor.
        """
        fut: futures.Future = futures.Future()

        # Define slurm-job-related files
        job = SlurmJob(file_prefix=job_file_prefix)
        job.input_pickle_file = self.get_input_pickle_file_path(
            job.workerid, prefix=job.file_prefix
        )
        job.output_pickle_file = self.get_output_pickle_file_path(
            job.workerid, prefix=job.file_prefix
        )
        job.slurm_script = self.get_slurm_script_file_path(
            prefix=job.file_prefix
        )
        job.stdout = self.get_slurm_stdout_file_path(prefix=job.file_prefix)
        job.stderr = self.get_slurm_stderr_file_path(prefix=job.file_prefix)

        # Dump serialized versions+function+args+kwargs to pickle file
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        funcser = cloudpickle.dumps((versions, fun, args, kwargs))
        with open(job.input_pickle_file, "wb") as f:
            f.write(funcser)

        # Submit job to SLURM, and get jobid
        jobid, job = self._start(job, additional_setup_lines)

        # Add the SLURM script/out/err paths to map_jobid_to_slurm_files (this
        # must be after self._start(job), so that "%j" has already been
        # replaced with the job ID)
        self.map_jobid_to_slurm_files[jobid] = (
            job.slurm_script.as_posix(),
            job.stdout.as_posix(),
            job.stderr.as_posix(),
        )

        # Thread will wait for it to finish.
        self.wait_thread.wait(job.output_pickle_file.as_posix(), jobid)

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

            # Input/output pickle files
            in_path = self.get_input_pickle_file_path(
                job.workerid, prefix=job.file_prefix
            )
            out_path = self.get_output_pickle_file_path(
                job.workerid, prefix=job.file_prefix
            )

            # Update the paths to use the files in self.working_dir (rather
            # than the user's ones in self.working_dir_user)
            out_path = self.working_dir / out_path.name
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

            # The output pickle file may be missing because of some slow
            # filesystem operation; wait some time before considering it as
            # missing
            if not out_path.exists():
                settings = Inject(get_settings)
                time.sleep(settings.FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME)

            if out_path.exists():
                # Output pickle file exists
                with out_path.open("rb") as f:
                    outdata = f.read()
                # Note: output can be either the task result (typically a
                # dictionary) or an ExceptionProxy object; in the latter case,
                # the ExceptionProxy definition is also part of the pickle file
                # (thanks to cloudpickle.dumps).
                success, output = cloudpickle.loads(outdata)
                try:
                    if success:
                        fut.set_result(output)
                    else:
                        proxy = output
                        if proxy.exc_type_name == "JobExecutionError":
                            job_exc = self._prepare_JobExecutionError(
                                jobid, info=proxy.kwargs.get("info", None)
                            )
                            fut.set_exception(job_exc)
                        else:
                            # This branch catches both TaskExecutionError's
                            # (coming from the typical fractal-server execution
                            # of tasks, and with additional fractal-specific
                            # kwargs) or arbitrary exceptions (coming from a
                            # direct use of FractalSlurmExecutor, possibly
                            # outside fractal-server)
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
                    out_path.unlink()
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
            else:
                # Output pickle file is missing
                info = (
                    "Output pickle file of the FractalSlurmExecutor job not "
                    "found.\n"
                    f"Expected file path: {str(out_path)}.\n"
                    "Here are some possible reasons:\n"
                    "1. The SLURM job was scancel-ed, either by the user or "
                    "due to an error (e.g. an out-of-memory or timeout "
                    "error). Note that if the scancel took place before "
                    "the job started running, the SLURM out/err files will "
                    "be empty.\n"
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
                except futures.InvalidStateError:
                    logging.warning(
                        f"Future {fut} (SLURM job ID: {jobid}) was already"
                        " cancelled, exit from"
                        " FractalSlurmExecutor._completion."
                    )
                    in_path.unlink()
                    self._cleanup(jobid)
                    return

            # Clean up input pickle file
            in_path.unlink()
            self._cleanup(jobid)

        except Exception as e:
            try:
                fut.set_exception(e)
            except futures.InvalidStateError:
                logging.warning(
                    f"Future {fut} (SLURM job ID: {jobid}) was already"
                    " cancelled, exit from"
                    " FractalSlurmExecutor._completion."
                )

    def _copy_files_from_user_to_server(self, job: SlurmJob):
        """
        Impersonate the user and copy task-related files

        For all files in `self.working_dir_user` that start with
        `job.file_prefix`, read them (with `sudo -u` impersonation) and write
        them to `self.working_dir`.

        Arguments:
            job: `SlurmJob` object (needed for its `file_prefix` attribute)

        Raises:
            JobExecutionError: If a `cat` command fails.
        """
        logging.debug("Enter _copy_files_from_user_to_server")
        if self.working_dir_user == self.working_dir:
            return

        files_to_copy = _glob_as_user(
            folder=str(self.working_dir_user),
            user=self.slurm_user,
            startswith=job.file_prefix,
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
        logging.debug("Exit _copy_files_from_user_to_server")

    def _start(
        self, job: SlurmJob, additional_setup_lines: Optional[List[str]] = None
    ) -> Tuple[str, SlurmJob]:
        """
        Submit function for execution on a SLURM cluster
        """

        if additional_setup_lines is None:
            additional_setup_lines = self.additional_setup_lines

        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        # Prepare script to be submitted via sbatch
        sbatch_script = self.compose_sbatch_script(
            cmdline=shlex.split(
                f"{python_worker_interpreter}"
                " -m fractal_server.app.runner._slurm.remote "
                f"--input-file {job.input_pickle_file} "
                f"--output-file {job.output_pickle_file}"
            ),
            outpath=job.stdout,
            errpath=job.stderr,
            additional_setup_lines=additional_setup_lines,
        )

        # Submit job via sbatch, and retrieve jobid
        pre_cmd = ""
        if self.slurm_user:
            pre_cmd = f"sudo --non-interactive -u {self.slurm_user}"
        jobid = self.submit_sbatch(
            sbatch_script,
            submit_pre_command=pre_cmd,
            script_path=job.slurm_script,
        )

        # Plug SLURM id in stdout/stderr file paths
        job.stdout = Path(job.stdout.as_posix().replace("%j", jobid))
        job.stderr = Path(job.stderr.as_posix().replace("%j", jobid))

        return jobid, job
