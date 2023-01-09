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

import cloudpickle
from cfut import SlurmExecutor
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject
from ....utils import close_logger
from ....utils import file_opener
from ....utils import set_logger
from ..common import JobExecutionError
from ..common import TaskExecutionError


class SlurmJob:
    workerid: str

    slurm_input: Path
    slurm_output: Path
    slurm_script: Path

    stdout: Path
    stderr: Path

    def __init__(self):
        self.workerid = random_string()


class FractalSlurmExecutor(SlurmExecutor):
    """
    FractalSlurmExecutor (inherits from cfut.SlurmExecutor)

    Attributes:
        slurm_user:
            shell username that runs the `sbatch` command
        common_script_lines:
            arbitrary script lines that will always be included in the
            sbatch script
        script_dir:
            directory for both the cfut/SLURM and fractal-server files and logs
        map_jobid_to_slurm_files:
            dictionary with paths of slurm out/err files for active jobs
    """

    def __init__(
        self,
        slurm_user: Optional[str] = None,
        script_dir: Optional[Path] = None,
        common_script_lines: Optional[List[str]] = None,
        slurm_poll_interval: Optional[int] = None,
        *args,
        **kwargs,
    ):
        """
        Init method for FractalSlurmExecutor
        """

        super().__init__(*args, **kwargs)

        self.slurm_user = slurm_user
        self.common_script_lines = common_script_lines or []
        if not script_dir:
            settings = Inject(get_settings)
            script_dir = settings.FRACTAL_RUNNER_WORKING_BASE_DIR
        self.script_dir: Path = script_dir  # type: ignore
        self.map_jobid_to_slurm_files: dict = {}

        # Set the attribute slurm_poll_interval for self.wait_thread (see
        # cfut.SlurmWaitThread)
        if not slurm_poll_interval:
            settings = Inject(get_settings)
            slurm_poll_interval = settings.FRACTAL_SLURM_POLL_INTERVAL
        self.wait_thread.slurm_poll_interval = slurm_poll_interval

    def _cleanup(self, jobid):
        """
        Given a job ID as returned by _start, perform any necessary
        cleanup after the job has finished.
        """
        self.map_jobid_to_slurm_files.pop(jobid)

    def get_stdout_filename(
        self, arg: str = "%j", prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "slurmpy.stdout"
        return self.script_dir / f"{prefix}.slurm.{arg}.out"

    def get_stderr_filename(
        self, arg: str = "%j", prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "slurmpy.stdout"
        return self.script_dir / f"{prefix}.slurm.{arg}.err"

    def get_in_filename(self, arg: str, prefix: Optional[str] = None) -> Path:
        prefix = prefix or "cfut"
        return self.script_dir / f"{prefix}.in.{arg}.pickle"

    def get_out_filename(self, arg: str, prefix: Optional[str] = None) -> Path:
        prefix = prefix or "cfut"
        return self.script_dir / f"{prefix}.out.{arg}.pickle"

    def get_slurm_script_filename(
        self, arg: Optional[str] = None, prefix: Optional[str] = None
    ) -> Path:
        prefix = prefix or "_temp"
        arg = arg or "submit"
        return self.script_dir / f"{prefix}.slurm.{arg}.sbatch"

    def write_batch_script(self, sbatch_script: str, dest: Path) -> Path:
        """
        Write batch script

        Returns:
            sbatch_script:
                The content of the batch script
            dest:
                The path to the batch script
        """
        with open(dest, "w", opener=file_opener) as f:
            f.write(sbatch_script)
        return dest

    def submit_sbatch(
        self,
        sbatch_script: str,
        submit_pre_command: str = "",
        script_path: Optional[Path] = None,
    ) -> int:
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
        script_path = script_path or self.get_slurm_script_filename(
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
        return int(jobid)

    def compose_sbatch_script(
        self,
        cmdline: List[str],
        # NOTE: In SLURM, `%j` is the placeholder for the job_id.
        outpath: Optional[Path] = None,
        errpath: Optional[Path] = None,
        additional_setup_lines=None,
    ) -> str:
        additional_setup_lines = additional_setup_lines or []
        outpath = outpath or self.get_stdout_filename()
        errpath = errpath or self.get_stderr_filename()

        sbatch_lines = [
            f"#SBATCH --output={outpath}",
            f"#SBATCH --error={errpath}",
        ] + [
            ln
            for ln in additional_setup_lines + self.common_script_lines
            if ln.startswith("#SBATCH")
        ]

        non_sbatch_lines = [
            ln
            for ln in additional_setup_lines + self.common_script_lines
            if not ln.startswith("#SBATCH")
        ] + [f"export CFUT_DIR={self.script_dir}"]

        cmd = [
            shlex.join(["srun", *cmdline]),
            f"chmod 777 {outpath.parent / '*'}",
        ]

        script_lines = ["#!/bin/sh"] + sbatch_lines + non_sbatch_lines + cmd
        return "\n".join(script_lines)

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
                    args=sanitize_string(args[0]),
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
        additional_setup_lines: List[str] = None,
        job_file_prefix: Optional[str] = None,
        **kwargs,
    ) -> futures.Future:
        """
        Submit a job to the pool.

        If additional_setup_lines is passed, it overrides the lines given
        when creating the executor.
        """
        fut: futures.Future = futures.Future()

        # Start the job.
        job = SlurmJob()
        job.slurm_input = self.get_in_filename(job.workerid)
        job.slurm_output = self.get_out_filename(job.workerid)
        job.slurm_script = self.get_slurm_script_filename(
            prefix=job_file_prefix
        )
        job.stdout = self.get_stdout_filename(prefix=job_file_prefix)
        job.stderr = self.get_stderr_filename(prefix=job_file_prefix)

        # Dump serialized function+args+kwargs to pickle file
        funcser = cloudpickle.dumps((fun, args, kwargs))
        with open(job.slurm_input, "wb", opener=file_opener) as f:
            f.write(funcser)

        # Submit job to SLURM, and get jobid
        jobid = self._start(job, additional_setup_lines)

        # Add the SLURM script/out/err paths to map_jobid_to_slurm_files,
        # after replacing the %j placeholder with jobid when needed
        slurm_script_file = job.slurm_script.as_posix()
        slurm_stdout_file = job.stdout.as_posix().replace("%j", str(jobid))
        slurm_stderr_file = job.stderr.as_posix().replace("%j", str(jobid))
        self.map_jobid_to_slurm_files[jobid] = (
            slurm_script_file,
            slurm_stdout_file,
            slurm_stderr_file,
        )

        # Thread will wait for it to finish.
        self.wait_thread.wait(job.slurm_output.as_posix(), jobid)

        with self.jobs_lock:
            self.jobs[jobid] = (fut, job)
        return fut

    def _completion(self, jobid):
        """
        Callback function to be executed whenever a job finishes.

        FIXME: update docstring

        Important: This function is executed on a different thread (the
        self.wait_thread one), so that its failures may not be captured by the
        main thread running the FractalSlurmExecutor. A reasonable way to
        handle an exception in this function (such that this exception is then
        raised in the main thread) is to use fut.set_exception().
        """

        with self.jobs_lock:
            fut, job = self.jobs.pop(jobid)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()

        in_path = self.get_in_filename(job.workerid)
        out_path = self.get_out_filename(job.workerid)

        # Handle all uncaught exceptions in this try/except block, in order to
        # use them in a fut.set_exception().
        try:
            # Proceed normally if the output pickle file exists, otherwise set
            # a JobExecutionError exception
            if out_path.exists():
                # Unpickle and load output pickle file
                with out_path.open("rb") as f:
                    outdata = f.read()
                success, result = cloudpickle.loads(outdata)
                # Update the future (with set_result or set_exception)
                if success:
                    fut.set_result(result)
                else:
                    exc = TaskExecutionError(
                        result.tb, *result.args, **result.kwargs
                    )
                    fut.set_exception(exc)
                # Remove output pickle file
                out_path.unlink()
            else:
                # Wait FRACTAL_SLURM_KILLWAIT_INTERVAL seconds before
                # proceeding, so that SLURM has time to complete the job
                # cancellation
                settings = Inject(get_settings)
                settings.FRACTAL_SLURM_KILLWAIT_INTERVAL
                time.sleep(settings.FRACTAL_SLURM_KILLWAIT_INTERVAL)

                # Extract SLURM file paths from map_jobid_to_slurm_files
                (
                    slurm_script_file,
                    slurm_stdout_file,
                    slurm_stderr_file,
                ) = self.map_jobid_to_slurm_files[jobid]

                exc = JobExecutionError(
                    cmd_file=slurm_script_file,
                    stdout_file=slurm_stdout_file,
                    stderr_file=slurm_stderr_file,
                )
                fut.set_exception(exc)

            # Clean up input pickle file
            in_path.unlink()

            self._cleanup(jobid)

        except Exception as e:
            fut.set_exception(e)

    def _start(
        self, job: SlurmJob, additional_setup_lines: Optional[List[str]] = None
    ) -> int:
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
                f"{job.slurm_input}"
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

        return jobid
