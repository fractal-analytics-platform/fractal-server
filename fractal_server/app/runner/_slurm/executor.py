# This adapts clusterfutures <https://github.com/sampsyo/clusterfutures>
# Original Copyright
# Copyright 2021 Adrian Sampson <asampson@cs.washington.edu>
# License: MIT
#
# Modified by:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
#
# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
import shlex
import subprocess  # nosec
import sys
from concurrent import futures
from pathlib import Path
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

import cloudpickle
from cfut import RemoteException
from cfut import SlurmExecutor
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject
from ....utils import close_logger
from ....utils import set_logger


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
    def __init__(
        self,
        username: Optional[str] = None,
        script_dir: Optional[Path] = None,
        common_script_lines: Optional[List[str]] = None,
        *args,
        **kwargs,
    ):
        """
        Fractal slurm executor

        Args:
            username:
                shell username that runs the `sbatch` command
            common_script_lines:
                arbitrary script lines that will always be included in the
                sbatch script
        """
        super().__init__(*args, **kwargs)
        self.username = username
        self.common_script_lines = common_script_lines or []
        if not script_dir:
            settings = Inject(get_settings)
            script_dir = settings.RUNNER_ROOT_DIR  # type: ignore
        self.script_dir: Path = script_dir  # type: ignore

    def get_stdout_filename(self, arg: str = "%j") -> Path:
        return self.script_dir / f"slurmpy.stdout.{arg}.out"

    def get_stderr_filename(self, arg: str = "%j") -> Path:
        return self.script_dir / f"slurmpy.stdout.{arg}.err"

    def get_in_filename(self, arg) -> Path:
        return self.script_dir / f"cfut.in.{arg}.pickle"

    def get_out_filename(self, arg) -> Path:
        return self.script_dir / f"cfut.out.{arg}.pickle"

    def get_slurm_script_filename(self, arg) -> Path:
        return self.script_dir / f"_temp_{arg}.sbatch"

    def write_batch_script(self, sbatch_script: str) -> Path:
        """
        Write batch script

        Returns:
            batch_script_path:
                The path to the batch script
        """
        batch_script_path = self.get_slurm_script_filename(random_string())
        with batch_script_path.open("w") as f:
            f.write(sbatch_script)
        return batch_script_path

    def submit_sbatch(
        self,
        sbatch_script: str,
        submit_pre_command: str = "",
        script_dir: Optional[Path] = None,
    ) -> int:
        """
        Submit a Slurm job script

        Write the batch script in a temporary file and submit it with `sbatch`.

        Args:
            sbatch_script:
                the string representing the full job
            submit_pre_command:
                command that is prefixed to `sbatch`
            script_dir:
                destination of temporary script files

        Returns:
            jobid:
                integer job id as returned by `sbatch` submission
        """
        filename = self.write_batch_script(sbatch_script=sbatch_script)
        submit_command = f"sbatch --parsable {filename}"
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
                f"Submit ommand `{submit_command}` returned "
                f"`{output.stdout.decode('utf-8')}`, which cannot be cast "
                "to an integer job ID."
            )
            close_logger(logger)
            raise e
        # TODO: unlink
        # filename.unlink()
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

        cmd = [shlex.join(["srun", *cmdline])]

        script_lines = ["#!/bin/sh"] + sbatch_lines + non_sbatch_lines + cmd
        return "\n".join(script_lines)

    def map(
        self,
        fn: Callable[..., Any],
        *iterables,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        additional_setup_lines: Optional[List[str]] = None,
        job_file_fmt: Optional[str] = None,
        # NOTE: the following parameters are here only for compatibility of
        # the interface, but should never be used.
        job_stderr=None,
        job_stdout=None,
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

        fs = [
            self.submit(
                fn,
                *args,
                additional_setup_lines=additional_setup_lines,
                job_stdout=job_file_fmt.format(
                    args=sanitize_string(args[0]), suffix=".out"
                ),
                job_stderr=job_file_fmt.format(
                    args=sanitize_string(args[0]), suffix=".err"
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
        job_stdout: Optional[Path] = None,
        job_stderr: Optional[Path] = None,
        job_file_fmt: Optional[str] = None,
        **kwargs,
    ):
        """Submit a job to the pool.
        If additional_setup_lines is passed, it overrides the lines given
        when creating the executor.
        """
        fut: futures.Future = futures.Future()

        # Start the job.
        job = SlurmJob()
        job.slurm_input = self.get_in_filename(job.workerid)
        job.slurm_output = self.get_out_filename(job.workerid)
        job.slurm_script = self.get_slurm_script_filename(random_string())

        job.stdout = job_stdout or self.get_stdout_filename()
        job.stderr = job_stderr or self.get_stderr_filename()

        funcser = cloudpickle.dumps((fun, args, kwargs))
        with job.slurm_input.open("wb") as f:
            f.write(funcser)
        jobid = self._start(job, additional_setup_lines)

        # Thread will wait for it to finish.
        self.wait_thread.wait(job.slurm_output.as_posix(), jobid)

        with self.jobs_lock:
            self.jobs[jobid] = (fut, job)
        return fut

    def _completion(self, jobid):
        """Called whenever a job finishes."""
        with self.jobs_lock:
            fut, job = self.jobs.pop(jobid)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()

        out_path = self.get_out_filename(job.workerid)
        in_path = self.get_in_filename(job.workerid)

        with out_path.open("rb") as f:
            outdata = f.read()
        success, result = cloudpickle.loads(outdata)

        if success:
            fut.set_result(result)
        else:
            fut.set_exception(RemoteException(result))

        # Clean up communication files.
        in_path.unlink()
        out_path.unlink()

        self._cleanup(jobid)

    def _start(
        self, job: SlurmJob, additional_setup_lines: Optional[List[str]] = None
    ):
        if additional_setup_lines is None:
            additional_setup_lines = self.additional_setup_lines

        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.SLURM_PYTHON_WORKER_INTERPRETER or sys.executable
        )

        sbatch_script = self.compose_sbatch_script(
            cmdline=shlex.split(
                f"{python_worker_interpreter} -m cfut.remote {job.workerid}"
            ),
            outpath=job.stdout,
            errpath=job.stderr,
            additional_setup_lines=additional_setup_lines,
        )

        pre_cmd = ""
        if self.username:
            pre_cmd = f"sudo --non-interactive -u {self.username}"

        job_id = self.submit_sbatch(
            sbatch_script,
            submit_pre_command=pre_cmd,
            script_dir=self.script_dir,
        )
        return job_id
