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
from cfut import SlurmExecutor  # type: ignore
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject
from ....utils import close_logger
from ....utils import set_logger


def get_slurm_script_dir() -> Path:
    settings = Inject(get_settings)
    script_dir = settings.RUNNER_ROOT_DIR  # type: ignore
    return script_dir  # type: ignore


def get_stdout_filename(arg: str = "%j") -> Path:
    return get_slurm_script_dir() / f"slurmpy.stdout.{arg}.log"


def get_in_filename(arg) -> Path:
    return get_slurm_script_dir() / f"cfut.in.{arg}.pickle"


def get_out_filename(arg) -> Path:
    return get_slurm_script_dir() / f"cfut.out.{arg}.pickle"


class SlurmJob:
    def __init__(self):
        self.workerid = random_string()


class FractalSlurmExecutor(SlurmExecutor):
    def __init__(
        self,
        username: Optional[str] = None,
        script_dir: Optional[Path] = None,
        *args,
        **kwargs,
    ):
        """
        Fractal slurm executor

        Args:
            username:
                shell username that runs the `sbatch` command
        """
        super().__init__(*args, **kwargs)
        self.username = username
        self.script_dir = script_dir

    def write_batch_script(
        self, sbatch_script: str, script_dir: Optional[Path] = None
    ) -> Path:
        """
        Write batch script

        Returns:
            batch_script_path:
                The path to the batch script
        """
        if not script_dir:
            script_dir = get_slurm_script_dir()

        batch_script_path = script_dir / f"_temp_{random_string()}.sh"
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
        filename = self.write_batch_script(
            sbatch_script=sbatch_script, script_dir=script_dir
        )
        submit_command = f"sbatch --parsable {filename}"
        full_cmd = shlex.join(
            shlex.split(submit_pre_command) + shlex.split(submit_command)
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
                f'submit_command="{submit_command}" returned '
                f'"{output.stdout}", which cannot be cast to an integer '
                "job ID"
            )
            close_logger(logger)
            raise e
        filename.unlink()
        return int(jobid)

    def compose_sbatch_script(
        self,
        cmdline: List[str],
        # NOTE: In SLURM, `%j` is the placeholder for the job_id.
        outpat: Optional[Path] = None,
        additional_setup_lines=[],
    ) -> str:
        if outpat is None:
            outpat = get_stdout_filename()
        script_lines = [
            "#!/bin/sh",
            f"#SBATCH --output={outpat}",
            *additional_setup_lines,
            # Export the slurm script directory so that nodes can find the
            # pickled payload
            f"export CFUT_DIR={get_slurm_script_dir()}",
            shlex.join(["srun", *cmdline]),
        ]
        return "\n".join(script_lines)

    def map(
        self,
        fn: Callable[..., Any],
        *iterables,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        additional_setup_lines: Optional[List[str]] = None,
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

        fs = [
            self.submit(
                fn, *args, additional_setup_lines=additional_setup_lines
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
        **kwargs,
    ):
        """Submit a job to the pool.
        If additional_setup_lines is passed, it overrides the lines given
        when creating the executor.
        """
        fut: futures.Future = futures.Future()

        # Start the job.
        job = SlurmJob()
        funcser = cloudpickle.dumps((fun, args, kwargs))
        with get_in_filename(job.workerid).open("wb") as f:
            f.write(funcser)
        jobid = self._start(job.workerid, additional_setup_lines)

        if self.debug:
            print("job submitted: %i" % jobid, file=sys.stderr)

        # Thread will wait for it to finish.
        self.wait_thread.wait(get_out_filename(job.workerid).as_posix(), jobid)

        with self.jobs_lock:
            self.jobs[jobid] = (fut, job)
        return fut

    def _completion(self, jobid):
        """Called whenever a job finishes."""
        with self.jobs_lock:
            fut, job = self.jobs.pop(jobid)
            if not self.jobs:
                self.jobs_empty_cond.notify_all()
        if self.debug:
            print("job completed: %i" % jobid, file=sys.stderr)

        out_path = get_out_filename(job.workerid)
        in_path = get_in_filename(job.workerid)

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

    def _start(self, workerid, additional_setup_lines):
        if additional_setup_lines is None:
            additional_setup_lines = self.additional_setup_lines

        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.SLURM_PYTHON_WORKER_INTERPRETER or sys.executable
        )

        sbatch_script = self.compose_sbatch_script(
            cmdline=shlex.split(
                f"{python_worker_interpreter} -m cfut.remote {workerid}"
            ),
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
