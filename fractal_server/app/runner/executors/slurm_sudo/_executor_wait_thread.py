import os
import threading
import time
import traceback
from itertools import count
from typing import Optional

from ._check_jobs_status import get_finished_jobs
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.logger import set_logger

logger = set_logger(__name__)


class FractalSlurmSudoWaitThread(threading.Thread):
    """
    Thread that monitors a pool of SLURM jobs

    This class is a custom re-implementation of the waiting thread class from:

    > clusterfutures <https://github.com/sampsyo/clusterfutures>
    > Original Copyright
    > Copyright 2021 Adrian Sampson <asampson@cs.washington.edu>
    > License: MIT

    Attributes:
        slurm_user:
        shutdown_file:
        shutdown_callback:
        slurm_poll_interval:
        waiting:
        shutdown:
        lock:
    """

    slurm_user: str
    shutdown_file: Optional[str] = None
    shutdown_callback: callable
    slurm_poll_interval: int = 30
    waiting: dict[tuple[str, ...], str]
    shutdown: bool
    _lock: threading.Lock

    def __init__(self, callback: callable, interval=1):
        threading.Thread.__init__(self, daemon=True)
        self.callback = callback
        self.interval = interval
        self.waiting = {}
        self._lock = threading.Lock()  # To protect the .waiting dict
        self.shutdown = False
        self.active_job_ids = []

    def wait(
        self,
        *,
        filenames: tuple[str, ...],
        jobid: str,
    ):
        """
        Add a a new job to the set of jobs being waited for.

        A job consists of a tuple of filenames and a callback value (i.e. a
        SLURM job ID).

        Note that (with respect to clusterfutures) we replaced `filename` with
        `filenames`.
        """
        if self.shutdown:
            error_msg = "Cannot call `wait` method after executor shutdown."
            logger.warning(error_msg)
            raise JobExecutionError(info=error_msg)
        with self._lock:
            self.waiting[filenames] = jobid

    def check_shutdown(self, i):
        """
        Do one shutdown-file-existence check.

        Note: the `i` parameter allows subclasses like `SlurmWaitThread` to do
        something on every Nth check.

        Changed from clusterfutures:
        * Do not check for output-pickle-file existence (we rather rely on
          `cfut.slurm.jobs_finished`);
        * Check for the existence of shutdown-file.
        """
        if self.shutdown_file and os.path.exists(self.shutdown_file):
            logger.info(
                f"Detected executor-shutdown file {str(self.shutdown_file)}"
            )
            self.shutdown = True

    def run(self):
        """
        Overrides the original clusterfutures.FileWaitThread.run, adding a call
        to self.shutdown_callback.

        Changed from clusterfutures:
        * We do not rely on output-file-existence checks to verify whether a
          job is complete.

        Note that `shutdown_callback` only takes care of cleaning up the
        FractalSlurmExecutor variables, and then the `return` here is enough to
        fully clean up the `FractalFileWaitThread` object.
        """
        for i in count():
            if self.shutdown:
                self.shutdown_callback()
                return
            with self._lock:
                self.check(i)
            time.sleep(self.interval)

    def check(self, i):
        self.check_shutdown(i)
        if i % (self.slurm_poll_interval // self.interval) == 0:
            try:
                finished_jobs = get_finished_jobs(self.waiting.values())
            except Exception:
                # Don't abandon completion checking if jobs_finished errors
                traceback.print_exc()
                return

            if not finished_jobs:
                return

            id_to_filenames = {v: k for (k, v) in self.waiting.items()}
            for finished_id in finished_jobs:
                self.callback(finished_id)
                self.waiting.pop(id_to_filenames[finished_id])
