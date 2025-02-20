import os
import threading
import time
import traceback
from itertools import count

from ......logger import set_logger
from fractal_server.app.runner.exceptions import JobExecutionError

logger = set_logger(__name__)


class FractalSlurmSSHWaitThread(threading.Thread):
    """
    Thread that monitors a pool of SLURM jobs

    This class is a custom re-implementation of the waiting thread class from:

    > clusterfutures <https://github.com/sampsyo/clusterfutures>
    > Original Copyright
    > Copyright 2021 Adrian Sampson <asampson@cs.washington.edu>
    > License: MIT

    Attributes:
        shutdown_file:
        shutdown_callback:
        slurm_poll_interval:
        jobs_finished_callback:
        active_job_ids:
        shutdown:
        lock:
    """

    shutdown_file: str
    shutdown_callback: callable
    slurm_poll_interval = 30
    jobs_finished_callback: callable
    active_job_ids: list[str]
    shutdown: bool
    _lock: threading.Lock

    def __init__(self, callback: callable, interval=1):
        """
        Init method

        This method is executed on the main thread.
        """
        threading.Thread.__init__(self, daemon=True)
        self.callback = callback
        self.interval = interval
        self._lock = threading.Lock()
        self.shutdown = False
        self.active_job_ids = []

    def wait(self, *, job_id: str):
        """
        Add a a new job to the set of jobs being waited for.

        This method is executed on the main thread.
        """
        if self.shutdown:
            error_msg = "Cannot call `wait` method after executor shutdown."
            logger.warning(error_msg)
            raise JobExecutionError(info=error_msg)
        with self._lock:
            self.active_job_ids.append(job_id)

    def check_shutdown(self):
        """
        Check whether the shutdown file exists

        This method is executed on the waiting thread.
        """
        if os.path.exists(self.shutdown_file):
            logger.info(
                f"Detected executor-shutdown file {self.shutdown_file}"
            )
            self.shutdown = True

    def check_jobs(self):
        """
        Check whether some jobs are over, and call callback.

        This method is executed on the waiting thread.
        """
        try:
            if self.active_job_ids == []:
                return
            finished_jobs = self.jobs_finished_callback(self.active_job_ids)
            if finished_jobs == set(self.active_job_ids):
                self.callback(self.active_job_ids)
                self.active_job_ids = []

        except Exception:
            # If anything goes wrong, print an exception without re-raising
            traceback.print_exc()

    def run(self):
        """
        Run forever (until a shutdown takes place) and trigger callback

        This method is executed on the waiting thread.

        Note that `shutdown_callback` only takes care of cleaning up the
        FractalSlurmExecutor variables, and then the `return` here is enough
        to fully clean up the `FractalFileWaitThread` object.
        """

        # FIXME SSH: are those try/except below needed?

        skip = max(self.slurm_poll_interval // self.interval, 1)
        for ind in count():
            self.check_shutdown()
            if self.shutdown:
                try:
                    self.shutdown_callback()
                except Exception:  # nosec
                    pass
                return
            if ind % skip == 0:
                with self._lock:
                    try:
                        self.check_jobs()
                    except Exception:  # nosec
                        pass
            time.sleep(self.interval)
