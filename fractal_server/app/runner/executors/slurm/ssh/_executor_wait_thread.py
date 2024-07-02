import os
import time
import traceback
from itertools import count
from typing import Callable

from cfut import FileWaitThread

from ......logger import set_logger

logger = set_logger(__name__)


class FractalSlurmWaitThread(FileWaitThread):
    """
    Overrides the original clusterfutures.FileWaitThread, so that:

    1. Each jobid in the waiting list is associated to a tuple of filenames,
       rather than a single one.
    2. In the `check` method, we avoid output-file existence checks (which
       would require `sudo -u user ls` calls), and we rather check for the
       existence of the shutdown file. All the logic to check whether a job is
       complete is deferred to the `cfut.slurm.jobs_finished` function.
    3. There are additional attributes (...).

    This class is based on clusterfutures 0.5. Original Copyright: 2022
    Adrian Sampson, released under the MIT licence
    """

    shutdown_file: str
    shutdown_callback: Callable
    jobs_finished_callback: Callable
    slurm_poll_interval = 30
    active_job_ids: list[str]

    def __init__(self, *args, **kwargs):
        """
        Init method

        This method is executed on the main thread.
        """
        super().__init__(*args, **kwargs)
        self.active_job_ids = []

    def wait(self, *, job_id: str):
        """
        Add a a new job to the set of jobs being waited for.

        This method is executed on the main thread.
        """
        with self.lock:
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
                with self.lock:
                    try:
                        self.check_jobs()
                    except Exception:  # nosec
                        pass
            time.sleep(self.interval)
