import os
import time
import traceback
from itertools import count
from typing import Callable
from typing import Optional

from cfut import FileWaitThread

from ......logger import set_logger
from ._check_jobs_status import _jobs_finished

logger = set_logger(__name__)


class FractalFileWaitThread(FileWaitThread):
    """
    Overrides the original clusterfutures.FileWaitThread, so that:

    1. Each jobid in the waiting list is associated to a tuple of filenames,
       rather than a single one.
    2. In the `check` method, we avoid output-file existence checks (which
       would require `sudo -u user ls` calls), and we rather check for the
       existence of the shutdown file. All the logic to check whether a job is
       complete is deferred to the `cfut.slurm.jobs_finished` function.
    3. There are additional attributes (`slurm_user`, `shutdown_file` and
       `shutdown_callback`).

    This class is copied from clusterfutures 0.5. Original Copyright: 2022
    Adrian Sampson, released under the MIT licence

    Note: in principle we could avoid the definition of
    `FractalFileWaitThread`, and pack all this code in
    `FractalSlurmWaitThread`.
    """

    slurm_user: str
    shutdown_file: Optional[str] = None
    shutdown_callback: Callable

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        with self.lock:
            self.waiting[filenames] = jobid

    def check(self, i):
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
            with self.lock:
                self.check(i)
            time.sleep(self.interval)


class FractalSlurmWaitThread(FractalFileWaitThread):
    """
    Replaces the original clusterfutures.SlurmWaitThread, to inherit from
    FractalFileWaitThread instead of FileWaitThread.

    The function is copied from clusterfutures 0.5. Original Copyright: 2022
    Adrian Sampson, released under the MIT licence

    **Note**: if `self.interval != 1` then this should be modified, but for
    `clusterfutures` v0.5 `self.interval` is indeed equal to `1`.

    Changed from clusterfutures:
    * Rename `id_to_filename` to `id_to_filenames`
    """

    slurm_poll_interval = 30

    def check(self, i):
        super().check(i)
        if i % (self.slurm_poll_interval // self.interval) == 0:
            try:
                finished_jobs = _jobs_finished(self.waiting.values())
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
