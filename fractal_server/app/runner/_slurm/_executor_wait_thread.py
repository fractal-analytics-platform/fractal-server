import os
import time
import traceback
from itertools import count
from typing import Callable

from cfut import FileWaitThread
from cfut import slurm

from ....logger import set_logger
from ._subprocess_run_as_user import _multiple_paths_exist_as_user


logger = set_logger(__name__)


class FractalFileWaitThread(FileWaitThread):
    """
    Overrides the original clusterfutures.FileWaitThread, so that:

    1. Each jobid in the waiting list is associated to a tuple of filenames,
       rather than a single one.
    2. The file-existence check can be replaced with the custom
       `_does_file_exist` method. This also requires a `slurm_user` attribute.

    The function is copied from clusterfutures 0.5. Original Copyright: 2022
    Adrian Sampson, released under the MIT licence

    Note: in principle we could avoid the definition of
    `FractalFileWaitThread`, and pack all this code in
    `FractalSlurmWaitThread`.
    """

    slurm_user: str
    shutdown_file: str = None
    shutdown_callback: Callable

    def __init__(self, *args, **kwargs):
        """
        Changed from clusterfutures:
        * Additional attribute `slurm_user`
        * Additional attribute `shutdown_file`
        """

        super().__init__(*args, **kwargs)

    def wait(
        self,
        *,
        filenames: tuple[str, ...],
        jobid: str,
    ):
        """
        Add a a new job (filenames and callback value, that is, SLURM job ID)
        to the set of files being waited upon.

        Changed from clusterfutures:
        * Replaced `filename` with `filenames`
        """
        with self.lock:
            self.waiting[filenames] = jobid

    def check(self, i):
        """
        Do one check for completed jobs

        Note: the `i` parameter allows subclasses like `SlurmWaitThread` to do
        something on every Nth check.

        Changed from clusterfutures:
        * Check file exitence via `_path_exists_as_user` instead of using `os`.
        * For each item in `self.waiting`, check simultaneous existence of
          multiple files.
        """
        # Poll for each file.
        for filenames in list(self.waiting):
            all_files_exist = _multiple_paths_exist_as_user(
                paths=filenames, user=self.slurm_user
            )
            if all_files_exist:
                self.callback(self.waiting[filenames])
                del self.waiting[filenames]
        if self.shutdown_file and os.path.exists(self.shutdown_file):
            logger.info(
                f"Detected executor-shutdown file {str(self.shutdown_file)}"
            )
            self.shutdown = True

    def run(self):
        """
        Overrides the original clusterfutures.FileWaitThread.run, adding a call
        to self.shutdown_callback.

        Note that shutdown_callback only takes care of cleaning up the
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


    Changed from clusterfutures:
    * Rename `id_to_filename` to `id_to_filenames`
    """

    slurm_poll_interval = 30

    def check(self, i):
        super().check(i)
        if i % (self.slurm_poll_interval // self.interval) == 0:
            try:
                finished_jobs = slurm.jobs_finished(self.waiting.values())
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
