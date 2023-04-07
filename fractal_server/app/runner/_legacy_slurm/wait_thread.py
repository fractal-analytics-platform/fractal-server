import logging
import traceback

from cfut import FileWaitThread
from cfut import slurm

from ._subprocess_run_as_user import _path_exists_as_user


class FractalFileWaitThread(FileWaitThread):
    """
    Overrides the original clusterfutures.FileWaitThread, so that the
    file-existence check can be replaced with the custom `_does_file_exist`
    method.

    The function is copied from clusterfutures 0.5. Original Copyright: 2022
    Adrian Sampson, released under the MIT licence

    Note: in principle we could avoid the definition of
    `FractalFileWaitThread`, and pack all this code in
    `FractalSlurmWaitThread`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slurm_user: str

    def check(self, i):
        """
        Do one check for completed jobs

        The `i` parameter allows subclasses like `SlurmWaitThread` to do
        something on every Nth check.
        """
        # Poll for each file.
        for filename in list(self.waiting):
            if _path_exists_as_user(path=filename, user=self.slurm_user):
                logging.info(
                    f"[FractalFileWaitThread.check] {filename} exists"
                )
                self.callback(self.waiting[filename])
                del self.waiting[filename]


class FractalSlurmWaitThread(FractalFileWaitThread):
    """
    Replaces the original clusterfutures.SlurmWaitThread, to inherit from
    FractalFileWaitThread instead of FileWaitThread.

    The function is copied from clusterfutures 0.5. Original Copyright: 2022
    Adrian Sampson, released under the MIT licence
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

            id_to_filename = {v: k for (k, v) in self.waiting.items()}
            for finished_id in finished_jobs:
                self.callback(finished_id)
                self.waiting.pop(id_to_filename[finished_id])
