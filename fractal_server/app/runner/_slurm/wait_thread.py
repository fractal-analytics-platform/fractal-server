import logging
import traceback

from cfut import FileWaitThread
from cfut import slurm

from ._subprocess_run_as_user import _run_command_as_user


def _does_file_exist(filepath: str, *, slurm_user: str) -> bool:
    """
    FIXME
    """
    cmd = f"ls {filepath}"
    res = _run_command_as_user(cmd=cmd, user=slurm_user)
    if res.returncode == 0:
        return True
    else:
        return False


class FractalFileWaitThread(FileWaitThread):
    """
    FIXME
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.slurm_user: str = "asdasa"

    def check(self, i):
        """Do one check for completed jobs
        The i parameter allows subclasses like SlurmWaitThread to do something
        on every Nth check.
        """
        # Poll for each file.
        for filename in list(self.waiting):
            if _does_file_exist(filename, slurm_user=self.slurm_user):
                logging.info(
                    f"[FractalFileWaitThread.check] {filename} exists"
                )
                self.callback(self.waiting[filename])
                del self.waiting[filename]


class FractalSlurmWaitThread(FractalFileWaitThread):
    """
    FIXME: can we get rid of this and only have a single custom class?
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
