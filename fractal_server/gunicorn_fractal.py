import logging
import os
import signal

from gunicorn.glogging import Logger as GunicornLogger
from uvicorn_worker import UvicornWorker

from fractal_server.utils import get_timestamp

logger = logging.getLogger("uvicorn.error")

# Server start time for the resume of orphaned jobs. Gunicorn imports this
# module in the master (via `--logger-class`), so all workers inherit the same
# value; without gunicorn, it is set on first import.
BOOT_TIMESTAMP_ENV_VAR = "FRACTAL_SERVER_BOOT_TIMESTAMP"
os.environ.setdefault(BOOT_TIMESTAMP_ENV_VAR, get_timestamp().isoformat())


class FractalGunicornLogger(GunicornLogger):
    error_fmt = r"%(asctime)s   - gunicorn.error - %(levelname)s - [pid %(process)d] - %(message)s"  # noqa: E501
    datefmt = r"%Y-%m-%d %H:%M:%S,%u"


class FractalWorker(UvicornWorker):
    """
    Subclass of uvicorn workers, which also captures SIGABRT and handles
    it within the `custom_handle_abort` method.
    """

    def init_signals(self) -> None:
        super().init_signals()
        signal.signal(signal.SIGABRT, self.custom_handle_abort)
        logger.info(
            f"[FractalWorker.init_signals - pid={self.pid}] "
            "Set `custom_handle_abort` for SIGABRT"
        )

    def custom_handle_abort(self, sig, frame) -> None:
        """
        Custom version of `gunicorn.workers.base.Worker.handle_abort`,
        transforming SIGABRT into SIGTERM.
        """
        self.alive = False
        logger.info(
            f"[FractalWorker.custom_handle_abort - pid={self.pid}] "
            "Now send SIGTERM to process."
        )
        os.kill(self.pid, signal.SIGTERM)
