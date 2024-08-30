import logging
import os
import signal
import time

from gunicorn.glogging import Logger as GunicornLogger
from uvicorn_worker import UvicornWorker

from fractal_server.logger import _converter
from fractal_server.logger import DATE_FORMAT

logger = logging.getLogger("uvicorn.error")


class FractalGunicornLoggingFormatter(logging.Formatter):
    def converter(self, timestamp: float) -> time.struct_time:
        return _converter(timestamp)


class FractalGunicornLogger(GunicornLogger):
    error_fmt = (
        "%(asctime)s::%(msecs)03d - gunicorn.error - %(levelname)s - "
        "[pid %(process)d] - %(message)s"
    )
    datefmt = DATE_FORMAT

    def setup(self, cfg):
        super().setup(cfg)
        formatter = FractalGunicornLoggingFormatter(
            self.error_fmt, self.datefmt
        )
        self._set_handler(self.error_log, cfg.errorlog, formatter)


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

    def custom_handle_abort(self, sig, frame):
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
