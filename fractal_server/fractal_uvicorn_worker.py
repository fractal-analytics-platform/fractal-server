import logging
import os
import signal

from uvicorn.workers import UvicornWorker

logger = logging.getLogger("uvicorn.error")


class FractalWorker(UvicornWorker):
    def init_signals(self) -> None:
        super().init_signals()
        signal.signal(signal.SIGABRT, self.custom_handle_abort)
        logger.info(f"INIT SIGNALS FROM CUSTOM WORKER ({self.pid=})")

    def custom_handle_abort(self, sig, frame):
        self.alive = False
        os.kill(self.pid, signal.SIGTERM)
        logger.info(f"TERMINATING CUSTOM WORKER {self.pid} WITH SIGTERM.")
