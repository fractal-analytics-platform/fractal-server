import logging
import os
import signal

from uvicorn.workers import UvicornWorker


class CustomWorker(UvicornWorker):
    def init_signals(self) -> None:
        super().init_signals()
        signal.signal(signal.SIGABRT, self.handle_abort)

    def handle_abort(self, signum, frame) -> None:
        logger = logging.getLogger(__name__)
        logger.error(f"Terminating worker {self.pid} with SIGTERM.")
        os.kill(self.pid, signal.SIGTERM)
