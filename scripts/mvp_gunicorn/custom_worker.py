import signal

from uvicorn.workers import UvicornWorker


class CustomWorker(UvicornWorker):
    def init_signals(self) -> None:
        super().init_signals()
        print(f"INIT SIGNALS FROM CUSTOM ({self.pid=})")
        signal.signal(signal.SIGABRT, self.custom_handle_abort)

    def custom_handle_abort(self, sig, frame):
        self.alive = False
        self.cfg.worker_abort(self)
