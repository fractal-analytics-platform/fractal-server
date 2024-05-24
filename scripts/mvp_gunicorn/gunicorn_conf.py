import os
import signal


def worker_abort(worker):
    print(f"TERMINATING WORKER {worker.pid} WITH SIGTERM.")
    os.kill(worker.pid, signal.SIGTERM)
