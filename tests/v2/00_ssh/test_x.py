from contextlib import contextmanager
from threading import Lock


@contextmanager
def acquire_timeout(lock: Lock, timeout: float):
    print(f"Trying to acquire lock, with {timeout=}")
    result = lock.acquire(timeout=timeout)
    try:
        if not result:
            raise TimeoutError(
                f"Failed to acquire lock within {timeout} seconds"
            )
        print("Lock was acquired.")
        yield result
    finally:
        if result:
            lock.release()
            print("Lock was released")


def test_acquire_lock():
    lock = Lock()
    lock.acquire()
    try:
        with acquire_timeout(lock=lock, timeout=0.1):
            pass
    except Exception as e:
        print(e)
