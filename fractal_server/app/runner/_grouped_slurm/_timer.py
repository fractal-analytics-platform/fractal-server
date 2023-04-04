"""
Provisional module to debug timing of some FractalSlurmExecutor methods
"""
import logging
import time
from typing import Callable


def tic(description: str, logging_level: int = logging.info) -> Callable:
    t_start = time.perf_counter()
    logging.log(logging_level, f"[{description}] start")

    def _toc():
        elapsed = time.perf_counter() - t_start
        logging.log(
            logging_level,
            f"[{description}] end. Elapsed: {elapsed:.4f} s",
        )
        pass

    return _toc


if __name__ == "__main__":
    toc = tic("things")
    time.sleep(0.5)
    toc()
