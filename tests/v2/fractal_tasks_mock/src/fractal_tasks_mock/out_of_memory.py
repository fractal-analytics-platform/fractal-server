import logging
import sys
import time

from pydantic import validate_call


@validate_call
def out_of_memory(
    *,
    zarr_dir: str,  # noqa
    size_MB: int = 10,
    sleep_time: int = 10,
) -> None:
    """
    Dummy task description.

    Args:
        size_MB: Approximate object size, in MB.
        sleep_time: Sleeping time.
    """

    logging.info("[out_of_memory] START")
    big_object = list(range(size_MB * 1_000_000 // 8))
    logging.info(f"[out_of_memory] {sys.getsizeof(big_object)/1e6=}")
    logging.info(f"[out_of_memory] Now sleep {sleep_time} seconds.")
    time.sleep(sleep_time)
    logging.info("[out_of_memory] END")
    return None


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=out_of_memory)
