# Importing fractal_tasks_mock is only to load __init__.py
import logging
import time

from pydantic import validate_call


@validate_call
def generic_task(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    sleep_time: int = 1,
    raise_error: bool = False,
) -> dict:
    """
    Dummy task description.

    Arguments:
        zarr_urls: description
        zarr_dir: description
        sleep_time: Time to sleep, in seconds
        raise_error: If true, the task raises a ValueError
    """

    logging.warning("[generic_task] START")
    logging.warning(f"[generic_task] {sleep_time=}")
    logging.warning(f"[generic_task] {raise_error=}")

    logging.warning(f"[generic_task] Now sleep for {sleep_time} seconds")
    time.sleep(sleep_time)
    logging.warning(f"[generic_task] I slept for {sleep_time} seconds")

    if raise_error:
        logging.error("[generic_task] I will now raise an error!")
        raise ValueError("This is the error message")
    logging.warning("[generic_task] END")


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=generic_task)
