import logging
import time

import fractal_tasks_mock  # noqa
from pydantic.decorator import validate_arguments


@validate_arguments
def generic_task_parallel(
    *,
    zarr_url: str,
    sleep_time: float = 0.0,
) -> dict:
    """
    Dummy task description.

    Arguments:
        zarr_dir: description
    """

    logging.info("[generic_task_parallel] START")
    logging.info(f"[generic_task_parallel] Sleep {sleep_time} seconds")
    time.sleep(sleep_time)
    logging.info("[generic_task_parallel] Do nothing and return None")
    logging.info("[generic_task_parallel] END")
    return None


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=generic_task_parallel)
