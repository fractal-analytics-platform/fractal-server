import logging

import fractal_tasks_mock  # noqa
from pydantic import validate_call


@validate_call
def generic_task_converter(
    zarr_dir: str,
    raise_error: bool = False,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_dir: description
        raise_error: If true, the task raises a ValueError
    """

    logging.warning("[generic_task_converter] START")
    logging.warning(f"[generic_task_converter] {raise_error=}")
    if raise_error:
        logging.error("[generic_task_converter] I will now raise an error!")
        raise ValueError("This is the error message")
    logging.warning("[generic_task_converter] END")


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=generic_task_converter)
