import logging

from pydantic import validate_call


@validate_call
def generic_task_parallel(*, zarr_url: str) -> dict:
    """
    Dummy task description.

    Arguments:
        zarr_dir: description
    """

    logging.info("[generic_task_parallel] START")
    logging.info("[generic_task_parallel] Do nothing and return None")
    logging.info("[generic_task_parallel] END")
    return None


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=generic_task_parallel)
