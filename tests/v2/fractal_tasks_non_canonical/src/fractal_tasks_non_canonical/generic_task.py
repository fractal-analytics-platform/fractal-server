import logging


def generic_task(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
) -> dict:
    """
    Dummy task description.

    Arguments:
        zarr_urls: description
        zarr_dir: description

    """
    logging.info("[generic_task] START")
    logging.info("[generic_task] END")


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=generic_task)
