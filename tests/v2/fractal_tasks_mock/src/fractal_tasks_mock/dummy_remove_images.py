import logging
from typing import Optional

from pydantic import validate_call


@validate_call
def dummy_remove_images(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    more_zarr_urls: Optional[list[str]] = None,
) -> dict:
    """
    Remove images

    Args:
        zarr_urls: description
        more_zarr_urls: Other paths that should be removed
    """
    logging.info("[dummy_remove_images] START")
    image_list_removals = zarr_urls
    if more_zarr_urls is not None:
        image_list_removals.extend(more_zarr_urls)
    logging.info("[dummy_remove_images] END")
    return dict(image_list_removals=image_list_removals)


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=dummy_remove_images)
