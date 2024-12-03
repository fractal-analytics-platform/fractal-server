import logging

from pydantic.decorator import validate_arguments


@validate_arguments
def dummy_remove_images(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    more_zarr_urls: list[str] | None = None,
) -> dict:
    """
    Remove images

    Arguments:
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
    from utils import run_fractal_task

    run_fractal_task(task_function=dummy_remove_images)
