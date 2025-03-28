import logging

from pydantic.decorator import validate_arguments


@validate_arguments
def dummy_unset_attribute(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    attribute: str,
) -> dict:
    """
    Unset an attribute for several images

    Arguments:
        zarr_urls: description
        zarr_dir: description
        attribute: The attribute that should be unset for all input images.
    """
    logging.info("[dummy_unset_images] START")
    out = dict(
        image_list_updates=[
            {
                "zarr_url": zarr_url,
                "attributes": {attribute: None},
            }
            for zarr_url in zarr_urls
        ]
    )
    logging.info("[dummy_unset_images] END")
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=dummy_unset_attribute)
