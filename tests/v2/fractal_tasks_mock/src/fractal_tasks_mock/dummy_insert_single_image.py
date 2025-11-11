import logging
from pathlib import Path
from typing import Any
from typing import Optional

from pydantic import validate_call


@validate_call
def dummy_insert_single_image(
    *,
    zarr_dir: str,
    attributes: Optional[dict[str, Any]] = None,
    types: Optional[dict[str, Any]] = None,
    fail: bool = False,
    fail_2: bool = False,
    trailing_slash: bool = False,
    full_new_image: Optional[dict[str, Any]] = None,
) -> dict:
    """
    Remove images

    Args:
        fail: If `True`, make new_zarr_url not relative to zarr_dir
        fail_2: If `True`, make new_zarr_url equal to zarr_dir
        trailing_slash: If `True`, add 10 trailing slashes to zarr_urls
        full_new_image: If set, it takes priority
    """
    logging.info("[dummy_insert_single_image] START")
    if fail:
        new_zarr_url = "/invalid/my-new-image"
    elif fail_2:
        new_zarr_url = zarr_dir
    else:
        new_zarr_url = Path(zarr_dir, "my-new-image").as_posix()
    if trailing_slash:
        new_zarr_url += "//////////"
    logging.info(f"[dummy_insert_single_image] {new_zarr_url=}")
    new_image = dict(zarr_url=new_zarr_url)
    if attributes is not None:
        new_image["attributes"] = attributes
    if types is not None:
        new_image["types"] = types
    if full_new_image is not None:
        new_image = full_new_image
    out = dict(image_list_updates=[new_image])
    logging.info("[dummy_insert_single_image] END")
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=dummy_insert_single_image)
