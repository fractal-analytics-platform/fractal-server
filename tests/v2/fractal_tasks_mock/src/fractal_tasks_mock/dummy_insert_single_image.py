from pathlib import Path
from typing import Any
from typing import Optional

from pydantic.decorator import validate_arguments


@validate_arguments
def dummy_insert_single_image(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    attributes: Optional[dict[str, Any]] = None,
    fail: bool = False,
    fail_2: bool = False,
    trailing_slash: bool = False,
    full_new_image: Optional[dict[str, Any]] = None,
) -> dict:
    """
    Remove images

    Arguments:
        zarr_urls: description
        fail: If `True`, make new_zarr_url not relative to zarr_dir
        fail_2: If `True`, make new_zarr_url equal to zarr_dir
        trailing_slash: If `True`, add 10 trailing slashes to zarr_urls
        full_new_image: If set, it takes priority
    """
    print("[dummy_insert_single_image] START")
    if fail:
        new_zarr_url = "/invalid/my-new-image"
    elif fail_2:
        new_zarr_url = zarr_dir
    else:
        new_zarr_url = Path(zarr_dir, "my-new-image").as_posix()
    if trailing_slash:
        new_zarr_url += "//////////"
    print(f"[dummy_insert_single_image] {new_zarr_url=}")
    new_image = dict(zarr_url=new_zarr_url)
    if attributes is not None:
        new_image["attributes"] = attributes
    if full_new_image is not None:
        new_image = full_new_image
    out = dict(image_list_updates=[new_image])
    print("[dummy_insert_single_image] END")
    return out


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=dummy_insert_single_image)
