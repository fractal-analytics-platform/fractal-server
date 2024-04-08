from pathlib import Path

from pydantic.decorator import validate_arguments


@validate_arguments
def dummy_insert_single_image(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    fail: bool = False,
) -> dict:
    """
    Remove images

    Arguments:
        zarr_urls: description
        fail: If `True`, make images invalid.
    """
    print("[dummy_insert_single_image] START")
    if fail:
        new_zarr_url = "/invalid/my-new-image/"
    else:
        new_zarr_url = Path(zarr_dir, "my-new-image").as_posix()
    print(f"[dummy_insert_single_image] {new_zarr_url=}")
    out = dict(image_list_updates=[dict(zarr_url=new_zarr_url)])
    print("[dummy_insert_single_image] END")
    return out


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=dummy_insert_single_image)
