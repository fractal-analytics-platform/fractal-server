from typing import Optional

from pydantic.decorator import validate_arguments


@validate_arguments
def dummy_remove_images(
    *,
    paths: list[str],
    zarr_dir: str,
    more_paths: Optional[list[str]] = None,
) -> dict:
    """
    Remove images

    Arguments:
        paths: description
        more_paths: Other paths that should be removed
    """
    print("[dummy_remove_images] START")
    image_list_removals = paths
    if more_paths is not None:
        image_list_removals.extend(more_paths)
    print("[dummy_remove_images] END")
    return dict(image_list_removals=image_list_removals)


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=dummy_remove_images)
