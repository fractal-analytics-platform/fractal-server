from typing import Any
from typing import Optional


def dummy_task(*args, **kwargs):
    """
    This task does nothing, and it is both valid as
    a parallel or non_parallel task.
    """
    return {}


# Non-parallel tasks


def create_images_from_scratch(
    paths: list[str],
    buffer: dict[str, Any],
    zarr_dir: str,
    new_paths: Optional[list[str]] = None,
) -> dict[str, Any]:
    if new_paths is None:
        new_paths = ["a", "b", "c"]
    added_images = [dict(path=new_path) for new_path in new_paths]
    return dict(added_images=added_images)


def remove_images(
    paths: list[str],
    buffer: dict[str, Any],
    zarr_dir: str,
    removed_images_paths: list[str],
) -> dict[str, Any]:

    removed_images = [
        dict(path=image_path) for image_path in removed_images_paths
    ]

    return dict(removed_images=removed_images)


# Parallel tasks


def print_path(path: str, buffer: dict[str, Any]) -> dict[str, Any]:
    print(f"Running `print_path` task, with {path=}")
    return {}


def edit_image(
    path: str, buffer: dict[str, Any], custom_parameter: int = 1
) -> dict[str, Any]:
    edited_images = [dict(path=path)]
    return dict(edited_images=edited_images)


def copy_and_edit_image(
    path: str,
    buffer: dict[str, Any],
) -> dict[str, Any]:
    added_images = [dict(path=f"{path}_new", attributes=dict(processed=True))]
    return dict(added_images=added_images)
