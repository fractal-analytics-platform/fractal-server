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
    zarr_dir: str,
    new_paths: Optional[list[str]] = None,
) -> dict[str, Any]:
    if new_paths is None:
        new_paths = ["a", "b", "c"]
    image_list_updates = [dict(path=new_path) for new_path in new_paths]
    return dict(image_list_updates=image_list_updates)


def remove_images(
    paths: list[str],
    zarr_dir: str,
    removed_images_paths: list[str],
) -> dict[str, Any]:
    return dict(image_list_removals=removed_images_paths)


# Parallel tasks


def print_path(
    path: str,
) -> dict[str, Any]:
    print(f"Running `print_path` task, with {path=}")
    return {}


def edit_image(path: str, custom_parameter: int = 1) -> dict[str, Any]:
    return dict(image_list_updates=[dict(path=path)])


def copy_and_edit_image(
    path: str,
) -> dict[str, Any]:
    return dict(
        image_list_updates=[
            dict(path=f"{path}_new", attributes=dict(processed=True))
        ]
    )


# Task v1


def generic_task_v1(
    input_paths: list[str], output_path: str, metadata: dict, component: str
) -> dict[str, Any]:
    print(f"Task v1, return {input_paths=}")
    return {}
