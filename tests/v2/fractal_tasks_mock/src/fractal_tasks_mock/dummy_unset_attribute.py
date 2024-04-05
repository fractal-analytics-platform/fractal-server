from pydantic.decorator import validate_arguments


@validate_arguments
def dummy_unset_attributes(
    *,
    paths: list[str],
    zarr_dir: str,
    attribute: str,
) -> dict:
    """
    Unset an attribute for several images

    Arguments:
        paths: description
        zarr_dir: description
        attribute: The attribute that should be unset for all input images.
    """
    print("[dummy_unset_images] START")
    out = dict(
        image_list_updates=[
            {
                "path": path,
                "attributes": {attribute: None},
            }
            for path in paths
        ]
    )
    print("[dummy_unset_images] END")
    return out


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=dummy_unset_attributes)
