from pathlib import Path

from fractal_tasks_mock.utils import _check_zarr_url_is_absolute
from pydantic.decorator import validate_arguments


@validate_arguments
def cellpose_segmentation(
    *,
    zarr_url: str,
) -> None:
    """
    Dummy task description.

    Arguments:
        zarr_url: description
    """

    _check_zarr_url_is_absolute(zarr_url)
    print("[cellpose_segmentation] START")
    print(f"[cellpose_segmentation] {zarr_url=}")

    with (Path(zarr_url) / "data").open("a") as f:
        f.write("Cellpose segmentation\n")

    print("[cellpose_segmentation] END")
    return None


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=cellpose_segmentation)
