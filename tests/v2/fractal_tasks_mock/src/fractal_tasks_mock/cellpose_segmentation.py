from pathlib import Path

from fractal_tasks_mock.utils import _check_path_is_absolute
from pydantic.decorator import validate_arguments


@validate_arguments
def cellpose_segmentation(
    *,
    path: str,
) -> None:
    """
    Dummy task description.

    Arguments:
        path: description
    """

    _check_path_is_absolute(path)
    print("[cellpose_segmentation] START")
    print(f"[cellpose_segmentation] {path=}")

    with (Path(path) / "data").open("a") as f:
        f.write("Cellpose segmentation\n")

    print("[cellpose_segmentation] END")
    return None


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=cellpose_segmentation)
