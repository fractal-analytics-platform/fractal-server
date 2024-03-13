from pathlib import Path

from pydantic.decorator import validate_arguments

from .utils import _check_path_is_absolute
from fractal_server.app.runner.v2.models import DictStrAny


@validate_arguments
def cellpose_segmentation(
    *,
    # Standard arguments
    path: str,
    # Non-standard arguments
    default_diameter: int = 100,
) -> dict:
    _check_path_is_absolute(path)
    print("[cellpose_segmentation] START")
    print(f"[cellpose_segmentation] {path=}")

    with (Path(path) / "data").open("a") as f:
        f.write("Cellpose segmentation\n")

    out = dict()
    print(f"[cellpose_segmentation] {out=}")
    print("[cellpose_segmentation] END")
    return out


@validate_arguments
def copy_data(
    *,
    # Standard arguments
    # Zarr group (typically the plate one)
    path: str,
) -> DictStrAny:

    raise NotImplementedError

    """
    old_path = path.replace(new_plate, old_plate)
    old_zarr_path = old_path
    new_zarr_path = path

    shutil.copytree(old_zarr_path, new_zarr_path)

    print("[copy_data] START")
    print(f"[copy_data] {old_zarr_path=}")
    print(f"[copy_data] {new_zarr_path=}")
    print("[copy_data] END")

    out = {}
    return out
    """
