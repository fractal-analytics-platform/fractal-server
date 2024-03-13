import shutil
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from pydantic.decorator import validate_arguments

from .utils import _check_buffer_is_empty
from .utils import _check_path_is_absolute
from fractal_server.app.runner.v2.models import DictStrAny


class InitArgs(BaseModel):
    raw_path: str
    acquisition: Optional[int]


@validate_arguments
def yokogawa_to_zarr(
    *,
    path: str,
    init_args: InitArgs,
) -> dict:
    """
    TBD

    Args:
        path:
            Absolute NGFF-image path, e.g.`"/tmp/plate.zarr/A/01/0"".
    """
    print("[yokogawa_to_zarr] START")
    print(f"[yokogawa_to_zarr] {path=}")

    raw_path = init_args.raw_path

    # Based on assumption on the path structure, find plate and well
    plate = Path(path).parents[2].name
    well = Path(path).parents[1].name + Path(path).parents[0].name

    # Read data_dimensionality from data
    data_dimensionality = 3  # Mock

    print(f"[yokogawa_to_zarr] {raw_path=}")
    # Write fake image data into image Zarr group
    _check_path_is_absolute(path)
    with (Path(path) / "data").open("w") as f:
        f.write(f"Source data: {raw_path}\n")
    print("[yokogawa_to_zarr] END")
    attributes = dict(well=well)
    if init_args.acquisition is not None:
        attributes["acquisition"] = init_args.acquisition
    return dict(
        added_images=[
            dict(
                path=path,
                attributes=attributes,
            )
        ],
        new_filters=dict(plate=plate, data_dimensionality=data_dimensionality),
    )


@validate_arguments
def cellpose_segmentation(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    default_diameter: int = 100,
) -> dict:
    _check_path_is_absolute(path)
    _check_buffer_is_empty(buffer)
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
    buffer: DictStrAny,  # Used to receive information from an "init" task
) -> DictStrAny:

    old_plate = buffer["new_ome_zarr"]["old_plate"]
    new_plate = buffer["new_ome_zarr"]["new_plate"]
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


class InitArgsMIP(BaseModel):
    new_path: str
    new_plate: str  # FIXME: remove this


@validate_arguments
def maximum_intensity_projection(
    *,
    path: str,
    init_args: InitArgsMIP,
) -> DictStrAny:

    new_path = init_args.new_path
    new_plate = init_args.new_plate  # FIXME: re-compute it here

    shutil.copytree(path, new_path)

    print("[maximum_intensity_projection] START")
    print(f"[maximum_intensity_projection] {path=}")
    print(f"[maximum_intensity_projection] {new_path=}")
    print("[maximum_intensity_projection] END")

    new_filters = dict(
        plate=new_plate,
        data_dimensionality=2,
    )

    out = dict(
        added_images=[dict(path=new_path)],
        new_filters=new_filters,
    )
    return out
