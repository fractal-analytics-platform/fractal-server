import shutil
from pathlib import Path

from pydantic import BaseModel
from pydantic.decorator import validate_arguments

from .utils import _extract_common_root
from fractal_server.app.runner.v2.models import DictStrAny


@validate_arguments
def new_ome_zarr(
    *,
    # Standard arguments
    paths: list[str],
    zarr_dir: str,
    # Non-standard arguments
    suffix: str = "new",
) -> dict:

    dict_shared = _extract_common_root(paths)
    old_plate = dict_shared.get("shared_plate")

    print("[new_ome_zarr] START")
    print(f"[new_ome_zarr] {paths=}")
    print(f"[new_ome_zarr] Identified {old_plate=}")

    assert old_plate.endswith(".zarr")  # nosec
    new_plate = old_plate.strip(".zarr") + f"_{suffix}.zarr"
    print(f"[new_ome_zarr] {new_plate=}")

    # Based on images in image_folder, create plate OME-Zarr
    new_zarr_path = (Path(zarr_dir) / new_plate).as_posix()
    print(f"[new_ome_zarr] {new_zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(new_zarr_path).mkdir()

    parallelization_list = []
    for old_path in paths:
        new_path = old_path.replace(old_plate, new_plate)
        parallelization_list.append(
            dict(
                path=old_path,
                init_args=dict(
                    new_path=new_path,
                    new_plate=new_plate,
                ),
            )
        )

    # Prepare output metadata
    out = dict(parallelization_list=parallelization_list)
    print("[new_ome_zarr] END")
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
    )

    out = dict(
        added_images=[dict(path=new_path)],
        new_filters=new_filters,
    )
    return out
