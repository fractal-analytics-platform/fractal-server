import shutil
from pathlib import Path
from typing import Literal
from typing import Optional

from .utils import _check_buffer_is_empty
from .utils import _check_path_is_absolute
from fractal_server.app.runner.v2.models import DictStrAny


def yokogawa_to_zarr(
    *,
    path: str,
    buffer: DictStrAny,
) -> dict:
    """
    TBD

    Args:
        path:
            Absolute NGFF-image path, e.g.`"/tmp/plate.zarr/A/01/0"".
    """
    print("[yokogawa_to_zarr] START")
    print(f"[yokogawa_to_zarr] {path=}")
    # Read raw-image path from buffer
    try:
        source_data = buffer["image_raw_paths"][path]
    except KeyError as e:
        raise ValueError(
            f"KeyError in yokogawa_to_zarr. Original error:\n{str(e)}"
        )
    except IndexError as e:
        raise ValueError(
            f"IndexError in yokogawa_to_zarr. Original error:\n{str(e)}"
        )
    print(f"[yokogawa_to_zarr] {source_data=}")
    # Write fake image data into image Zarr group
    _check_path_is_absolute(path)
    with (Path(path) / "data").open("w") as f:
        f.write(f"Source data: {source_data}\n")
    print("[yokogawa_to_zarr] END")
    return {}


def illumination_correction(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    subsets: Optional[
        dict[Literal["T_index", "C_index", "Z_index"], int]
    ] = None,
    overwrite_input: bool = False,
) -> dict:
    print("[illumination_correction] START")
    print(f"[illumination_correction] {path=}")
    print(f"[illumination_correction] {overwrite_input=}")
    print(f"[illumination_correction] {subsets=}")

    _check_buffer_is_empty(buffer)

    # Prepare output metadata and set actual_path
    if overwrite_input:
        out = dict(edited_images=[dict(path=path)])
        actual_path = path
    else:
        new_path = f"{path}_corr"
        Path(new_path).mkdir(exist_ok=True)
        out = dict(new_images=[dict(path=new_path)])
        actual_path = new_path
        print(f"[illumination_correction] {new_path=}")

    with (Path(actual_path) / "data").open("a") as f:
        f.write(
            "Illumination correction " f"({overwrite_input=}, {subsets=})\n"
        )

    print("[illumination_correction] END")
    return out


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


def maximum_intensity_projection(
    *,
    # Standard arguments
    # group (typically the plate one)
    path: str,  # Relative path to NGFF image within root_dir
    buffer: DictStrAny,  # Used to receive information from an "init" task
) -> DictStrAny:
    old_plate = buffer["new_ome_zarr"]["old_plate"]
    new_plate = buffer["new_ome_zarr"]["new_plate"]
    old_path = path.replace(new_plate, old_plate)
    old_zarr_path = old_path
    new_zarr_path = path

    shutil.copytree(old_zarr_path, new_zarr_path)

    print("[maximum_intensity_projection] START")
    print(f"[maximum_intensity_projection] {old_zarr_path=}")
    print(f"[maximum_intensity_projection] {new_zarr_path=}")
    print("[maximum_intensity_projection] END")

    out = dict(edited_images=[dict(path=path)])
    return out


def registration(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    ref_path: str,
    overwrite_input: bool = True,
) -> dict:
    print("[registration] START")
    print(f"[registration] {path=}")

    if overwrite_input:
        out = dict(edited_images=[dict(path=path)])

        with (Path(path) / "data").open("a") as f:
            f.write(f"registration against {ref_path=}\n")
    else:
        raise NotImplementedError
    print(f"[registration] {out=}")
    print("[registration] END")
    return out
