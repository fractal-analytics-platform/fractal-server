from pathlib import Path
from typing import Optional

from pydantic.decorator import validate_arguments

from .utils import _check_buffer_is_empty
from .utils import _extract_common_root
from fractal_server.app.runner.v2.models import DictStrAny


@validate_arguments
def create_ome_zarr(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    zarr_dir: str,
    # Task-specific arguments
    image_dir: str,
    fake_list_relative_paths: Optional[list[str]] = None,
) -> dict:
    """
    TBD

    Args:
        image_dir: Absolute path to images folder.
        zarr_dir: Absolute path to parent folder for plate-level Zarr.
    """
    zarr_dir = zarr_dir.rstrip("/")

    if len(paths) > 0:
        raise ValueError(
            "Error in create_ome_zarr, `paths` argument must be empty, but "
            f"{paths=}."
        )
    _check_buffer_is_empty(buffer)

    # Based on images in image_folder, create plate OME-Zarr
    Path(zarr_dir).mkdir(parents=True)
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(zarr_dir) / plate_zarr_name).as_posix()

    print("[create_ome_zarr] START")
    print(f"[create_ome_zarr] {image_dir=}")
    print(f"[create_ome_zarr] {zarr_dir=}")
    print(f"[create_ome_zarr] {zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(zarr_path).mkdir()

    # Prepare fake list of OME-Zarr images
    if fake_list_relative_paths is None:
        image_relative_paths = ["A/01/0", "A/02/0"]
    else:
        image_relative_paths = fake_list_relative_paths

    # Create well/image OME-Zarr folders on disk, and prepare output
    # metadata
    parallelization_list = []
    for image_relative_path in image_relative_paths:
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)
        path = f"{zarr_dir}/{plate_zarr_name}/{image_relative_path}"
        raw_path = (
            Path(image_dir) / image_relative_path.replace("/", "_")
        ).as_posix() + ".tif"
        parallelization_list.append(
            dict(
                path=path,
                init_args=dict(raw_path=raw_path),
            )
        )
    print("[create_ome_zarr] END")
    return dict(parallelization_list=parallelization_list)


@validate_arguments
def create_ome_zarr_multiplex(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    zarr_dir: str,
    # Task-specific arguments
    image_dir: str,
) -> dict:

    if len(paths) > 0:
        raise ValueError(
            "Error in create_ome_zarr_multiplex, "
            f"`paths` argument must be empty, but {paths=}."
        )

    _check_buffer_is_empty(buffer)

    # Based on images in image_folder, create plate OME-Zarr
    zarr_dir = zarr_dir.rstrip("/")
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(zarr_dir) / plate_zarr_name).as_posix()

    print("[create_ome_zarr_multiplex] START")
    print(f"[create_ome_zarr_multiplex] {image_dir=}")
    print(f"[create_ome_zarr_multiplex] {zarr_dir=}")
    print(f"[create_ome_zarr_multiplex] {zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(zarr_path).mkdir(parents=True)

    # Create well/image OME-Zarr folders on disk, and prepare output
    # metadata
    image_relative_paths = [
        f"{well}/{cycle}"
        for well in ["A/01", "A/02"]
        for cycle in ["0", "1", "2"]
    ]
    acquisitions = [
        int(cycle) for well in ["A/01", "A/02"] for cycle in ["0", "1", "2"]
    ]
    image_raw_paths = {}
    added_images = []
    for ind, image_relative_path in enumerate(image_relative_paths):
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)
        path = f"{zarr_dir}/{plate_zarr_name}/{image_relative_path}"
        image_raw_paths[path] = (
            Path(image_dir) / image_relative_path.replace("/", "_")
        ).as_posix() + ".tif"
        added_images.append(
            dict(
                path=path,
                attributes=dict(
                    well="".join(image_relative_path.split("/")[:2]),
                    acquisition=acquisitions[ind],
                ),
            )
        )

    # Compose output metadata
    out = dict(
        added_images=added_images,
        buffer=dict(image_raw_paths=image_raw_paths),
        new_filters=dict(
            plate=plate_zarr_name,
            data_dimensionality=3,
        ),
    )
    print("[create_ome_zarr_multiplex] END")
    return out


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


"""
zarr_dirr = /tmp/my_folder
image.path = /tmp/my_folder/plate.zarr/B/03/0


image.path = /tmp/my_folder/0.zarr
image.path = /tmp/my_folder/1.zarr
image.path = /tmp/my_folder/2.zarr
"""
