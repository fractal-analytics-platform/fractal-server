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
    # Task-specific arguments
    image_dir: str,
    zarr_dir: str,
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
    image_raw_paths = {}
    added_images = []
    for image_relative_path in image_relative_paths:
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)
        path = f"{zarr_dir}/{plate_zarr_name}/{image_relative_path}"
        image_raw_paths[path] = (
            Path(image_dir) / image_relative_path.replace("/", "_")
        ).as_posix() + ".tif"
        added_images.append(
            dict(
                path=path,
                attributes=dict(
                    well="_".join(image_relative_path.split("/")[:2])
                ),
            )
        )

    # Combine output metadata
    out = dict(
        added_images=added_images,
        buffer=dict(image_raw_paths=image_raw_paths),
        new_filters=dict(
            plate=plate_zarr_name,
            data_dimensionality=3,
        ),
    )
    print("[create_ome_zarr] END")
    return out


@validate_arguments
def create_ome_zarr_multiplex(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    # Task-specific arguments
    image_dir: str,
    zarr_dir: str,
) -> dict:
    if len(paths) > 0:
        raise ValueError(
            "Error in create_ome_zarr_multiplex, "
            f"`paths` argument must be empty, but {paths=}."
        )

    zarr_dir = zarr_dir.rstrip("/")
    # Based on images in image_folder, create plate OME-Zarr
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
        str(cycle) for well in ["A/01", "A/02"] for cycle in ["0", "1", "2"]
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
                    well="_".join(image_relative_path.split("/")[:2]),
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
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    suffix: str = "new",
    project_to_2D: bool = True,
) -> dict:

    dict_shared = _extract_common_root(paths)
    shared_root_dir = dict_shared.get("shared_root_dir")
    old_plate = dict_shared.get("shared_plate")

    print("[new_ome_zarr] START")
    print(f"[new_ome_zarr] {paths=}")
    print(f"[new_ome_zarr] Identified {old_plate=}")

    assert old_plate.endswith(".zarr")  # nosec
    new_plate = old_plate.strip(".zarr") + f"_{suffix}.zarr"
    print(f"[new_ome_zarr] {new_plate=}")

    # Based on images in image_folder, create plate OME-Zarr
    new_zarr_path = (Path(shared_root_dir) / new_plate).as_posix()

    print(f"[new_ome_zarr] {new_zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(new_zarr_path).mkdir()

    added_image_paths = [path.replace(old_plate, new_plate) for path in paths]

    new_filters = dict(plate=new_plate)
    if project_to_2D:
        new_filters["data_dimensionality"] = 2

    # Prepare output metadata
    out = dict(
        added_images=[dict(path=path) for path in added_image_paths],
        new_filters=new_filters,
        buffer=dict(
            new_ome_zarr=dict(
                old_plate=old_plate,
                new_plate=new_plate,
            )
        ),
    )
    print("[new_ome_zarr] END")
    return out


@validate_arguments
def init_channel_parallelization(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
) -> dict:
    print("[init_channel_parallelization] START")
    print(f"[init_channel_parallelization] {paths=}")
    parallelization_list = []
    for path in paths:
        # Find out number of channels, from Zarr
        # array shape or from NGFF metadata
        num_channels = 2  # mock
        for ind_channel in range(num_channels):
            parallelization_list.append(
                dict(path=path, subsets=dict(C_index=ind_channel))
            )
    print("[init_channel_parallelization] END")
    return dict(parallelization_list=parallelization_list)


@validate_arguments
def init_registration(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    ref_cycle_name: str,
) -> dict:

    print("[init_registration] START")
    print(f"[init_registration] {paths=}")

    # Detect plate prefix
    shared_plate = _extract_common_root(paths).get("shared_plate")
    shared_root_dir = _extract_common_root(paths).get("shared_root_dir")
    print(f"[init_registration] Identified {shared_plate=}")

    ref_cycles_per_well = {}
    x_cycles_per_well = {}
    wells = []
    for path in paths:
        path_splits = (
            path.lstrip(shared_root_dir + shared_plate).strip("/").split("/")
        )
        well = "/".join(path_splits[0:2])
        wells.append(well)
        image = path_splits[2]
        if image == ref_cycle_name:
            assert well not in ref_cycles_per_well.keys()  # nosec
            ref_cycles_per_well[well] = path
        else:
            cycles = x_cycles_per_well.get(well, [])
            cycles.append(path)
            x_cycles_per_well[well] = cycles

    parallelization_list = []
    for well in sorted(set(wells)):
        print(f"[init_registration] {well=}")
        ref_path = ref_cycles_per_well[well]
        for path in x_cycles_per_well[well]:
            parallelization_list.append(
                dict(
                    path=path,
                    ref_path=ref_path,
                )
            )

    print("[init_registration] END")
    return dict(parallelization_list=parallelization_list)
