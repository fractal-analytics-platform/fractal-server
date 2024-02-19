import os
import shutil
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional

from .models import Task


def _extract_common_root(paths: list[str]) -> dict[str, str]:
    shared_plates = []
    shared_root_dirs = []
    for path in paths:
        tmp = path.split(".zarr/")[0]
        shared_root_dirs.append("/".join(tmp.split("/")[:-1]))
        shared_plates.append(tmp.split("/")[-1] + ".zarr")

    if len(set(shared_plates)) > 1 or len(set(shared_root_dirs)) > 1:
        raise ValueError
    shared_plate = list(shared_plates)[0]
    shared_root_dir = list(shared_root_dirs)[0]

    return dict(shared_root_dir=shared_root_dir, shared_plate=shared_plate)


def create_ome_zarr(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[dict[str, Any]] = None,
    # Task-specific arguments
    image_dir: str,
    zarr_dir: str,
) -> dict:
    """
    TBD

    Args:
        root_dir: Absolute path to parent folder for plate-level Zarr.
        image_dir: Absolute path to images folder.
    """
    zarr_dir = zarr_dir.rstrip("/")

    if len(paths) > 0:
        raise RuntimeError(f"Something wrong in create_ome_zarr. {paths=}")

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

    # Create well/image OME-Zarr folders on disk
    image_relative_paths = ["A/01/0", "A/02/0"]
    for image_relative_path in image_relative_paths:
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)

    # Prepare output metadata
    out = dict(
        new_images=[
            dict(
                path=(
                    f"{zarr_dir}/{plate_zarr_name}/" f"{image_relative_path}"
                ),
                well="_".join(image_relative_path.split("/")[:2]),
            )
            for image_relative_path in image_relative_paths
        ],
        buffer=dict(
            image_raw_paths={
                (
                    f"{zarr_dir}/{plate_zarr_name}/A/01/0"
                ): f"{image_dir}/figure_A01.tif",
                (
                    f"{zarr_dir}/{plate_zarr_name}/A/02/0"
                ): f"{image_dir}/figure_A02.tif",
            },
        ),
        new_filters=dict(
            plate=plate_zarr_name,
            data_dimensionality="3",
        ),
    )
    print("[create_ome_zarr] END")
    return out


def yokogawa_to_zarr(
    *,
    # Standard arguments
    path: str,
    buffer: dict[str, Any],
) -> dict:
    """
    TBD

    Args:
        root_dir: Absolute path to parent folder for plate-level Zarr.
        path:
            Relative image path within `root_dir`, e.g.`"plate.zarr/A/01/0"".
    """

    print("[yokogawa_to_zarr] START")
    print(f"[yokogawa_to_zarr] {path=}")

    source_data = buffer["image_raw_paths"][path]
    print(f"[yokogawa_to_zarr] {source_data=}")

    # Write fake image data into image Zarr group
    if not os.path.isabs(path):
        raise ValueError(f"Path is not absolute {path=}")

    with (Path(path) / "data").open("w") as f:
        f.write(f"Source data: {source_data}\n")

    print("[yokogawa_to_zarr] END")
    return {}


def illumination_correction(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[dict[str, Any]] = None,
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

    if overwrite_input:
        out = dict(edited_images=[dict(path=path)])

        with (Path(path) / "data").open("a") as f:
            f.write("Illumination correction\n")

    else:
        new_path = f"{path}_corr"

        Path(new_path).mkdir()
        with (Path(new_path) / "data").open("w") as f:
            f.write("Illuination correction\n")

        print(f"[illumination_correction] {new_path=}")
        out = dict(new_images=[dict(path=new_path)])
    print(f"[illumination_correction] {out=}")
    print("[illumination_correction] END")
    return out


def cellpose_segmentation(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[dict[str, Any]] = None,
    # Non-standard arguments
    default_diameter: int = 100,
) -> dict:
    print("[cellpose_segmentation] START")
    print(f"[cellpose_segmentation] {path=}")

    with (Path(path) / "data").open("a") as f:
        f.write("Cellpose segmentation\n")

    out = dict()
    print(f"[cellpose_segmentation] {out=}")
    print("[cellpose_segmentation] END")
    return out


def new_ome_zarr(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[dict[str, Any]] = None,
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

    new_image_paths = [path.replace(old_plate, new_plate) for path in paths]

    new_filters = dict(plate=new_plate)
    if project_to_2D:
        new_filters["data_dimensionality"] = "2"

    # Prepare output metadata
    out = dict(
        new_images=[dict(path=path) for path in new_image_paths],
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


def copy_data(
    *,
    # Standard arguments
    # Zarr group (typically the plate one)
    path: str,  # Relative path to NGFF image within root_dir
    buffer: dict[str, Any],  # Used to receive information from an "init" task
) -> dict[str, Any]:

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
    buffer: dict[str, Any],  # Used to receive information from an "init" task
) -> dict[str, Any]:
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


# This is a task that only serves as an init task
def init_channel_parallelization(
    *,
    # Standard arguments
    # root_dir: str,
    paths: list[str],
    buffer: Optional[dict[str, Any]] = None,
) -> dict:
    print("[init_channel_parallelization] START")
    # print(f"[init_channel_parallelization] {root_dir=}")
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


def create_ome_zarr_multiplex(
    *,
    # Standard arguments
    root_dir: str,
    paths: list[str],
    buffer: Optional[dict[str, Any]] = None,
    # Task-specific arguments
    image_dir: str,
) -> dict:
    if len(paths) > 0:
        raise RuntimeError(
            f"Something wrong in create_ome_zarr_multiplex. {paths=}"
        )

    # Based on images in image_folder, create plate OME-Zarr
    # Path(root_dir).mkdir(parents=True)
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(root_dir) / plate_zarr_name).as_posix()

    print("[create_ome_zarr_multiplex] START")
    print(f"[create_ome_zarr_multiplex] {image_dir=}")
    print(f"[create_ome_zarr_multiplex] {root_dir=}")
    print(f"[create_ome_zarr_multiplex] {zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(zarr_path).mkdir()

    # Create well/image OME-Zarr folders on disk
    image_relative_paths = [
        f"{well}/{cycle}"
        for well in ["A/01", "A/02"]
        for cycle in ["0", "1", "2"]
    ]
    acquisitions = [
        str(cycle) for well in ["A/01", "A/02"] for cycle in ["0", "1", "2"]
    ]

    for image_relative_path in image_relative_paths:
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)

    # Prepare output metadata
    new_images = []
    for ind, image_relative_path in enumerate(image_relative_paths):
        new_images.append(
            dict(
                path=f"{plate_zarr_name}/{image_relative_path}",
                well="_".join(image_relative_path.split("/")[:2]),
                acquisition=acquisitions[ind],
            )
        )
    out = dict(
        new_images=new_images,
        buffer=dict(
            image_raw_paths={
                f"{plate_zarr_name}/A/01/0": f"{image_dir}/figure_A01_0.tif",
                f"{plate_zarr_name}/A/01/1": f"{image_dir}/figure_A01_1.tif",
                f"{plate_zarr_name}/A/01/2": f"{image_dir}/figure_A01_2.tif",
                f"{plate_zarr_name}/A/02/0": f"{image_dir}/figure_A02_0.tif",
                f"{plate_zarr_name}/A/02/1": f"{image_dir}/figure_A02_1.tif",
                f"{plate_zarr_name}/A/02/2": f"{image_dir}/figure_A02_2.tif",
            },
        ),
        new_filters=dict(
            plate=plate_zarr_name,
            data_dimensionality="3",
        ),
    )
    print("[create_ome_zarr] END")
    return out


# This is a task that only serves as an init task
def init_registration(
    *,
    # Standard arguments
    root_dir: str,
    paths: list[str],
    buffer: Optional[dict[str, Any]] = None,
    # Non-standard arguments
    ref_cycle_name: str,
) -> dict:

    print("[init_registration] START")
    print(f"[init_registration] {root_dir=}")
    print(f"[init_registration] {paths=}")

    # Detect plate prefix
    shared_plate = set(path.split("/")[0] for path in paths)
    if len(shared_plate) > 1:
        raise ValueError
    shared_plate = list(shared_plate)[0]
    print(f"[init_registration] Identified {shared_plate=}")

    ref_cycles_per_well = {}
    x_cycles_per_well = {}
    wells = []
    for path in paths:
        path_splits = path.lstrip(shared_plate).strip("/").split("/")
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


TASK_LIST = {
    "create_ome_zarr": Task(
        function=create_ome_zarr, task_type="non_parallel"
    ),
    "yokogawa_to_zarr": Task(function=yokogawa_to_zarr, task_type="parallel"),
    "create_ome_zarr_multiplex": Task(
        function=create_ome_zarr_multiplex, task_type="non_parallel"
    ),
    "cellpose_segmentation": Task(
        function=cellpose_segmentation, task_type="parallel"
    ),
    "new_ome_zarr": Task(function=new_ome_zarr, task_type="non_parallel"),
    "copy_data": Task(function=copy_data, task_type="parallel"),
    "illumination_correction": Task(
        function=illumination_correction,
        task_type="parallel",
        new_filters=dict(illumination_correction=True),
    ),
    "maximum_intensity_projection": Task(
        function=maximum_intensity_projection,
        task_type="parallel",
        new_filters=dict(data_dimensionality="2"),
    ),
    "init_channel_parallelization": Task(
        function=init_channel_parallelization, task_type="non_parallel"
    ),
    "init_registration": Task(
        function=init_registration, task_type="non_parallel"
    ),
}
