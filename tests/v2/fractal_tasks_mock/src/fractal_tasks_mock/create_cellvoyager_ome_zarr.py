from pathlib import Path
from typing import Optional

from pydantic.decorator import validate_arguments


@validate_arguments
def create_cellvoyager_ome_zarr(
    *,
    paths: list[str],
    zarr_dir: str,
    image_dir: str,
    fake_list_relative_paths: Optional[list[str]] = None,
) -> dict:

    zarr_dir = zarr_dir.rstrip("/")

    if len(paths) > 0:
        raise ValueError(
            "Error in create_cellvoyager_ome_zarr: "
            "`paths` argument must be empty, but "
            f"{paths=}."
        )

    # Based on images in image_folder, create plate OME-Zarr
    Path(zarr_dir).mkdir(parents=True)
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(zarr_dir) / plate_zarr_name).as_posix()

    print("[create_cellvoyager_ome_zarr] START")
    print(f"[create_cellvoyager_ome_zarr] {image_dir=}")
    print(f"[create_cellvoyager_ome_zarr] {zarr_dir=}")
    print(f"[create_cellvoyager_ome_zarr] {zarr_path=}")

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
    print("[create_cellvoyager_ome_zarr] END")
    return dict(parallelization_list=parallelization_list)
