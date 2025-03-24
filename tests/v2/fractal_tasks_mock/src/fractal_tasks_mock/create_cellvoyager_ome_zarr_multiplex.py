import logging
from pathlib import Path

from pydantic.decorator import validate_arguments


@validate_arguments
def create_cellvoyager_ome_zarr_multiplex(
    *,
    zarr_dir: str,
    image_dir: str,
) -> dict:
    """
    Dummy task description.


    Arguments:
        zarr_dir: description
        image_dir: Image where the raw images are
    """

    # Based on images in image_folder, create plate OME-Zarr
    zarr_dir = zarr_dir.rstrip("/")
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(zarr_dir) / plate_zarr_name).as_posix()

    logging.info("[create_cellvoyager_ome_zarr_multiplex] START")
    logging.info(f"[create_cellvoyager_ome_zarr_multiplex] {image_dir=}")
    logging.info(f"[create_cellvoyager_ome_zarr_multiplex] {zarr_dir=}")
    logging.info(f"[create_cellvoyager_ome_zarr_multiplex] {zarr_path=}")

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

    parallelization_list = []
    for ind, image_relative_path in enumerate(image_relative_paths):
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)
        zarr_url = f"{zarr_dir}/{plate_zarr_name}/{image_relative_path}"
        raw_zarr_url = (
            Path(image_dir) / image_relative_path.replace("/", "_")
        ).as_posix() + ".tif"
        parallelization_list.append(
            dict(
                zarr_url=zarr_url,
                init_args=dict(
                    raw_zarr_url=raw_zarr_url, acquisition=acquisitions[ind]
                ),
            )
        )
    # Compose output metadata
    out = dict(parallelization_list=parallelization_list)
    logging.info("[create_cellvoyager_ome_zarr_multiplex] END")
    return out


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=create_cellvoyager_ome_zarr_multiplex)
