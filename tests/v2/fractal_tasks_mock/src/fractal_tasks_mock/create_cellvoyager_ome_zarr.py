import logging
from pathlib import Path

from pydantic import validate_call

import fractal_tasks_mock  # noqa


@validate_call
def create_cellvoyager_ome_zarr(
    *,
    zarr_dir: str,
    image_dir: str,
    num_images: int = 2,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_urls: description
        zarr_dir: description
        image_dir: Image where the raw images are
        num_images: Number of images that this dummy task will produce.
    """

    zarr_dir = zarr_dir.rstrip("/")

    # Based on images in image_folder, create plate OME-Zarr
    Path(zarr_dir).mkdir(parents=True)
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(zarr_dir) / plate_zarr_name).as_posix()

    logging.info("[create_cellvoyager_ome_zarr] START")
    logging.info(f"[create_cellvoyager_ome_zarr] {image_dir=}")
    logging.info(f"[create_cellvoyager_ome_zarr] {zarr_dir=}")
    logging.info(f"[create_cellvoyager_ome_zarr] {zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(zarr_path).mkdir()

    # Prepare fake list of OME-Zarr images
    image_relative_paths = [
        f"A/{ind_image:02d}/0" for ind_image in range(1, num_images + 1)
    ]

    # Create well/image OME-Zarr folders on disk, and prepare output
    # metadata
    parallelization_list = []
    for image_relative_path in image_relative_paths:
        (Path(zarr_path) / image_relative_path).mkdir(parents=True)
        zarr_url = f"{zarr_dir}/{plate_zarr_name}/{image_relative_path}"
        raw_zarr_url = (
            Path(image_dir) / image_relative_path.replace("/", "_")
        ).as_posix() + ".tif"
        parallelization_list.append(
            dict(
                zarr_url=zarr_url,
                init_args=dict(raw_zarr_url=raw_zarr_url),
            )
        )
    logging.info("[create_cellvoyager_ome_zarr] END")
    return dict(parallelization_list=parallelization_list)


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=create_cellvoyager_ome_zarr)
