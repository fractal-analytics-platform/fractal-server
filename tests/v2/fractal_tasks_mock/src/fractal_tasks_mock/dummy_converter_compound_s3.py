import logging

from pydantic import validate_call

import fractal_tasks_mock  # noqa


@validate_call
def dummy_converter_compound_s3(
    *,
    zarr_dir: str,
    image_dir: str,
    num_images: int = 2,
) -> dict:
    """
    Dummy task to check that a compound converter task can properly
     handle S3 urls, and pass them to the subtasks.

    Args:
        zarr_dir: S3 URL where the OME-Zarr files will be stored
        image_dir: S3 URL where the raw images are located
        num_images: Number of images that this dummy task will produce.
    """
    # We want to make sure that the url was not altered by the Fractal Server
    if not zarr_dir.startswith("s3://"):
        raise ValueError(f"zarr_dir must start with 's3://', got {zarr_dir}")
    if not image_dir.startswith("s3://"):
        raise ValueError(f"image_dir must start with 's3://', got {image_dir}")

    plate_zarr_name = "my_plate.zarr"
    zarr_path: str = f"{zarr_dir}/{plate_zarr_name}"

    logging.info("[dummy_converter_compound_s3] START")
    logging.info(f"[dummy_converter_compound_s3] {image_dir=}")
    logging.info(f"[dummy_converter_compound_s3] {zarr_dir=}")
    logging.info(f"[dummy_converter_compound_s3] {zarr_path=}")

    # Prepare fake list of OME-Zarr images
    image_relative_paths = [
        f"A/{ind_image:02d}/0" for ind_image in range(1, num_images + 1)
    ]

    # Prepare output metadata
    parallelization_list = []
    for image_relative_path in image_relative_paths:
        zarr_url = f"{zarr_dir}/{plate_zarr_name}/{image_relative_path}"
        raw_zarr_url = (
            f"{image_dir}/{image_relative_path.replace('/', '_')}.tif"
        )
        parallelization_list.append(
            dict(
                zarr_url=zarr_url,
                init_args=dict(raw_zarr_url=raw_zarr_url),
            )
        )
    logging.info("[dummy_converter_compound_s3] END")
    return dict(parallelization_list=parallelization_list)


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=dummy_converter_compound_s3)
