import logging

from pydantic import validate_call

from fractal_tasks_mock.input_models import InitArgsCellVoyager


@validate_call
def dummy_converter_s3(
    *,
    zarr_url: str,
    init_args: InitArgsCellVoyager,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_url: S3 URL where the OME-Zarr files will be stored
        init_args: Initialization arguments for the task
    """
    # We want to make sure that the url was not altered by the Fractal Server
    if not zarr_url.startswith("s3://"):
        raise ValueError(f"zarr_url must start with 's3://', got {zarr_url}")

    logging.info("[dummy_converter_s3] START")
    logging.info(f"[dummy_converter_s3] {zarr_url=}")

    raw_zarr_url = init_args.raw_zarr_url
    if not raw_zarr_url.startswith("s3://"):
        raise ValueError(
            f"raw_zarr_url in init_args must start with 's3://', "
            f"got {raw_zarr_url}"
        )

    # Based on assumption on the zarr_url structure, find plate and well
    # Just a mock but we keep it realistic
    # zarr_url has the form: s3://<bucket>/.../<plate>.zarr/<row>/<col>/<idx>
    parts = zarr_url.rstrip("/").split("/")
    plate = parts[-4]
    well = parts[-3] + parts[-2]

    # Read 3D from data
    is_3D = True  # Mock

    logging.info(f"[dummy_converter_s3] {raw_zarr_url=}")
    logging.info("[dummy_converter_s3] END")
    attributes = dict(well=well, plate=plate)
    if init_args.acquisition is not None:
        attributes["acquisition"] = init_args.acquisition
    out = dict(
        image_list_updates=[
            dict(
                zarr_url=zarr_url,
                attributes=attributes,
                types={"3D": is_3D},
            )
        ],
    )
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=dummy_converter_s3)
