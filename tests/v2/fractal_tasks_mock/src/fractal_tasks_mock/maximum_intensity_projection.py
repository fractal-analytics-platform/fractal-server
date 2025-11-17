import logging
import shutil

from pydantic import validate_call

from fractal_tasks_mock.input_models import InitArgsMIP


@validate_call
def maximum_intensity_projection(
    *,
    zarr_url: str,
    init_args: InitArgsMIP,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_url: dummy argument description.
        init_args: dummy argument description.
    """

    new_zarr_url = init_args.new_zarr_url
    new_plate = init_args.new_plate

    shutil.copytree(zarr_url, new_zarr_url)

    logging.info("[maximum_intensity_projection] START")
    logging.info(f"[maximum_intensity_projection] {zarr_url=}")
    logging.info(f"[maximum_intensity_projection] {new_zarr_url=}")
    logging.info("[maximum_intensity_projection] END")

    out = dict(
        image_list_updates=[
            dict(
                zarr_url=new_zarr_url,
                origin=zarr_url,
                attributes=dict(plate=new_plate),
            )
        ],
    )
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=maximum_intensity_projection)
