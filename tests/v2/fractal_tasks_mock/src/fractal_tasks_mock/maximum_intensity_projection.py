import shutil

from fractal_tasks_mock.input_models import InitArgsMIP
from pydantic.decorator import validate_arguments


@validate_arguments
def maximum_intensity_projection(
    *,
    zarr_url: str,
    init_args: InitArgsMIP,
) -> dict:
    """
    Dummy task description.

    Arguments:
        zarr_url: dummy argument description.
        init_args: dummy argument description.
    """

    new_zarr_url = init_args.new_zarr_url
    new_plate = init_args.new_plate  # FIXME: re-compute it here

    shutil.copytree(zarr_url, new_zarr_url)

    print("[maximum_intensity_projection] START")
    print(f"[maximum_intensity_projection] {zarr_url=}")
    print(f"[maximum_intensity_projection] {new_zarr_url=}")
    print("[maximum_intensity_projection] END")

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
    from utils import run_fractal_task

    run_fractal_task(task_function=maximum_intensity_projection)
