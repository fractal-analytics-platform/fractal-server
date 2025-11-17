import logging
from pathlib import Path

from pydantic import validate_call

from fractal_tasks_mock.input_models import InitArgsCellVoyager
from fractal_tasks_mock.utils import _check_zarr_url_is_absolute


@validate_call
def fill_cellvoyager_ome_zarr(
    *,
    zarr_url: str,
    init_args: InitArgsCellVoyager,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_url: description
        init_args: description
    """

    logging.info("[fill_cellvoyager_ome_zarr] START")
    logging.info(f"[fill_cellvoyager_ome_zarr] {zarr_url=}")

    raw_zarr_url = init_args.raw_zarr_url

    # Based on assumption on the zarr_url structure, find plate and well
    plate = Path(zarr_url).parents[2].name
    well = Path(zarr_url).parents[1].name + Path(zarr_url).parents[0].name

    # Read 3D from data
    is_3D = True  # Mock

    logging.info(f"[fill_cellvoyager_ome_zarr] {raw_zarr_url=}")
    # Write fake image data into image Zarr group
    _check_zarr_url_is_absolute(zarr_url)
    with (Path(zarr_url) / "data").open("w") as f:
        f.write(f"Source data: {raw_zarr_url}\n")
    logging.info("[fill_cellvoyager_ome_zarr] END")
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

    run_fractal_task(task_function=fill_cellvoyager_ome_zarr)
