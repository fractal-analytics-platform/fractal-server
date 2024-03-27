from pathlib import Path

from input_models import InitArgsCellVoyager
from pydantic.decorator import validate_arguments
from utils import _check_path_is_absolute


@validate_arguments
def fill_cellvoyager_ome_zarr(
    *,
    path: str,
    init_args: InitArgsCellVoyager,
) -> dict:
    """
    Dummy task description.
    """

    print("[fill_cellvoyager_ome_zarr] START")
    print(f"[fill_cellvoyager_ome_zarr] {path=}")

    raw_path = init_args.raw_path

    # Based on assumption on the path structure, find plate and well
    plate = Path(path).parents[2].name
    well = Path(path).parents[1].name + Path(path).parents[0].name

    # Read 3D from data
    is_3D = True  # Mock

    print(f"[fill_cellvoyager_ome_zarr] {raw_path=}")
    # Write fake image data into image Zarr group
    _check_path_is_absolute(path)
    with (Path(path) / "data").open("w") as f:
        f.write(f"Source data: {raw_path}\n")
    print("[fill_cellvoyager_ome_zarr] END")
    attributes = dict(well=well, plate=plate)
    if init_args.acquisition is not None:
        attributes["acquisition"] = init_args.acquisition
    out = dict(
        image_list_updates=[
            dict(
                path=path,
                attributes=attributes,
                types={"3D": is_3D},
            )
        ],
    )
    return out


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=fill_cellvoyager_ome_zarr)
