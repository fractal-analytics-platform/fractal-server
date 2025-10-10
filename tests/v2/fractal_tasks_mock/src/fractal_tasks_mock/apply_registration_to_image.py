import logging
from pathlib import Path
from typing import Optional

from pydantic import validate_call


@validate_call
def apply_registration_to_image(
    *,
    zarr_url: str,
    overwrite_input: bool = True,
) -> Optional[dict]:
    """
    Dummy task description.

    Args:
        zarr_url: description
        overwrite_input: whether to overwrite the existing image
    """

    prefix = "[apply_registration_to_image]"
    logging.info(f"{prefix} START")
    logging.info(f"{prefix} {zarr_url=}")
    logging.info(f"{prefix} {overwrite_input=}")

    table_path = Path(zarr_url) / "registration_table_final"
    logging.info(f" Reading information from {table_path.as_posix()}")
    with table_path.open("r") as f:
        f.read()

    # Handle the case of zarr_url=ref_zarr_url
    if overwrite_input:
        out = None
        with (Path(zarr_url) / "data").open("a") as f:
            f.write("Applying registration\n")
    else:
        new_zarr_url = f"{zarr_url}_r"
        logging.info(f"{prefix} {new_zarr_url=}")
        out = dict(
            image_list_updates=[dict(zarr_url=new_zarr_url, origin=zarr_url)]
        )
        Path(new_zarr_url).mkdir()
        with (Path(new_zarr_url) / "data").open("a") as f:
            f.write("Applying registration\n")
    logging.info(f"{prefix} {out=}")
    logging.info(f"{prefix} END")
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=apply_registration_to_image)
