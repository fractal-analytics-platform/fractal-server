import logging
from pathlib import Path
from typing import Optional

from pydantic import validate_call


@validate_call
def illumination_correction(
    *,
    zarr_url: str,
    overwrite_input: bool = False,
) -> Optional[dict]:
    """
    Dummy task description.
    """

    logging.info("[illumination_correction] START")
    logging.info(f"[illumination_correction] {zarr_url=}")
    logging.info(f"[illumination_correction] {overwrite_input=}")

    # Prepare output metadata and set actual_zarr_url
    if overwrite_input:
        out = None
        actual_zarr_url = zarr_url
    else:
        new_zarr_url = f"{zarr_url}_corr"
        Path(new_zarr_url).mkdir(exist_ok=True)
        out = dict(
            image_list_updates=[dict(zarr_url=new_zarr_url, origin=zarr_url)]
        )
        actual_zarr_url = new_zarr_url
        logging.info(f"[illumination_correction] {new_zarr_url=}")

    with (Path(actual_zarr_url) / "data").open("a") as f:
        f.write(f"[illumination_correction] ({overwrite_input=})\n")

    logging.info("[illumination_correction] END")
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=illumination_correction)
