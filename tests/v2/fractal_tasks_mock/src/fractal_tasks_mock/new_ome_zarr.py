import logging
from pathlib import Path

from pydantic import validate_call

from fractal_tasks_mock.utils import _extract_common_root


@validate_call
def new_ome_zarr(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    suffix: str = "new",
) -> dict:
    """
    Dummy task description.
    """

    dict_shared = _extract_common_root(zarr_urls)
    old_plate = dict_shared.get("shared_plate")

    logging.info("[new_ome_zarr] START")
    logging.info(f"[new_ome_zarr] {zarr_urls=}")
    logging.info(f"[new_ome_zarr] Identified {old_plate=}")

    assert old_plate.endswith(".zarr")  # nosec
    new_plate = old_plate.strip(".zarr") + f"_{suffix}.zarr"
    logging.info(f"[new_ome_zarr] {new_plate=}")

    # Based on images in image_folder, create plate OME-Zarr
    new_zarr_path = (Path(zarr_dir) / new_plate).as_posix()
    logging.info(f"[new_ome_zarr] {new_zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(new_zarr_path).mkdir()

    parallelization_list = []
    for old_zarr_url in zarr_urls:
        new_zarr_url = old_zarr_url.replace(old_plate, new_plate)
        parallelization_list.append(
            dict(
                zarr_url=old_zarr_url,
                init_args=dict(
                    new_zarr_url=new_zarr_url,
                    new_plate=new_plate,
                ),
            )
        )

    # Prepare output metadata
    out = dict(parallelization_list=parallelization_list)
    logging.info("[new_ome_zarr] END")
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=new_ome_zarr)
