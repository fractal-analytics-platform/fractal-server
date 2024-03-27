import json
import logging
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Callable

from pydantic.decorator import validate_arguments


def run_fractal_task(task_function: Callable) -> None:
    # Parse command-line-interface arguments
    parser = ArgumentParser()
    parser.add_argument(
        "--args-json", help="JSON file with task arguments", required=True
    )
    parser.add_argument(
        "--out-json",
        help="JSON file for task output metadata.",
        required=True,
    )
    cli_args = parser.parse_args()

    # Preliminary check
    if Path(cli_args.out_json).exists():
        logging.error(f"Output file {cli_args.out_json} already exists, exit.")
        sys.exit(1)

    # Read parameters dictionary
    with open(cli_args.args_json, "r") as f:
        task_args = json.load(f)

    # Run task
    logging.warning(f"START {task_function.__name__} task")
    metadata_update = task_function(**task_args)
    logging.warning(f"END {task_function.__name__} task")

    # Write output metadata to file
    with open(cli_args.out_json, "w") as fout:
        json.dump(metadata_update, fout, indent=2)


@validate_arguments
def create_dummy_images(
    *,
    paths: list[str],
    zarr_dir: str,
    image_dir: str,
) -> dict:
    """
    Dummy task description.
    """

    if len(paths) > 0:
        raise ValueError(
            "Error in create_dummy_images, "
            f"`paths` argument must be empty, but {paths=}."
        )

    # Based on images in image_folder, create plate OME-Zarr
    zarr_dir = zarr_dir.rstrip("/")
    plate_zarr_name = "my_plate.zarr"
    zarr_path = (Path(zarr_dir) / plate_zarr_name).as_posix()

    logging.warning("[create_dummy_images] START")
    logging.warning(f"[create_dummy_images] {image_dir=}")
    logging.warning(f"[create_dummy_images] {zarr_dir=}")
    logging.warning(f"[create_dummy_images] {zarr_path=}")

    # Create (fake) OME-Zarr folder on disk
    Path(zarr_path).mkdir(parents=True)

    image_list_updates = []
    for img_relative in ["A/01", "A/02"]:
        path = (Path(zarr_path) / img_relative).as_posix()
        image_list_updates.append(dict(path=path))

    out = dict(image_list_updates=image_list_updates)
    logging.warning("[create_dummy_images] END")
    return out


if __name__ == "__main__":
    run_fractal_task(task_function=create_dummy_images)
