import logging
from pathlib import Path

from pydantic import validate_call

from fractal_tasks_mock.utils import _group_zarr_urls_by_well


@validate_call
def find_registration_consensus(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
) -> None:
    """
    Dummy task description.

    Args:
        zarr_urls: description
        zarr_dir: description
    """

    logging.info("[find_registration_consensus] START")
    well_to_zarr_urls = _group_zarr_urls_by_well(zarr_urls)
    for well, well_zarr_urls in well_to_zarr_urls.items():
        logging.info(f"[find_registration_consensus] {well=}")
        for zarr_url in well_zarr_urls:
            table_path = Path(zarr_url) / "registration_table"
            try:
                with table_path.open("r") as f:
                    f.read()
                logging.info(
                    f"[find_registration_consensus]  "
                    f"Read {table_path.as_posix()}"
                )
            except FileNotFoundError:
                logging.info(
                    f"[find_registration_consensus]  "
                    f"FAIL Reading {table_path.as_posix()}"
                )

        logging.info(
            f"[find_registration_consensus] Find consensus for {well=}"
        )
        for zarr_url in well_zarr_urls:
            table_path = Path(zarr_url) / "registration_table_final"
            logging.info(
                f"[find_registration_consensus]   Write {table_path.as_posix()}"
            )
            with table_path.open("w") as f:
                f.write("This is the consensus-based new table.\n")

    logging.info("[find_registration_consensus] END")


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=find_registration_consensus)
