from pydantic import validate_call

from fractal_tasks_mock.utils import _extract_common_root
from fractal_tasks_mock.utils import _group_zarr_urls_by_well


def _read_acquisition_index_from_ngff_metadata(path: str) -> int:
    """
    This is a mock, where we simply guess the acquisition index
    from the path. The actual function should read OME-NGFF metadata.
    """
    return int(path.split("/")[-1][0])


@validate_call
def calculate_registration_init(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    ref_acquisition: int,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_urls: description
        zarr_dir: description
        ref_acquisition: Reference-cycle acquisition number
    """

    print("[calculate_registration_init] START")
    print(f"[calculate_registration_init] {zarr_urls=}")

    # Detect plate prefix
    shared_plate = _extract_common_root(zarr_urls).get("shared_plate")
    print(f"[calculate_registration_init] Identified {shared_plate=}")
    print(f"[calculate_registration_init] Identified {zarr_dir=}")

    well_to_zarr_urls = _group_zarr_urls_by_well(zarr_urls)

    parallelization_list = []
    for well, well_zarr_urls in well_to_zarr_urls.items():
        print(f"[calculate_registration_init] {well=}")
        x_cycles = []
        ref_zarr_url = None

        # Loop over all well images, and find the reference one
        for zarr_url in well_zarr_urls:
            if (
                _read_acquisition_index_from_ngff_metadata(zarr_url)
                == ref_acquisition
            ):
                if ref_zarr_url is not None:
                    raise ValueError("We should have not reached this branch.")
                ref_zarr_url = zarr_url
            else:
                x_cycles.append(zarr_url)

        # Then, include all actually-relevant (zarr_url, ref_zarr_url) pairs
        for zarr_url in x_cycles:
            parallelization_list.append(
                dict(
                    zarr_url=zarr_url,
                    init_args=dict(ref_zarr_url=ref_zarr_url),
                )
            )

    print("[calculate_registration_init] END")
    return dict(parallelization_list=parallelization_list)


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=calculate_registration_init)
